package pilosa

import (
	"io"
	"sort"
	"time"

	"github.com/pkg/errors"
)

type recordImportManager struct {
	client *Client
}

func newRecordImportManager(client *Client) *recordImportManager {
	return &recordImportManager{
		client: client,
	}
}

type importWorkerChannels struct {
	batches <-chan map[uint64][]Record
	errs    chan<- error
	status  chan<- ImportStatusUpdate
}

func (rim recordImportManager) Run(field *Field, iterator RecordIterator, options ImportOptions) error {
	shardWidth := options.shardWidth
	threadCount := options.threadCount
	batchesChan := make(chan map[uint64][]Record, threadCount)
	errChan := make(chan error)
	statusChan := options.statusChan
	batchSize := options.batchSize

	if options.importRecordsFunction == nil {
		return errors.New("importRecords function is required")
	}

	for i := 0; i < threadCount; i++ {
		r := &recordImportWorker{
			id:             i,
			client:         rim.client,
			field:          field,
			pendingRecords: make(map[uint64][]Record),
			chans: importWorkerChannels{
				batches: batchesChan,
				errs:    errChan,
				status:  statusChan,
			},
			options: options,
		}
		go r.Start()
	}

	var record Record
	var recordIteratorError error
	var recordCount int
	batchForShard := map[uint64][]Record{}

	for {
		record, recordIteratorError = iterator.NextRecord()
		if recordIteratorError != nil {
			if recordIteratorError == io.EOF {
				recordIteratorError = nil
			}
			break
		}
		//dont think this way of distributing records to recordChans is optimal
		//a lot of blocking to non-empty channels that can be making progress from full channels that
		//are in process to being uploaded and waiting for pilosa cluster, especially noticable with
		//large batch sizes

		// shard := record.Shard(shardWidth)
		// recordChans[shard%threadCount] <- record

		//will try different approach
		recordCount++
		shard := record.Shard(shardWidth)
		batchForShard[shard] = append(batchForShard[shard], record)

		if recordCount >= batchSize {
			batchesChan <- batchForShard
			batchForShard = make(map[uint64][]Record)
			recordCount = 0

		}
	}

	close(batchesChan)

	if recordIteratorError != nil {
		return recordIteratorError
	}

	done := 0
	for {
		select {
		case workerErr := <-errChan:
			if workerErr != nil {
				return workerErr
			}
			done++
			if done == threadCount {
				return nil
			}
		}
	}
}

//max number of records held before sending request to the cluster
//to avoid flooding cluster with tons of tiny batches for shards
//but can't be too big too, since records would be held in memory for every shard
const maxPendingRecords = 10000

//changing this from simple function so that can keep state for pendingRecord here
type recordImportWorker struct {
	id             int
	pendingRecords map[uint64][]Record
	client         *Client
	field          *Field
	chans          importWorkerChannels
	options        ImportOptions
}

func (w *recordImportWorker) Start() {
	importFun := w.options.importRecordsFunction
	statusChan := w.chans.status
	batchesChan := w.chans.batches
	errChan := w.chans.errs
	shardNodes := map[uint64][]fragmentNode{}

	importRecords := func(shard uint64, records []Record) error {
		var nodes []fragmentNode
		var ok bool
		var err error
		if nodes, ok = shardNodes[shard]; !ok {
			// if the data has row or column keys, send the data only to the coordinator
			if w.field.index.options.keys || w.field.options.keys {
				node, err := w.client.fetchCoordinatorNode()
				if err != nil {
					return err
				}
				nodes = []fragmentNode{node}
			} else {
				nodes, err = w.client.fetchFragmentNodes(w.field.index.name, shard)
				if err != nil {
					return err
				}
			}
		}
		tic := time.Now()
		sort.Sort(recordSort(records))
		err = importFun(w.field, shard, records, nodes, &w.options)
		if err != nil {
			return err
		}
		took := time.Since(tic)
		if statusChan != nil {
			statusChan <- ImportStatusUpdate{
				ThreadID:      w.id,
				Shard:         shard,
				ImportedCount: len(records),
				Time:          took,
			}
		}
		return nil
	}

	var err error

	for batchForShard := range batchesChan {
		for shard, records := range batchForShard {
			pending := w.pendingRecords[shard]
			if len(records)+len(pending) < maxPendingRecords {
				//if number of records too small - add to pending and continue
				w.pendingRecords[shard] = append(pending, records...)
				continue
			}
			//add pending records
			if len(pending) != 0 {
				records = append(records, pending...)
				w.pendingRecords[shard] = nil
			}
			err = importRecords(shard, records)
			if err != nil {
				//take fail-fast approach for now
				panic(err)
			}
		}
	}

	if err != nil {
		errChan <- err
		return
	}

	// import remaining pending records
	for shard, records := range w.pendingRecords {
		if len(records) == 0 {
			continue
		}
		err = importRecords(shard, records)
		if err != nil {
			//take fail-fast approach for now
			panic(err)
		}
	}

	errChan <- err
}

type ImportStatusUpdate struct {
	ThreadID      int
	Shard         uint64
	ImportedCount int
	Time          time.Duration
}

type recordSort []Record

func (rc recordSort) Len() int {
	return len(rc)
}

func (rc recordSort) Swap(i, j int) {
	rc[i], rc[j] = rc[j], rc[i]
}

func (rc recordSort) Less(i, j int) bool {
	return rc[i].Less(rc[j])
}
