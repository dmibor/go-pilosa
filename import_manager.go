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
	records <-chan Record
	errs    chan<- error
	status  chan<- ImportStatusUpdate
}

func (rim recordImportManager) Run(field *Field, iterator RecordIterator, options ImportOptions) error {
	shardWidth := options.shardWidth
	threadCount := uint64(options.threadCount)
	recordChans := make([]chan Record, threadCount)
	errChan := make(chan error)
	statusChan := options.statusChan

	if options.importRecordsFunction == nil {
		return errors.New("importRecords function is required")
	}

	for i := range recordChans {
		recordChans[i] = make(chan Record, options.batchSize)
		chans := importWorkerChannels{
			records: recordChans[i],
			errs:    errChan,
			status:  statusChan,
		}
		go recordImportWorker(i, rim.client, field, chans, options)
	}

	var record Record
	var recordIteratorError error

	for {
		record, recordIteratorError = iterator.NextRecord()
		if recordIteratorError != nil {
			if recordIteratorError == io.EOF {
				recordIteratorError = nil
			}
			break
		}
		shard := record.Shard(shardWidth)
		//try to submit to channel and push to the beginning of the queue if it would block.
		select {
		case recordChans[shard%threadCount] <- record:
		default:
			iterator.ToBeginning(record)
		}
	}

	for _, q := range recordChans {
		close(q)
	}

	if recordIteratorError != nil {
		return recordIteratorError
	}

	done := uint64(0)
	for {
		select {
		case workerErr := <-errChan:
			if workerErr != nil {
				return workerErr
			}
			done += 1
			if done == threadCount {
				return nil
			}
		}
	}
}

func recordImportWorker(id int, client *Client, field *Field, chans importWorkerChannels, options ImportOptions) {
	batchForShard := map[uint64][]Record{}
	importFun := options.importRecordsFunction
	statusChan := chans.status
	recordChan := chans.records
	errChan := chans.errs
	shardNodes := map[uint64][]fragmentNode{}

	importRecords := func(shard uint64, records []Record) error {
		var nodes []fragmentNode
		var ok bool
		var err error
		if nodes, ok = shardNodes[shard]; !ok {
			// if the data has row or column keys, send the data only to the coordinator
			if field.index.options.keys || field.options.keys {
				node, err := client.fetchCoordinatorNode()
				if err != nil {
					return err
				}
				nodes = []fragmentNode{node}
			} else {
				nodes, err = client.fetchFragmentNodes(field.index.name, shard)
				if err != nil {
					return err
				}
			}
		}
		tic := time.Now()
		sort.Sort(recordSort(records))
		err = importFun(field, shard, records, nodes, &options)
		if err != nil {
			return err
		}
		took := time.Since(tic)
		if statusChan != nil {
			statusChan <- ImportStatusUpdate{
				ThreadID:      id,
				Shard:         shard,
				ImportedCount: len(records),
				Time:          took,
			}
		}
		return nil
	}

	var err error
	recordCount := 0
	batchSize := options.batchSize
	shardWidth := options.shardWidth

	for record := range recordChan {
		recordCount += 1
		shard := record.Shard(shardWidth)
		batchForShard[shard] = append(batchForShard[shard], record)

		if recordCount >= batchSize {
			for shard, records := range batchForShard {
				if len(records) == 0 {
					continue
				}
				err = importRecords(shard, records)
				if err != nil {
					break
				}
				batchForShard[shard] = nil
			}
			recordCount = 0
		}
	}

	if err != nil {
		errChan <- err
		return
	}

	// import remaining records
	for shard, records := range batchForShard {
		if len(records) == 0 {
			continue
		}
		err = importRecords(shard, records)
		if err != nil {
			break
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
