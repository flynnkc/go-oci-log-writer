package goocilogwriter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
)

var (
	ErrNoLogId       = errors.New("error Log ID not specified")
	ErrNoLogSource   = errors.New("error Log Source not specified")
	ErrNoLogType     = errors.New("error Log Type not specified")
	ErrInvalidWorker = errors.New("error WorkerCount need to be 1 or greater")
	ErrClosed        = errors.New("error log writer closed")
	ErrNot2XX        = errors.New("error non-2XX status code in put log response")
	logSpecVersion   = common.String("1.0") // Mandatory value per SDK docs
)

const (
	flushInterval       = time.Duration(1 * time.Minute)
	fileInterval        = time.Duration(1 * time.Hour)
	responseTimeout     = time.Duration(10 * time.Second)
	megaByte        int = 1024 * 1024
)

// LoggingClient is intended to accept a loggingingestion.LoggingClient
type LoggingClient interface {
	PutLogs(context.Context, loggingingestion.PutLogsRequest) (loggingingestion.PutLogsResponse, error)
}

// OCILogWriter implements a fan-in/fan-out writer that batches log entries and
// flushes them via a worker pool. Writes go to a fixed inbox channel. A single
// aggregator goroutine batches entries (by count) and sends slices to workers.
type OCILogWriter struct {
	inbox      chan loggingingestion.LogEntry   // Fan-in: never swapped, all writes go here
	batches    chan []loggingingestion.LogEntry // Fan-out: aggregator emits batches here for workers to consume
	log        *log.Logger                      // Logger for sending any error messages if desired
	numWorkers int                              // Number of workers processing and sending logs to OCI
	flushed    chan bool                        // Signal worker is finished to parent
	closed     bool                             // Prevent further writes after Close called
	rwMux      sync.RWMutex

	// Batching threshold (number of entries). Updated atomically by UpdateMaxBufferSize.
	maxBatchEntries int64
}

// OCILogWriterDetails stores details required for OCI Logging entries. Source,
// Subject, and Type MUST NOT be nil. More information can be found at
// https://pkg.go.dev/github.com/oracle/oci-go-sdk/v65@v65.105.0/loggingingestion
type OCILogWriterDetails struct {

	// Configuration provider for initializing logging ingestion client.
	Provider common.ConfigurationProvider `mandatory:"true" json:"provider"`

	// OCID of OCI Logging Log to send data to.
	LogId string `mandatory:"true" json:"logId"`

	// OCI Logging Log fields
	Source  string `mandatory:"true" json:"source"`
	Type    string `mandatory:"true" json:"type"`
	Subject string `mandatory:"true" json:"subject"`

	// MaxBuffer for logging ingestion Log Entry. Enables non-blocking writes to log.
	// Default set to 50MB. In this implementation, this value controls the initial
	// batch threshold (entries) and the inbox channel capacity heuristic.
	MaxBufferSize *int `mandatory:"false" json:"buffersize"`

	// Number of worker goroutines to process log entries concurrently.
	// Default is 1.
	WorkerCount *int `mandatory:"false" json:"workercount"`

	// Queue file location for storing failed log entries. A temp queue file is
	// created in the system temp location if left empty. LogWriter will
	// periodically attempt to write logs to OCI Logging Log.
	QueueFile *string `mandatory:"false" json:"queuefile"`

	// Logger to write error messages and critical info to.
	Logger *log.Logger `mandatory:"false"`
}

// NewOCILogWriter returns a pointer to the LogWriter or an error. Use
// OCILogWriterDetails to define OCILogWriter behavior.
func NewOCILogWriter(cfg OCILogWriterDetails) (*OCILogWriter, error) {

	// Validate LogWriterDetails here and pass good values to worker
	if cfg.LogId == "" {
		return nil, ErrNoLogId
	}

	if cfg.Source == "" {
		return nil, ErrNoLogSource
	}

	if cfg.Type == "" {
		return nil, ErrNoLogType
	}

	// Default buffer size of 50
	if cfg.MaxBufferSize == nil {
		cfg.MaxBufferSize = common.Int(50)
	} else if *cfg.MaxBufferSize < 2 {
		// Minimum buffer size
		cfg.MaxBufferSize = common.Int(2)
	}

	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "oci-logger-error: ", log.LstdFlags|log.LUTC)
	}

	if cfg.WorkerCount == nil {
		cfg.WorkerCount = common.Int(1)
	} else if *cfg.WorkerCount < 1 {
		return nil, ErrInvalidWorker
	}

	var queueFile *os.File
	var err error
	if cfg.QueueFile == nil {
		// Create a temporary file for the queue
		queueFile, err = os.CreateTemp("", "oci-log-queue-")
		if err != nil {
			return nil, err
		}
	} else {
		queueFile, err = os.Open(*cfg.QueueFile)
		if err != nil {
			return nil, err
		}
	}

	// provider can be user, instance, workload, or resource principal for flexibility
	client, err := loggingingestion.NewLoggingClientWithConfigurationProvider(cfg.Provider)
	if err != nil {
		return nil, err
	}

	// Heuristic: use half of MaxBufferSize as inbox capacity, minimum 2
	inboxCap := *cfg.MaxBufferSize >> 1
	if inboxCap < 2 {
		inboxCap = 2
	}

	lw := OCILogWriter{
		inbox:           make(chan loggingingestion.LogEntry, inboxCap),
		batches:         make(chan []loggingingestion.LogEntry, *cfg.WorkerCount),
		numWorkers:      *cfg.WorkerCount,
		log:             cfg.Logger,
		closed:          false,
		flushed:         make(chan bool),
		maxBatchEntries: int64(inboxCap), // initial batch threshold
	}

	closer := sync.Once{}

	lw.startAggregator()

	// Start worker(s)
	for range *cfg.WorkerCount {
		w := worker{
			client:     client,
			numWorkers: *cfg.WorkerCount,
			queueFile:  queueFile,
			log:        cfg.Logger,
			logId:      &cfg.LogId,
			source:     &cfg.Source,
			subject:    &cfg.Subject,
			logType:    &cfg.Type,
		}

		go w.runBatches(lw.batches, lw.flushed, &closer)
	}

	return &lw, nil
}

// Write implements the io.Writer interface. Writes a single log entry with slice
// of byte p. Uses buffered inbox to reduce network traffic and API requests.
// Returns the length of data written and an error for checking. Entries greater than
// 1MB will have data truncated.
func (lw *OCILogWriter) Write(p []byte) (int, error) {
	lw.rwMux.RLock()
	defer lw.rwMux.RUnlock()

	// Don't write to closed logger
	if lw.closed {
		return 0, ErrClosed
	}

	// Random UUID allocation for each log entry
	id, err := uuid.NewRandom()
	if err != nil {
		return 0, err
	}

	le := loggingingestion.LogEntry{
		Data: common.String(string(p)),
		Id:   common.String(id.String()),
		Time: &common.SDKTime{
			Time: time.Now().UTC(),
		},
	}

	// Truncate data
	if len(le.String()) > megaByte {
		data := *le.Data
		data = data[:len(le.String())-megaByte]
		le.Data = &data
	}

	// Fan-in
	lw.inbox <- le

	return len(p), nil
}

// UpdateMaxBufferSize dynamically adjusts the batching threshold (entry count)
// used by the aggregator. This is an atomic update and does not swap channels
// or risk data loss. The aggregator will observe the new threshold and start
// emitting batches accordingly.
func (lw *OCILogWriter) UpdateMaxBufferSize(n int) {
	if n < 2 {
		n = 2
	}
	atomic.StoreInt64(&lw.maxBatchEntries, int64(n))
}

func (lw *OCILogWriter) getMaxBatchEntries() int {
	return int(atomic.LoadInt64(&lw.maxBatchEntries))
}

func (lw *OCILogWriter) startAggregator() {
	go func() {
		flushTicker := time.NewTicker(flushInterval)
		defer flushTicker.Stop()
		defer close(lw.batches) // signal workers when aggregator finishes

		cur := make([]loggingingestion.LogEntry, 0, lw.getMaxBatchEntries())

		for {
			select {
			case e, ok := <-lw.inbox:
				if !ok {
					// drain remaining
					if len(cur) > 0 {
						lw.emitBatch(&cur)
					}
					return
				}
				cur = append(cur, e)
				if len(cur) >= lw.getMaxBatchEntries() {
					lw.emitBatch(&cur)
				}
			case <-flushTicker.C:
				if len(cur) > 0 {
					lw.emitBatch(&cur)
				}
			}
		}
	}()
}

func (lw *OCILogWriter) emitBatch(cur *[]loggingingestion.LogEntry) {
	b := *cur
	lw.batches <- b
	*cur = make([]loggingingestion.LogEntry, 0, lw.getMaxBatchEntries())
}

// Close flushes the buffer and closes the channels. Should be called in the
// cleanup for an exiting program. Implements io.Closer interface. Close MUST be
// called from here and from no other function.
func (lw *OCILogWriter) Close() error {
	lw.rwMux.Lock()

	if lw.closed {
		lw.rwMux.Unlock()
		return ErrClosed
	}
	lw.closed = true

	lw.rwMux.Unlock()

	// Stop aggregator; it will close batches when done
	close(lw.inbox)

	for range lw.numWorkers {
		<-lw.flushed // Wait for logs to finish flushing
	}

	return nil
}

type worker struct {
	client     LoggingClient
	numWorkers int
	queueFile  *os.File
	log        *log.Logger
	logId      *string
	source     *string
	subject    *string
	logType    *string
	fileMux    sync.Mutex
}

// runBatches processes emitted log entry batches from the batches channel,
// flushing them to OCI Logging, and periodically processing the queue file.
// Continues until the batches channel is closed.
func (w *worker) runBatches(batches <-chan []loggingingestion.LogEntry, flushed chan bool, closer *sync.Once) {
	fileTicker := time.NewTicker(fileInterval)
	defer fileTicker.Stop()

	for {
		select {
		case entries, open := <-batches:
			if !open {
				goto done
			}
			if len(entries) == 0 {
				continue
			}

			if err := w.flush(entries); err != nil {
				w.log.Printf("error flushing OCI logs: %s\n", err)
				w.writeLogsToFile(entries)
			}

		case <-fileTicker.C:
			if err := w.processQueueFile(); err != nil {
				w.log.Printf("error processing queue file: %s\n", err)
			}
		}
	}

done:
	// Only one worker needs to finish processing and close file
	closer.Do(func() {
		// Process queue file
		if err := w.processQueueFile(); err != nil {
			w.log.Printf("error processing queue file: %s\n", err)
		}

		w.queueFile.Close()
	})

	flushed <- true
}

// writeLogsToFile writes failed log entries to the queue file
func (w *worker) writeLogsToFile(entries []loggingingestion.LogEntry) {
	w.fileMux.Lock()
	defer w.fileMux.Unlock()

	for _, entry := range entries {
		err := w.writeLogEntryToFile(entry)
		if err != nil {
			w.log.Printf("error writing to queue file: %s\n", err)
		}
	}
}

func (w *worker) processQueueFile() error {
	// Read from queue file and send to OCI
	w.queueFile.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(w.queueFile)
	scanner.Buffer(make([]byte, 0, megaByte+1), megaByte+1)

	entries := []loggingingestion.LogEntry{}
	for scanner.Scan() {
		var entry loggingingestion.LogEntry
		err := json.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			w.log.Printf("error unmarshaling log entry: %s\n", err)
			continue
		}
		entries = append(entries, entry)
	}

	// Reset file
	w.fileMux.Lock()
	w.queueFile.Truncate(0)
	w.queueFile.Seek(0, io.SeekStart)
	w.fileMux.Unlock()

	// Flush and rewrite if failed
	err := w.flush(entries)
	if err != nil {
		w.log.Printf("error flushing log entry: %s\n", err)
		w.writeLogsToFile(entries)
	}
	return scanner.Err()
}

// Flush writes the logs to the OCI Logging service. Flush empties the buffer and
// sends collected log entries to OCI as a single batch. Returns an error on bad
// requests.
func (w *worker) flush(entries []loggingingestion.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	batch := loggingingestion.LogEntryBatch{
		Entries: entries,
		Type:    w.logType,
		Subject: w.subject,
		Source:  w.source,
		Defaultlogentrytime: &common.SDKTime{
			Time: time.Now().UTC(), // Tz most commonly used for cloud resources
		},
	}

	details := loggingingestion.PutLogsDetails{
		Specversion:     logSpecVersion,
		LogEntryBatches: []loggingingestion.LogEntryBatch{batch},
	}

	request := loggingingestion.PutLogsRequest{
		LogId:          w.logId,
		PutLogsDetails: details,
	}

	ctx, cancel := context.WithTimeout(context.Background(), responseTimeout)
	defer cancel()

	response, err := w.client.PutLogs(ctx, request)
	if err != nil {
		return err
	} else if response.RawResponse.StatusCode < 200 ||
		response.RawResponse.StatusCode > 299 {
		return ErrNot2XX
	}

	return nil
}

// writeLogEntryToFile writes individual log entries to the queue file. MUST be
// protected by fileMux lock.
func (w *worker) writeLogEntryToFile(entry loggingingestion.LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = w.queueFile.Write(append(data, '\n'))
	return err
}
