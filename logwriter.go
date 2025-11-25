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
	ErrLogEntrySize  = errors.New("error log entry greater than 1 megabyte")
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

// OCILogWriter implements the io.Writer interface for the purposes of sending data
// to the OCI Logging service.
type OCILogWriter struct {
	buffer  chan loggingingestion.LogEntry
	log     *log.Logger
	flushed chan bool // Signal worker is finished to parent
	closed  bool      // Prevent further writes after Close called
	rwMux   sync.RWMutex
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

	// Buffer for logging ingestion Log Entry. Enables non-blocking writes to log.
	// Default set to 200.
	BufferSize *int `mandatory:"false" json:"buffersize"`

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
	if cfg.BufferSize == nil {
		cfg.BufferSize = common.Int(50)
	} else if *cfg.BufferSize < 2 {
		// Minimum buffer size
		cfg.BufferSize = common.Int(2)
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

	lw := OCILogWriter{
		buffer:  make(chan loggingingestion.LogEntry, *cfg.BufferSize),
		log:     cfg.Logger,
		closed:  false,
		flushed: make(chan bool),
	}

	closer := sync.Once{}

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

		w.run(lw.buffer, lw.flushed, &closer)
	}

	return &lw, nil
}

// Write implements the io.Writer interface. Writes a single log entry with slice
// of byte p. Uses buffered output to reduce network traffic and API requests.
// Returns the length of data written and an error for checking. Entries must be
// less than 1MB.
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

	if len(le.String()) > megaByte {
		return 0, ErrLogEntrySize
	}

	// Send entry to buffer for processing
	lw.buffer <- le

	return len(p), nil
}

func (lw *OCILogWriter) UpdateBufferSize(n int) {
	go func() {
		b1 := lw.buffer
		lw.buffer = make(chan loggingingestion.LogEntry, n)

		for entry := range b1 {
			lw.buffer <- entry
		}
	}()
}

// Close flushes the buffer and closes the channel. Should be called in the
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

	close(lw.buffer)
	<-lw.flushed // Wait for logs to finish flushing

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

// run processes log entries from the buffer channel, flushing them to OCI Logging
// when the buffer is full or on a regular interval. It also periodically processes
// the queue file to retry failed log entries. This method runs as a goroutine and
// continues until the done channel is closed.
func (w *worker) run(buffer <-chan loggingingestion.LogEntry, flushed chan bool, closer *sync.Once) {
	entries := w.makeEntries(cap(buffer))

	flushTicker := time.NewTicker(flushInterval)
	fileTicker := time.NewTicker(fileInterval)

process:
	for {
		select {
		// Standard case, read from buffer and write when full
		case entry, open := <-buffer:
			if !open {
				break process
			}

			entries = append(entries, entry)
			if len(entries) >= cap(entries) {
				err := w.flush(entries)
				if err != nil {
					w.log.Printf("error flushing OCI logs: %s\n", err)
					w.writeLogsToFile(entries)
				}
				entries = w.makeEntries(cap(buffer))
				flushTicker.Reset(flushInterval)
			}

		// Flush timer
		case <-flushTicker.C:
			if len(entries) > 0 {
				err := w.flush(entries)
				if err != nil {
					w.log.Printf("error flushing OCI logs: %s\n", err)
					w.writeLogsToFile(entries)
				}
				entries = w.makeEntries(cap(buffer))
			}

		// File write timer
		case <-fileTicker.C:
			err := w.processQueueFile()
			if err != nil {
				w.log.Printf("error processing queue file: %s\n", err)
			}
		}
	}

	// Process remaining entries
	if len(entries) > 0 {
		err := w.flush(entries)
		if err != nil {
			w.log.Printf("error flushing OCI logs: %s\n", err)
			w.writeLogsToFile(entries)
		}
	}

	// Only one worker needs to finish processing and close file
	closer.Do(func() {
		// Process queue file
		err := w.processQueueFile()
		if err != nil {
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

func (w *worker) makeEntries(bufCap int) []loggingingestion.LogEntry {
	return make([]loggingingestion.LogEntry, 0, bufCap/w.numWorkers)
}
