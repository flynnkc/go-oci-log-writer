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
	ErrNoLogId      = errors.New("error Log ID not specified")
	ErrNoLogSource  = errors.New("error Log Source not specified")
	ErrNoLogType    = errors.New("error Log Type not specified")
	ErrLogEntrySize = errors.New("error log entry greater than 1 megabyte")
	ErrClosed       = errors.New("error log writer closed")
	ErrNot2XX       = errors.New("error non-2XX status code in put log response")
	logSpecVersion  = common.String("1.0") // Mandatory value per SDK docs
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
	Client    LoggingClient
	LogId     *string
	Source    *string
	Subject   *string
	Type      *string
	log       *log.Logger
	buffer    chan loggingingestion.LogEntry
	queueFile *os.File
	closed    bool      // Prevent further writes after Close called
	done      chan bool // Signal closure to worker
	flushed   chan bool // Signal worker is finished to parent
	rwMux     sync.RWMutex
	fileMux   sync.Mutex
}

// OCILogWriterDetails stores details required for OCI Logging entries. Source,
// Subject, and Type MUST NOT be nil. More information can be found at
// https://pkg.go.dev/github.com/oracle/oci-go-sdk/v65@v65.105.0/loggingingestion
type OCILogWriterDetails struct {

	// Configuration provider for initializing logging ingestion client.
	Provider common.ConfigurationProvider `mandatory:"true" json:"provider"`

	// OCID of OCI Logging Log to send data to.
	LogId *string `mandatory:"true" json:"logId"`

	// OCI Logging Log fields
	Source  *string `mandatory:"true" json:"source"`
	Type    *string `mandatory:"true" json:"type"`
	Subject *string `mandatory:"false" json:"subject"`

	// Buffer for logging ingestion Log Entry. Enables non-blocking writes to log.
	// Default set to 200.
	BufferSize *int `mandatory:"false" json:"buffersize"`

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

	// Validate LogWriterDetails
	if cfg.LogId == nil || *cfg.LogId == "" {
		return nil, ErrNoLogId
	}
	if cfg.Source == nil {
		return nil, ErrNoLogSource
	}
	if cfg.Type == nil {
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
		buffer:    make(chan loggingingestion.LogEntry, *cfg.BufferSize),
		Client:    client,
		LogId:     cfg.LogId,
		Source:    cfg.Source,
		Subject:   cfg.Subject,
		Type:      cfg.Type,
		log:       cfg.Logger,
		queueFile: queueFile,
		closed:    false,
		done:      make(chan bool),
		flushed:   make(chan bool),
	}

	lw.log.Printf("temporary log local failover location at %s\n", lw.queueFile.Name())

	go lw.worker(lw.done, lw.flushed)

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

	select {
	// Write to in-memory buffer
	case lw.buffer <- le:
	default:
		// Failover to queue file write
		lw.fileMux.Lock()
		defer lw.fileMux.Unlock()
		// If buffer is full, write directly to queue file
		err = writeLogEntryToFile(lw.queueFile, le)
		if err != nil {
			lw.log.Printf("error writing to queue file: %s\n", err)
			return 0, err
		}
	}

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

	lw.done <- true
	<-lw.flushed // Wait for logs to finish flushing

	return nil

}

// writeLogsToFile writes failed log entries to the queue file
func (lw *OCILogWriter) writeLogsToFile(entries []loggingingestion.LogEntry) {
	lw.fileMux.Lock()
	defer lw.fileMux.Unlock()

	for _, entry := range entries {
		err := writeLogEntryToFile(lw.queueFile, entry)
		if err != nil {
			lw.log.Printf("error writing to queue file: %s\n", err)
		}
	}
}

// Worker runs as a goroutine and receives logs, flushing when buffer is reached.
// Since sending logs to OCI is a slow network operation, this should happen
// concurrently with other log processing to prevent blocking.
func (lw *OCILogWriter) worker(done, flushed chan bool) {
	entries := make([]loggingingestion.LogEntry, 0, cap(lw.buffer))

	flushTicker := time.NewTicker(flushInterval)
	fileTicker := time.NewTicker(fileInterval)

	for {
		select {
		case entry := <-lw.buffer:
			entries = append(entries, entry)
			if len(entries) >= cap(lw.buffer)>>1 {
				err := lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
					lw.writeLogsToFile(entries)
				}
				entries = make([]loggingingestion.LogEntry, 0, cap(lw.buffer))
				flushTicker.Reset(flushInterval)
			}

		case <-flushTicker.C:
			if len(entries) > 0 {
				err := lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
					lw.writeLogsToFile(entries)
				}
				entries = make([]loggingingestion.LogEntry, 0, cap(lw.buffer))
			}

		case <-fileTicker.C:
			err := lw.processQueueFile()
			if err != nil {
				lw.log.Printf("error processing queue file: %s\n", err)
			}

		case <-done:
			// Check to make sure buffer is empty
			for i := 0; i < len(lw.buffer); i++ {
				entries = append(entries, <-lw.buffer)
			}

			// Process remaining entries
			if len(entries) > 0 {
				err := lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
					lw.writeLogsToFile(entries)
				}
			}

			// Process queue file
			err := lw.processQueueFile()
			if err != nil {
				lw.log.Printf("error processing queue file: %s\n", err)
			}

			lw.queueFile.Close()

			flushed <- true
			return
		}
	}
}

func (lw *OCILogWriter) processQueueFile() error {
	// Read from queue file and send to OCI
	lw.queueFile.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(lw.queueFile)
	scanner.Buffer(make([]byte, megaByte+1), megaByte+1)

	entries := []loggingingestion.LogEntry{}
	for scanner.Scan() {
		var entry loggingingestion.LogEntry
		err := json.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			lw.log.Printf("error unmarshaling log entry: %s\n", err)
			continue
		}
		entries = append(entries, entry)
	}

	// Reset file
	lw.fileMux.Lock()
	lw.queueFile.Truncate(0)
	lw.queueFile.Seek(0, io.SeekStart)
	lw.fileMux.Unlock()

	// Flush and rewrite if failed
	err := lw.flush(entries)
	if err != nil {
		lw.log.Printf("error flushing log entry: %s\n", err)
		lw.writeLogsToFile(entries)
	}
	return scanner.Err()
}

// Flush writes the logs to the OCI Logging service. Flush empties the buffer and
// sends collected log entries to OCI as a single batch. Returns an error on bad
// requests.
func (lw *OCILogWriter) flush(entries []loggingingestion.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	batch := loggingingestion.LogEntryBatch{
		Entries: entries,
		Type:    lw.Type,
		Subject: lw.Subject,
		Source:  lw.Source,
		Defaultlogentrytime: &common.SDKTime{
			Time: time.Now().UTC(), // Tz most commonly used for cloud resources
		},
	}

	details := loggingingestion.PutLogsDetails{
		Specversion:     logSpecVersion,
		LogEntryBatches: []loggingingestion.LogEntryBatch{batch},
	}

	request := loggingingestion.PutLogsRequest{
		LogId:          lw.LogId,
		PutLogsDetails: details,
	}

	ctx, cancel := context.WithTimeout(context.Background(), responseTimeout)
	defer cancel()

	response, err := lw.Client.PutLogs(ctx, request)
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
func writeLogEntryToFile(file *os.File, entry loggingingestion.LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	return err
}
