package goocilogwriter

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"os"
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
	flushInterval   = time.Duration(1 * time.Minute)
	fileInterval    = time.Duration(1 * time.Hour)
	responseTimeout = time.Duration(10 * time.Second)
)

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
	closed    bool // Prevent further writes after Close called
	buffer    chan loggingingestion.LogEntry
	queueFile *os.File
	done      chan bool
	flushed   chan bool
}

// OCILogWriterDetails stores details required for OCI Logging entries. Source,
// Subject, and Type MUST NOT be nil.
type OCILogWriterDetails struct {
	// Log OCID
	LogId    *string                      `mandatory:"true" json:"logId"`
	Provider common.ConfigurationProvider `mandatory:"true" json:"provider"`
	// Source of log. Needs to be a string. (ex. ServerA)
	Source *string `mandatory:"true" json:"source"`
	// Type of log. (ex. ServerA.AccessLog)
	Type *string `mandatory:"true" json:"type"`
	// Subject for further specification if desired
	Subject    *string     `mandatory:"false" json:"subject"`
	BufferSize *int        `mandatory:"false" json:"buffersize"`
	ErrorLog   *log.Logger `mandatory:"false"`
}

// NewOCILogWriter returns a pointer to the LogWriter or an error
func NewOCILogWriter(cfg OCILogWriterDetails) (*OCILogWriter, error) {
	// Create a temporary file for the queue
	queueFile, err := os.CreateTemp("", "oci-log-queue-")
	if err != nil {
		return nil, err
	}

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
	// Default buffer size of 200
	if cfg.BufferSize == nil {
		cfg.BufferSize = common.Int(200)
	} else if *cfg.BufferSize < 2 {
		// Minimum buffer size
		cfg.BufferSize = common.Int(2)
	}
	if cfg.ErrorLog == nil {
		cfg.ErrorLog = log.New(os.Stderr, "oci-logger-error", log.LstdFlags|log.LUTC)
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
		log:       cfg.ErrorLog,
		queueFile: queueFile,
		closed:    false,
		done:      make(chan bool),
		flushed:   make(chan bool),
	}

	go lw.worker(lw.done, lw.flushed)

	return &lw, nil
}

// Write implements the io.Writer interface. Writes a single log entry with slice
// of byte p. Uses buffered output to reduce network traffic and API requests.
// Returns the length of data written and an error for checking. Max 1 MB per entry.
func (lw *OCILogWriter) Write(p []byte) (int, error) {

	if len(p) > 1048576 {
		return 0, ErrLogEntrySize
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

	// Don't write to closed logger
	if lw.closed {

		return 0, ErrClosed
	}

	// Write to in-memory buffer
	select {
	case lw.buffer <- le:
	default:
		// If buffer is full, write directly to queue file
		err = writeLogEntryToFile(lw.queueFile, le)
		if err != nil {
			lw.log.Printf("error writing to queue file: %s\n", err)
			return 0, err
		}
	}

	return len(p), nil
}

// Close flushes the buffer and closes the channel. Should be called in the
// cleanup for an exiting program. Uses sync to prevent multiple closes on
// the buffer channel. Implements io.Closer interface.
func (lw *OCILogWriter) Close() error {
	if lw.closed {
		return ErrClosed
	}

	// Close up shop
	lw.closed = true
	lw.done <- true
	<-lw.flushed // Wait for logs to finish flushing

	return nil

}

func writeLogEntryToFile(file *os.File, entry loggingingestion.LogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	_, err = file.Write(append(data, '\n'))
	return err
}

// Worker runs as a goroutine and receives logs, flushing when buffer is reached.
// Since sending logs to OCI is a slow network operation, this should happen
// concurrently with other log processing to prevent blocking.
func (lw *OCILogWriter) worker(done, flushed chan bool) {
	var entries []loggingingestion.LogEntry
	var err error
	bufferSize := cap(lw.buffer)

	flushTicker := time.NewTicker(flushInterval)
	fileTicker := time.NewTicker(fileInterval)

	for {
		select {
		case entry := <-lw.buffer:
			entries = append(entries, entry)
			if len(entries) >= bufferSize>>1 {
				err = lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
					// Write failed entries back to queue file
					for _, entry := range entries {
						err = writeLogEntryToFile(lw.queueFile, entry)
						if err != nil {
							lw.log.Printf("error writing to queue file: %s\n", err)
						}
					}
				}
				entries = nil
				flushTicker.Reset(flushInterval)
			}

		case <-flushTicker.C:
			if len(entries) > 0 {
				err = lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
					// Write failed entries back to queue file
					for _, entry := range entries {
						err = writeLogEntryToFile(lw.queueFile, entry)
						if err != nil {
							lw.log.Printf("error writing to queue file: %s\n", err)
						}
					}
				}
				entries = nil
			}

		case <-fileTicker.C:
			err = lw.processQueueFile()
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
				err = lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
					// Write failed entries back to queue file
					for _, entry := range entries {
						err = writeLogEntryToFile(lw.queueFile, entry)
						if err != nil {
							lw.log.Printf("error writing to queue file: %s\n", err)
						}
					}
				}
			}

			// Process queue file
			err = lw.processQueueFile()
			if err != nil {
				lw.log.Printf("error processing queue file: %s\n", err)
			}

			flushed <- true
			return
		}
	}
}

func (lw *OCILogWriter) processQueueFile() error {
	// Read from queue file and send to OCI
	lw.queueFile.Seek(0, io.SeekStart)
	scanner := bufio.NewScanner(lw.queueFile)
	for scanner.Scan() {
		var entry loggingingestion.LogEntry
		err := json.Unmarshal(scanner.Bytes(), &entry)
		if err != nil {
			lw.log.Printf("error unmarshaling log entry: %s\n", err)
			continue
		}
		err = lw.flush([]loggingingestion.LogEntry{entry})
		if err != nil {
			lw.log.Printf("error flushing log entry: %s\n", err)
			// Write failed entry back to queue file
			err = writeLogEntryToFile(lw.queueFile, entry)
			if err != nil {
				lw.log.Printf("error writing to queue file: %s\n", err)
			}
		}
	}
	lw.queueFile.Truncate(0)
	lw.queueFile.Seek(0, io.SeekStart)
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
