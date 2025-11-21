package goocilogwriter

import (
	"context"
	"errors"
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
	responseTimeout = time.Duration(10 * time.Second)
)

// OCILogWriter implements the io.Writer interface for the purposes of sending data
// to the OCI Logging service.
type OCILogWriter struct {
	Client  loggingingestion.LoggingClient
	LogId   *string
	Source  *string
	Subject *string
	Type    *string
	log     *log.Logger
	closed  bool // Prevent further writes after Close called
	buffer  chan loggingingestion.LogEntry
	done    chan bool
	flushed chan bool
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

	// Validate LogWriterDetails
	if cfg.LogId == nil {
		return nil, ErrNoLogId
	} else if *cfg.LogId == "" {
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
		i := 200
		cfg.BufferSize = &i
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
		buffer:  make(chan loggingingestion.LogEntry, *cfg.BufferSize),
		Client:  client,
		LogId:   cfg.LogId,
		Source:  cfg.Source,
		Subject: cfg.Subject,
		Type:    cfg.Type,
		log:     cfg.ErrorLog,
		closed:  false,
		done:    make(chan bool),
		flushed: make(chan bool),
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
	id, _ := uuid.NewRandom()

	le := loggingingestion.LogEntry{
		Data: common.String(string(p)),
		Id:   common.String(id.String()),
		Time: &common.SDKTime{
			Time: time.Now().UTC(), // Tz most commonly used for cloud resources
		},
	}

	// Don't write to closed logger
	if lw.closed {
		return 0, ErrClosed
	}
	lw.buffer <- le

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

// Worker runs as a goroutine and receives logs, flushing when buffer is reached.
// Since sending logs to OCI is a slow network operation, this should happen
// concurrently with other log processing to prevent blocking.
func (lw *OCILogWriter) worker(done, flushed chan bool) {
	var entries []loggingingestion.LogEntry
	var err error
	bufferSize := cap(lw.buffer)

	ticker := time.NewTicker(flushInterval)

	for {
		select {
		case entry := <-lw.buffer:
			entries = append(entries, entry)
			if len(entries) >= bufferSize>>1 {
				err = lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
				}
				entries = nil
				ticker.Reset(flushInterval)
			}
		case <-ticker.C:
			err = lw.flush(entries)
			if err != nil {
				lw.log.Printf("error flushing OCI logs: %s\n", err)
			}
			entries = nil
		case <-done:
			// Check to make sure buffer is empty
			for i := 0; i < len(lw.buffer); i++ {
				entries = append(entries, <-lw.buffer)
			}

			// Check if there are any entries to flush
			if len(entries) > 0 {
				err = lw.flush(entries)
				if err != nil {
					lw.log.Printf("error flushing OCI logs: %s\n", err)
				}
			}
			flushed <- true
			ticker.Stop()
			return
		}
	}
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
