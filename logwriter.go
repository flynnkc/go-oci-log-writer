package goocilogwriter

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
)

// LogWriter implements the io.Writer interface for the purposes of sending data
// to the OCI Logging service.
type LogWriter struct {
	Client  loggingingestion.LoggingClient
	LogId   *string
	Source  *string
	Subject *string
	Type    *string
	buffer  chan loggingingestion.LogEntry
	once    sync.Once
}

type LogWriterDetails struct {
	// Log OCID
	LogId    *string                      `mandatory:"true" name:"logId"`
	Provider common.ConfigurationProvider `mandatory:"true" name:"provider"`
	// Source of log. Needs to be a string. (ex. ServerA)
	Source *string `mandatory:"true" name:"source"`
	// Type of log. (ex. ServerA.AccessLog)
	Type *string `mandatory:"true" name:"type"`
	// Subject for further specification if desired
	Subject    *string `mandatory:"false" name:"subject"`
	BufferSize *int    `mandatory:"false" name:"buffersize"`
}

// New returns a pointer to the LogWriter or an error
func New(cfg LogWriterDetails) (*LogWriter, error) {

	// Validate LogWriterDetails
	if cfg.LogId == nil {
		return nil, errors.New("error Log ID not specified")
	}
	if cfg.Source == nil {
		return nil, errors.New("error Log Source not specified")
	}
	if cfg.Type == nil {
		return nil, errors.New("error Log Type not specified")
	}
	// Default buffer size of 200
	if cfg.BufferSize == nil {
		i := 200
		cfg.BufferSize = &i
	}

	// provider can be user, instance, workload, or resource principal for flexibility
	client, err := loggingingestion.NewLoggingClientWithConfigurationProvider(cfg.Provider)
	if err != nil {
		return nil, err
	}

	lw := LogWriter{
		buffer:  make(chan loggingingestion.LogEntry, *cfg.BufferSize),
		Client:  client,
		LogId:   cfg.LogId,
		Source:  cfg.Source,
		Subject: cfg.Subject,
		Type:    cfg.Type,
	}

	return &lw, nil
}

// Write implements the io.Writer interface. Writes a single log entry with slice
// of byte p. Uses buffered output to reduce network traffic and API requests.
// Returns the length of data written and an error for checking.
func (lw *LogWriter) Write(p []byte) (int, error) {
	// Max 1 MB per entry
	if len(p) > 1048576 {
		return 0, errors.New("error log entry greater than 1 megabyte")
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

	lw.buffer <- le

	// Check if channel is full and flush if true. There is a possibility that
	// more entries than the total buffer size are created if there are entries
	// blocked or appended to the channel as it's being flushed.
	if len(lw.buffer) == cap(lw.buffer) {
		err := lw.flush()
		if err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

// Close flushes the buffer and closes the channel. Should be called in the
// cleanup for an exiting program. Uses sync to prevent multiple closes on
// the buffer channel.
func (lw *LogWriter) Close() {
	lw.flush()
	// Politely close the buffer to prevent panic
	lw.once.Do(func() {
		close(lw.buffer)
	})
}

// Flush writes the logs to the OCI Logging service. Flush empties the buffer and
// sends collected log entries to OCI as a single batch. Returns an error on bad
// requests.
func (lw *LogWriter) flush() error {
	var entries []loggingingestion.LogEntry
	for entry := range lw.buffer {
		entries = append(entries, entry)
	}

	batch := loggingingestion.LogEntryBatch{
		Entries: entries,
		Type:    lw.Type,
		Subject: lw.Subject,
		Defaultlogentrytime: &common.SDKTime{
			Time: time.Now().UTC(), // Tz most commonly used for cloud resources
		},
	}

	details := loggingingestion.PutLogsDetails{
		Specversion:     common.String("1.0"), // Mandatory value per SKD docs
		LogEntryBatches: []loggingingestion.LogEntryBatch{batch},
	}

	request := loggingingestion.PutLogsRequest{
		LogId:          lw.LogId,
		PutLogsDetails: details,
	}

	response, err := lw.Client.PutLogs(context.Background(), request)
	if err != nil {
		return err
	} else if response.RawResponse.StatusCode != 200 {
		return errors.New(response.RawResponse.Status)
	}

	return nil
}
