package goocilogwriter

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
)

type LogWriter struct {
	Client  loggingingestion.LoggingClient
	LogId   *string
	Source  *string
	Subject *string
	Type    *string
	buffer  chan loggingingestion.LogEntry
}

// New returns a pointer to the LogWriter or an error
func New(provider common.ConfigurationProvider, logType, logSrc, logSubject string,
	bufferSize int) (*LogWriter, error) {

	client, err := loggingingestion.NewLoggingClientWithConfigurationProvider(provider)
	if err != nil {
		return nil, err
	}

	lw := LogWriter{
		buffer:  make(chan loggingingestion.LogEntry, bufferSize),
		Client:  client,
		Source:  &logSrc,
		Subject: &logSubject,
		Type:    &logType,
	}

	return &lw, nil
}

// Write implements the io.Writer interface.
func (lw *LogWriter) Write(p []byte) (int, error) {

	id, _ := uuid.NewRandom()

	le := loggingingestion.LogEntry{
		Data: common.String(string(p)),
		Id:   common.String(id.String()),
		Time: &common.SDKTime{
			Time: time.Now().UTC(),
		},
	}

	lw.buffer <- le

	// Check if channel is full and flush if true
	if len(lw.buffer) == cap(lw.buffer) {
		err := lw.flush()
		if err != nil {
			return 0, err
		}
	}

	return len(p), nil
}

// Close flushes the buffer and closes the channel. Should be called in the
// cleanup for an exiting program.
func (lw *LogWriter) Close() {
	lw.flush()
	close(lw.buffer)
}

// Flush writes the logs to the OCI Logging service
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
			Time: time.Now().UTC(),
		},
	}

	details := loggingingestion.PutLogsDetails{
		Specversion:     common.String("1.0"),
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
