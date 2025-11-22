package goocilogwriter

import (
	"context"
	"crypto/rsa"
	"os"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockLoggingClient struct {
	mock.Mock
}

func (m *mockLoggingClient) PutLogs(ctx context.Context,
	request loggingingestion.PutLogsRequest) (loggingingestion.PutLogsResponse,
	error) {
	args := m.Called(ctx, request)
	return args.Get(0).(loggingingestion.PutLogsResponse), args.Error(1)
}

type mockConfigurationProvider struct {
	mock.Mock
}

func (m *mockConfigurationProvider) PrivateRSAKey() (*rsa.PrivateKey, error) {
	args := m.Called()
	return args.Get(0).(*rsa.PrivateKey), args.Error(1)
}

func (m *mockConfigurationProvider) KeyID() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *mockConfigurationProvider) TenancyOCID() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *mockConfigurationProvider) UserOCID() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *mockConfigurationProvider) KeyFingerprint() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *mockConfigurationProvider) Region() (string, error) {
	args := m.Called()
	return args.String(0), args.Error(1)
}

func (m *mockConfigurationProvider) AuthType() (common.AuthConfig, error) {
	args := m.Called()
	return args.Get(0).(common.AuthConfig), args.Error(1)
}

func TestNewOCILogWriter(t *testing.T) {
	logId := "log id"
	source := "source"
	logType := "log type"
	provider := &mockConfigurationProvider{}

	// Standard use case
	t.Run("NewLog=1", func(t *testing.T) {
		d := OCILogWriterDetails{
			Provider: provider,
			LogId:    &logId,
			Source:   &source,
			Type:     &logType,
		}

		_, err := NewOCILogWriter(d)
		assert.NoError(t, err, "error on valid new log writer")
	})

	// No Log ID
	t.Run("NewLogId=1", func(t *testing.T) {
		d := OCILogWriterDetails{
			Provider: provider,
			Source:   &source,
			Type:     &logType,
		}

		_, err := NewOCILogWriter(d)
		assert.Error(t, err, "no error on missing log id")
	})

	// Empty Log ID
	t.Run("NewLogId=2", func(t *testing.T) {
		d := OCILogWriterDetails{
			LogId:    common.String(""),
			Provider: provider,
			Source:   &source,
			Type:     &logType,
		}

		_, err := NewOCILogWriter(d)
		assert.Error(t, err, "no error on empty log id")
	})

	// No log source
	t.Run("NewLogSource=1", func(t *testing.T) {
		d := OCILogWriterDetails{
			Provider: provider,
			LogId:    &logId,
			Type:     &logType,
		}

		_, err := NewOCILogWriter(d)
		assert.Error(t, err, "no error on missing log source")
	})

	// No Log Type
	t.Run("NewLogType=1", func(t *testing.T) {
		d := OCILogWriterDetails{
			Provider: provider,
			LogId:    &logId,
			Source:   &source,
		}

		_, err := NewOCILogWriter(d)
		assert.Error(t, err, "no error on missing log type")
	})
}

func TestWrite(t *testing.T) {
	writer := OCILogWriter{
		Client: &mockLoggingClient{},
		buffer: make(chan loggingingestion.LogEntry, 8),
		closed: false,
	}

	// Normal write
	t.Run("Write=Valid", func(t *testing.T) {
		s := []byte("Write Test 1")
		p, err := writer.Write(s)
		assert.Equal(t, len(s), p,
			"write bytes written does not match data length")
		assert.NoError(t, err, "error on write")
	})

	t.Run("Write=TooLarge", func(t *testing.T) {
		s := make([]byte, 1048577)
		p, err := writer.Write(s)
		assert.Equal(t, 0, p, "incorrect bytes written")
		assert.EqualError(t, err, ErrLogEntrySize.Error())
	})

	t.Run("Write=File", func(t *testing.T) {
		queueFile, _ := os.CreateTemp("", "logTest")
		writer.queueFile = queueFile
		writer.buffer = make(chan loggingingestion.LogEntry, 1)

		t.Logf("Temporary test file: %s", writer.queueFile.Name())

		s := []byte("Write Test 3 - 1")
		p, err := writer.Write(s)
		assert.Equal(t, len(s), p)
		assert.NoError(t, err)

		s = []byte("Write Test 3 - 2")
		p, err = writer.Write(s)
		assert.Equal(t, len(s), p)
		assert.NoError(t, err)

		info, _ := writer.queueFile.Stat()
		assert.Greater(t, info.Size(), int64(0))

		writer.queueFile.Close()
		os.Remove(writer.queueFile.Name())
	})

	t.Run("Write=Closed", func(t *testing.T) {
		writer.closed = true
		s := []byte("Write test 4")
		p, err := writer.Write(s)
		assert.Equal(t, 0, p)
		assert.EqualError(t, err, ErrClosed.Error())
	})
}

func TestClose(t *testing.T) {

	// Normal Close
	t.Run("Close=Valid", func(t *testing.T) {
		writer := OCILogWriter{
			closed:  false,
			done:    make(chan bool),
			flushed: make(chan bool),
		}

		go func() {
			time.Sleep(3 * time.Second)
			<-writer.done
			writer.flushed <- true
		}()

		err := writer.Close()
		assert.NoError(t, err)
		assert.Equal(t, true, writer.closed)
	})

	t.Run("Close=Closed", func(t *testing.T) {
		writer := OCILogWriter{
			closed:  true,
			done:    make(chan bool),
			flushed: make(chan bool),
		}

		err := writer.Close()
		assert.EqualError(t, ErrClosed, err.Error())
	})

}
