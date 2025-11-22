package goocilogwriter

import (
	"context"
	"crypto/rsa"
	"errors"
	"os"
	"testing"

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

var (
	details OCILogWriterDetails = OCILogWriterDetails{
		Provider:   &mockConfigurationProvider{},
		Source:     common.String("Source"),
		Type:       common.String("Type"),
		Subject:    common.String("Subject"),
		BufferSize: common.Int(2),
	}
)

func TestNewOCILogWriter(t *testing.T) {
	logId := "abc"
	source := "def"
	logType := "ghi"
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
	}

	// Normal writes
	t.Run("Write=1", func(t *testing.T) {
		t.Parallel()

		s := []byte("Write Test 1: 1 of 2")
		p, err := writer.Write(s)
		if err != nil {
			t.Fatalf("error on first write: %v", err)
		} else if p != len(s) {
			t.Logf("Incorrect number of bytes returned, got %v want %v", p, len(s))
			t.Fail()
		}

		s = []byte("Write Test 1: 2 of 2")
		p, err = writer.Write(s)
		if err != nil {
			t.Errorf("error on second write: %v", err)
		} else if p != len(s) {
			t.Errorf("Incorrect number of bytes returned, got %v want %v", p, len(s))
		}
	})

	// Remove mandatory fields and see what happens
	t.Run("Write=2", func(t *testing.T) {
		t.Parallel()

		writer.Source = nil
		writer.Subject = nil
		writer.Type = nil

		s := []byte("Write Test 2: 1 of 2")
		p, err := writer.Write(s)
		if err != nil {
			t.Fatalf("error on third write: %v", err)
		} else if p != len(s) {
			t.Logf("Incorrect number of bytes returned, got %v want %v", p, len(s))
			t.Fail()
		}

		s = []byte("Write Test 2: 2 of 2")
		p, err = writer.Write(s)
		if err != nil {
			t.Fatalf("error on third write: %v", err)
		} else if p != len(s) {
			t.Logf("Incorrect number of bytes returned, got %v want %v", p, len(s))
			t.Fail()
		}
	})
}

func TestClose(t *testing.T) {
	details.LogId = common.String(os.Getenv("OCI_LOG_ID"))

	// Normal Close
	t.Run("Close=1", func(t *testing.T) {
		t.Parallel()

		writer, err := NewOCILogWriter(details)
		if err != nil {
			t.Errorf("error configuring writer for Close1: %v", err)
		}

		err = writer.Close()
		if err != nil {
			t.Errorf("Failed first close: %v", err)
		}
	})

	// No call to flush
	t.Run("Close=2", func(t *testing.T) {
		t.Parallel()

		writer, err := NewOCILogWriter(details)
		if err != nil {
			t.Errorf("error configuring writer for Close1: %v", err)
		}

		writer.Close()
		err = writer.Close()
		if !errors.Is(err, ErrClosed) {
			t.Errorf("Failed second close: %v", err)
		}
	})

	// Should see all 3 messages after this test
	t.Run("Close=3", func(t *testing.T) {
		t.Parallel()

		// New open writer
		writer, err := NewOCILogWriter(details)
		if err != nil {
			t.Fatalf("error initializing writer: %v", err)
		}

		for _, s := range []string{"1 of 3", "2 of 3", "3 of 3"} {
			p, err := writer.Write([]byte("Close Test 3: Message " + s))
			if p == 0 || err != nil {
				t.Fail()
			}
		}

		err = writer.Close()
		if err != nil {
			t.Errorf("Failed third close: %v", err)
		}
	})
}
