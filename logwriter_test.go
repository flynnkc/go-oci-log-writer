package goocilogwriter

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/loggingingestion"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

const (
	mockPEMKey string = `-----BEGIN PRIVATE KEY-----
MIICdgIBADANBgkqhkiG9w0BAQEFAASCAmAwggJcAgEAAoGBAJ+2/yQhkiw8mzIW
7bQbrDB2QH4HsTEX9Uh4f0PDJ+2fonuwSmuTLjclXwKOd8lxgTH2nu5MFFA7siD/
iik5XggD8RGfwqLfIJe62FhYG/c71Tfoocvmv/OOaYSapsXlWlE7fu5SDp2yAZFV
Z1+TUP/3vEYVxpPjQAZb6qZ7j+tPAgMBAAECgYAIHYJFUbddrA6ust+NIULUi42n
Wbi1J+R8tDKzPL1Qo6Xb5w9A/A+DGdEEDj0j7TKFWWSl8xOtJ/tbFeDtS07twMSj
3shmgvrd/2J0LyDbX01N4w3T8eB0TQvJZuI70wodKV8ZrkeaKLI0ntcPJRFiy/Oq
7L41Q7s6tnd9hQ/Y4QJBAMIeijjinSF9yV7PjqRcmMi/7IYy7+uMXr3xMXXty5P3
NQ5htAn8lO0uK8fg0Pi/GTEBPVQcAU8sWlVSuI023CkCQQDSoNE/fkoOGNBWxaFz
Zf/g9xE88GWdEtFuhaq7RofTxmtmIv1J+Ho3WCODGFEXr0dicox97XRyhUMJCbVO
pXq3AkAC2vglhg/RokwH/P2YJVSJ/2i3QKCO0m3CVX3owiqwbn51S7KeQvzd0EQM
mJ36SrVQJziDuDW8uGZLwv+79AahAkEAohyGkLDZvJnamD6J8fCajYJ7cQSxoMBg
EwmsC3HQju2TscvSWQF2x2v+ASNRHsKYVaxGd5GwY4gvvSAMvNheZwJAO9QapTwP
HZ7iMqihh32R+WsG7AemC/9uniupdiQor8zs1O9n1KGjDbqUEk0hXLS+D0i2FO/O
mcS2wXH8qr8YvQ==
-----END PRIVATE KEY-----`
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

	key, _ := loadRSAKeyFromPEM(mockPEMKey)

	provider.Mock.On("AuthType").Return(
		common.AuthConfig{
			AuthType: common.UserPrincipal,
		}, nil,
	)
	provider.Mock.On("TenancyOCID").Return("tenancy", nil)
	provider.Mock.On("UserOCID").Return("user", nil)
	provider.Mock.On("KeyFingerprint").Return("fingerprint", nil)
	provider.Mock.On("Region").Return("region", nil)
	provider.Mock.On("KeyID").Return("signing key", nil)
	provider.Mock.On("PrivateRSAKey").Return(key, nil)

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
		s := make([]byte, megaByte)
		p, err := writer.Write(s)
		assert.Equal(t, 0, p, "incorrect bytes written")
		assert.EqualError(t, err, ErrLogEntrySize.Error())
	})

	t.Run("Write=File", func(t *testing.T) {
		queueFile, _ := os.CreateTemp("", "logTest")
		writer.queueFile = queueFile
		writer.buffer = make(chan loggingingestion.LogEntry)

		// Block buffer to force file writes
		go func() {
			writer.buffer <- loggingingestion.LogEntry{}
		}()

		totalBytes := 0

		for i := 1; i < 6; i++ {
			s := fmt.Appendf(nil, "Write Test 3 - %d", i)
			p, err := writer.Write(s)
			totalBytes += p

			assert.Equal(t, len(s), p)
			assert.NoError(t, err)
		}

		info, err := writer.queueFile.Stat()
		if err != nil {
			t.Logf("Write=File: error getting queue file info %s", err)
		}
		t.Logf("%d bytes written to file", totalBytes)
		assert.Greater(t, info.Size(), int64(totalBytes))

		writer.queueFile.Close()
		os.Remove(writer.queueFile.Name())
		t.Logf("Cleaned up queue file at %s", writer.queueFile.Name())
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
			time.Sleep(2 * time.Second)
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

func loadRSAKeyFromPEM(pemData string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(pemData))
	if block == nil {
		return nil, errors.New("failed to decode PEM block")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse RSA private key: %w", err)
	}

	return privateKey, nil
}
