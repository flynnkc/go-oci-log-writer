package goocilogwriter_test

import (
	"errors"
	"os"
	"testing"

	lw "github.com/flynnkc/go-oci-log-writer"
	"github.com/oracle/oci-go-sdk/v65/common"
)

var (
	details lw.LogWriterDetails = lw.LogWriterDetails{
		Provider:   common.DefaultConfigProvider(),
		Source:     common.String("Source"),
		Type:       common.String("Type"),
		Subject:    common.String("Subject"),
		BufferSize: common.Int(2),
	}
)

func TestNew(t *testing.T) {
	logId := "abc"
	source := "def"
	logType := "ghi"
	provider := common.DefaultConfigProvider()

	t.Run("LogId=1", func(t *testing.T) {
		d := lw.LogWriterDetails{
			Provider: provider,
			Source:   &source,
			Type:     &logType,
		}

		_, err := lw.New(d)
		if !errors.Is(err, lw.ErrNoLogId) {
			t.Errorf("No log ID: got %v want %v", err, lw.ErrNoLogId)
		}
	})

	t.Run("LogId=2", func(t *testing.T) {
		d := lw.LogWriterDetails{
			LogId:    common.String(""),
			Provider: provider,
			Source:   &source,
			Type:     &logType,
		}

		_, err := lw.New(d)
		if !errors.Is(err, lw.ErrNoLogId) {
			t.Errorf("No log ID: got %v want %v", err, lw.ErrNoLogId)
		}
	})

	t.Run("LogSource=1", func(t *testing.T) {
		d := lw.LogWriterDetails{
			Provider: provider,
			LogId:    &logId,
			Type:     &logType,
		}

		_, err := lw.New(d)
		if !errors.Is(err, lw.ErrNoLogSource) {
			t.Errorf("No log ID: got %v want %v", err, lw.ErrNoLogSource)
		}
	})

	t.Run("LogType=1", func(t *testing.T) {
		d := lw.LogWriterDetails{
			Provider: provider,
			LogId:    &logId,
			Source:   &source,
		}

		_, err := lw.New(d)
		if !errors.Is(err, lw.ErrNoLogType) {
			t.Errorf("No log ID: got %v want %v", err, lw.ErrNoLogType)
		}
	})
}

func TestWrite(t *testing.T) {
	details.LogId = common.String(os.Getenv("OCI_LOG_ID"))
	writer, err := lw.New(details)
	if err != nil {
		t.Fatalf("error setting up write test: %v", err)
	} else if writer.LogId == common.String("") {
		t.Fatal("writer LogId empty")
	}

	t.Run("Write=1", func(t *testing.T) {
		s := []byte("Write Test 1")
		p, err := writer.Write(s)
		if err != nil {
			t.Fatalf("error on first write: %v", err)
		} else if p != len(s) {
			t.Logf("Incorrect number of bytes returned, got %v want %v", p, len(s))
			t.Fail()
		}

		p, err = writer.Write(s)
		if err != nil {
			t.Fatalf("error on second write: %v", err)
		} else if p != len(s) {
			t.Logf("Incorrect number of bytes returned, got %v want %v", p, len(s))
			t.Fail()
		}
	})

	// Remove mandatory fields and see what happens
	t.Run("Write=2", func(t *testing.T) {
		writer.Source = nil
		writer.Subject = nil
		writer.Type = nil

		s := []byte("Write Test 2")
		p, err := writer.Write(s)
		if err != nil {
			t.Fatalf("error on third write: %v", err)
		} else if p != len(s) {
			t.Logf("Incorrect number of bytes returned, got %v want %v", p, len(s))
			t.Fail()
		}

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

	writer, err := lw.New(details)
	if err != nil {
		t.Fatalf("error setting up write test: %v", err)
	}

	t.Run("Close=1", func(t *testing.T) {
		err := writer.Close()
		if err != nil {
			t.Errorf("Failed first close: %v", err)
		}
	})

	// No call to flush
	t.Run("Close=2", func(t *testing.T) {
		err := writer.Close()
		if !errors.Is(err, lw.ErrClosed) {
			t.Errorf("Failed second close: %v", err)
		}
	})

	// Should see all 3 messages after this test
	t.Run("Close=3", func(t *testing.T) {
		// New open writer
		writer, err = lw.New(details)
		if err != nil {
			t.Fatalf("error initializing writer: %v", err)
		}

		for _, s := range []string{"1 of 3", "2 of 3", "3 of 3"} {
			p, err := writer.Write([]byte("Close Test 3: Message " + s))
			if p == 0 || err != nil {
				t.Fail()
			}
		}

		err := writer.Close()
		if err != nil {
			t.Errorf("Failed third close: %v", err)
		}
	})
}
