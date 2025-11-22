package goocilogwriter_test

import (
	"fmt"
	"os"

	ocilog "github.com/flynnkc/go-oci-log-writer"
	"github.com/oracle/oci-go-sdk/v65/common"
)

// Requirements
// OCI Configuration File with writer permission under DEFAULT profile. More info:
//   https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm
//
// Environment Variables:
//   OCI_LOG_ID - OCID of a valid log in OCI MUST have permissions to write to log

func ExampleOCILogWriter() {
	provider := common.DefaultConfigProvider()
	logId := os.Getenv("OCI_LOG_ID")

	details := ocilog.OCILogWriterDetails{
		LogId:    &logId,
		Provider: provider,
		Source:   common.String("ServerA"),
		Type:     common.String("Access_Log"),
	}

	writer, err := ocilog.NewOCILogWriter(details)
	if err != nil {
		fmt.Println(err)
		return
	}
	// Defer close to flush buffer & prevent additional entries
	defer writer.Close()

	message := []byte("Access Granted")
	b, err := writer.Write(message)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("Bytes written:", b)
	// Output:
	// Bytes written: 14
}
