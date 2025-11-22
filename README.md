# Go OCI Log Writer

This repository creates a buffered log that satisfies the io.Writer interface to allow for logging to a Custom Log in Oracle Cloud Infrastructure.

For use primarily as an example.

## Useage

```go
package main

import (
    "log"

    "github.com/oracle/oci-go-sdk/v65/common"
    "github.com/oracle/oci-go-sdk/v65/identity"
    olog "github.com/flynnkc/go-oci-log-writer"
)

func main() {
    provider := common.DefaultConfigProvider()
    logId := "ocid.abcd.1234"

    details := ocilog.OCILogWriterDetails{
        LogId:    &logId,
        Provider: provider,
        Source:   common.String("ServerA"),
        Type:     common.String("Access_Log"),
    }

    writer, err := ocilog.NewOCILogWriter(details)
    if err != nil {
        // do something
    }
    defer writer.Close()

    message := []byte("Access Granted")
    b, err := writer.Write(message)
    if err != nil {
        // do something
    }

    fmt.Println("Bytes written:", b)
}
```
