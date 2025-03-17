# Go OCI Log Writer

This repository creates a buffered log that satisfies the io.Writer interface to allow for logging to a Custom Log in Oracle Cloud Infrastructure.

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
    provider, _ := identity.NewIdentityClientWithConfigurationProvider(common.DefaultConfigProvider())

    details := olog.LogWriterDetails {
        LogId: common.String("abc...xyz"),
        Provider: provider,
        Source: common.String("ServerA"),
        Type: common.String("ServerA.AccessLog"),
    }

    writer, err := olog.New(details)
    if err != nil {
        // do something
    }

    logger := log.New(writer, "", log.Lshortfile)
    logger.Println("this is a useful log entry")
}
```
