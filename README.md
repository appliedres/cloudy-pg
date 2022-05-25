# cloudy-pg
Postgresql implementation of Cloudy providers

## Example

```bash
go get "github.com/cloudy-pg"
```

```go

package main

import (
    "github.com/cloudy-pg"
    "github.com/cloudy"
)

func main() {
    ctx := cloudy.StartContext()

    localPG := "postgres://postgres:admin@localhost:5432/postgres"
    pgConfig := &PostgreSqlConfig{
        Connection: localPG,
        Database:   "sample",
        Table:      "sample",
    }

	ds := NewPostgreSqlJsonDataStore[tests.TestItem](ctx, pgConfig)

    testDoc := &TestItem{
		ID:   "12345",
		Name: "TEST",
	}

    fmt.Println("Saving")
	err := ds.Save(ctx, testDoc, testDoc.ID)
}

```
