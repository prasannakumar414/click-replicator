# Click-Replicator
A Go package to replicate data from one clickhouse server to another, with support to infer schema.

- Generally we may face issue when we need to replicate data from a database at one clickhouse server to another database hosted at another server, this package solves this issue by replicating the database and all the tables in it. 
- This package also provides type inference support i.e types of the columns are preserved by inferring type during creation of table.

Installation:

``` go get -u github.com/prasannakumar414/click-replicator ```

Example: (refer examples directory for the file to run)

``` 
 sourceConfig := models.ClickHouseConfig{
      Host: "localhost",
      Port: 9000,  
      Username: "default",
      Password: "",
      Database: "default",
    }
    destinationConfig := models.ClickHouseConfig{
        Host: "localhost",
        Port: 9000,  
        Username: "default",
        Password: "",
        Database: "destination",
      }
    replicator := clickreplicator.NewClickReplicator(sourceConfig, destinationConfig)
    err := replicator.ReplicateDatabase()
    if err != nil {
        log.Fatal(err)
    }

```
