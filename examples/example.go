package main

import (
	"log"

	clickreplicator "github.com/prasannakumar414/click-replicator"
	"github.com/prasannakumar414/click-replicator/models"
)
func main() {
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
}