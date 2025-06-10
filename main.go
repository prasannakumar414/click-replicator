package clickreplicator

import (
	"github.com/prasannakumar414/click-replicator/datasources/clickhouse"
	"github.com/prasannakumar414/click-replicator/models"
	"github.com/prasannakumar414/click-replicator/services/generator"
	"github.com/prasannakumar414/click-replicator/services/inserter"
	"github.com/prasannakumar414/click-replicator/services/replicator"
	"go.uber.org/zap"
)

type ClickReplicator struct {
	sourceConfig      models.ClickHouseConfig
	destinationConfig models.ClickHouseConfig
}

func NewClickReplicator(sourceConfig models.ClickHouseConfig, destinationConfig models.ClickHouseConfig) *ClickReplicator {
	return &ClickReplicator{
		sourceConfig:      sourceConfig,
		destinationConfig: destinationConfig,
	}
}

func (f *ClickReplicator) ReplicateDatabase() error {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	logger.Info("Starting ClickHouse Transformer")
	sourceConn, err := clickhouse.Connect(f.sourceConfig)
	if err != nil {
		logger.Error("could not connect to source clickhouse")
		return err
	}
	destinationConn, err := clickhouse.Connect(f.destinationConfig)
	if err != nil {
		logger.Error("could not connect to destination clickhouse")
		return err
	}
	sourceService := clickhouse.NewClickhouseService(sourceConn, logger, f.sourceConfig.Database)
	destinationService := clickhouse.NewClickhouseService(destinationConn, logger, f.destinationConfig.Database)
	generator := generator.NewGenerator(logger, f.sourceConfig)
	inserter := inserter.NewInserter(f.destinationConfig)
	replicator := replicator.NewReplicator(logger, sourceService, destinationService, generator, inserter)
	err = replicator.ReplicateDatabase()
	return err
}
