package flatsert

import (
	"github.com/prasannakumar414/flatsert/datasources/clickhouse"
	"github.com/prasannakumar414/flatsert/models"
	"github.com/prasannakumar414/flatsert/services/finalizer"
	"github.com/prasannakumar414/flatsert/services/replicator"
	"github.com/prasannakumar414/flatsert/services/submitter"
	"go.uber.org/zap"
)

type FlatSert struct {
	config         models.ClickHouseConfig
	sourceDatabase string
	columnName     string
	newDatabase    string
}

func NewFlatsert(config models.ClickHouseConfig, sourceDatabase string, columnName string, newDatabase string) *FlatSert {
	return &FlatSert{
		config:         config,
		sourceDatabase: sourceDatabase,
		columnName:     columnName,
		newDatabase:    newDatabase,
	}
}

func (f *FlatSert) ReplicateJSONtoSchema() error {
	logger, _ := zap.NewProduction()
	defer logger.Sync() // flushes buffer, if any
	logger.Info("Starting ClickHouse Transformer")
	conn, err := clickhouse.Connect(f.config)
	if err != nil {
		logger.Error("could not connect to clickhouse")
		return err
	}
	clickhouseService := clickhouse.NewClickhouseService(conn, logger)
	finalize := finalizer.NewFinalizer(logger, clickhouseService)
	submitter := submitter.NewSubmitter(f.config.Host)
	normalizer := replicator.NewReplicator(logger, clickhouseService, finalize, submitter)
	err = normalizer.ReplicateDatabase(f.sourceDatabase, f.columnName, f.newDatabase)
	return err
}
