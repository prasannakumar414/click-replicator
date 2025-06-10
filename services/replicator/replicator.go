package replicator

import (
	"context"

	"go.uber.org/zap"
)

type DataSource interface {
	GetAllTables(ctx context.Context) ([]string, error)
	GetRowCount(ctx context.Context, tableName string) (uint64, error)
	IsTableExists(ctx context.Context, tableName string) (bool, error)
	CreateClickhouseTable(ctx context.Context, tableName string, rowJson string) error
	AddColumns(ctx context.Context, tableName string, columns []string) error
	GetColumnNames(ctx context.Context, tableName string) ([]string, error)
	CreateTableFromJSONFile(ctx context.Context, tableName string, orderBy string, fileName string) error
	CreateDatabase(ctx context.Context) error
	OptimizeTable(ctx context.Context, tableName string) error
	GetRowJsonsWithLimit(ctx context.Context, tableName string, format string, limit int, offset int) ([]string, error)
	CreateTableFromJSONData(ctx context.Context, tableName string, orderBy string, rows []string) error
}

type Inserter interface {
	InsertToClickhouse(ctx context.Context, logger *zap.Logger, table string, filePath string, format string) error
}

type Generator interface {
	GenerateFileFromJSON(rows []string, fileName string) error
}

type Replicator struct {
	source      DataSource
	destination DataSource
	logger      *zap.Logger
	generator   Generator
	inserter   Inserter
}

func NewReplicator(logger *zap.Logger, source DataSource, destination DataSource, generator Generator, inserter Inserter) *Replicator {
	return &Replicator{
		source:      source,
		destination: destination,
		logger:      logger,
		generator:   generator,
		inserter:   inserter,
	}
}

func (n *Replicator) ReplicateDatabase() error {
	// Replication logic for the database
	// We must fetch all the tables of the database (In our case we are normalizng JSON data)
	// Create Respective jsonl files with data
	// Insert in to the respective source tables.

	n.logger.Info("Replication has begun")

	tables, err := n.source.GetAllTables(context.Background())

	if err != nil {
		n.logger.Error("Error fetching tables", zap.Error(err))
		return err
	}

	err = n.destination.CreateDatabase(context.Background())
	if err != nil {
		n.logger.Error("Error when creating database", zap.Error(err))
	}
	for _, table := range tables {
		tableExists, err := n.destination.IsTableExists(context.Background(), table)

		if err != nil {
			n.logger.Error("Error checking if table exists", zap.String("table", table), zap.Error(err))
			continue
		}
		n.logger.Info("Replicating table", zap.String("table", table))
		uRowCount, err := n.source.GetRowCount(context.Background(), table)
		rowCount := int(uRowCount)
		if err != nil {
			n.logger.Error("Error fetching row count", zap.String("table", table), zap.Error(err))
			continue
		}
		if rowCount == 0 {
			n.logger.Info("Skipping empty table", zap.String("table", table))
			continue
		}

		extractedRowCount := 0

		if tableExists {

			uCurrentRowCount, err := n.destination.GetRowCount(context.Background(), table)
			if err != nil {
				n.logger.Error("Error fetching row count", zap.String("table", table), zap.Error(err))
				continue
			}
			currentRowCount := int(uCurrentRowCount)

			if currentRowCount == rowCount {
				n.logger.Info("Skipping the table since current table contains all rows")
				continue
			}
		}

		fileName := table + "_final.jsonl"

		for extractedRowCount < rowCount {
			rows, err := n.source.GetRowJsonsWithLimit(context.Background(), table, "JSON", 5000, extractedRowCount)
			if err != nil {
				n.logger.Error("Error when getting row jsons from clickhouse", zap.Error(err))
				return err
			}
			extractedRowCount += len(rows)
			if !tableExists {
				n.logger.Info("uses 1000 rows for schema inference")
				err = n.destination.CreateTableFromJSONData(context.Background(), table, "tuple()", rows[:1])
				if err != nil {
					n.logger.Error("Error when creating table from JSON data", zap.Error(err))
					err = n.destination.CreateClickhouseTable(context.Background(), table, rows[0])
					if err != nil {
						n.logger.Error("Error when creating table from JSON data", zap.Error(err))
						continue
					}
				}
				tableExists = true
			}
			err = n.generator.GenerateFileFromJSON(rows, fileName)
			if err != nil {
				n.logger.Error("Error when creating file", zap.Error(err))
				return err
			}
			n.logger.Info("extracted rows from database", zap.Int("extracted count", extractedRowCount))
		}
		err = n.inserter.InsertToClickhouse(context.Background(), n.logger, table, fileName, "JSONEachRow")
		if err != nil {
			n.logger.Error("Error when Inserting to Clickhouse", zap.Error(err))
			return err
		}
		n.logger.Info("Successfully Replicated " + table)
	}
	return nil
}
