package normalizer

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/prasannakumar414/flatsert/tools"
	"github.com/prasannakumar414/flatsert/utils"
	"go.uber.org/zap"
)

type DataSource interface {
	GetAllTables(ctx context.Context, database string) ([]string, error)
	GetRowCount(ctx context.Context, database string, tableName string) (uint64, error)
	IsTableExists(ctx context.Context, database string, tableName string) (bool, error)
	CreateClickhouseTable(ctx context.Context, database string, tableName string, rowJson string) error
	AddColumns(ctx context.Context, database string, tableName string, columns []string) error
	Insert2(ctx context.Context, database string, tableName string, rowData []map[string]interface{}) error
	GetColumnNames(ctx context.Context, database string, tableName string) ([]string, error)
	CreateTableFromJSONFile(ctx context.Context, database string, tableName string, orderBy string, fileName string) error
	CreateDatabase(ctx context.Context, database string) error
	OptimizeTable(ctx context.Context, database string, tableName string) error
	GetLatestSyncTime(ctx context.Context, database string, tableName string) (*time.Time, error)
	GetRowJsonsWithLimit(ctx context.Context, database string, tableName string, columnName string, format string, limit int, offset int) ([]string, error)
	CreateTableFromJSONData(ctx context.Context, database string, tableName string, orderBy string, rows []string) error
}

type Submitter interface {
	SubmitToClickhouse(ctx context.Context, logger *zap.Logger, database, table, ingestionFilePath, format string) error
}

type Finalizer interface {
	FinalizeJSONData(rows []string, fileName string) error
}

type Normalizer struct {
	dataSource DataSource
	logger     *zap.Logger
	finalizer  Finalizer
	submitter  Submitter
}

func NewNormalizer(logger *zap.Logger, dataSource DataSource, finalizer Finalizer, submitter Submitter) *Normalizer {
	return &Normalizer{
		dataSource: dataSource,
		logger:     logger,
		finalizer:  finalizer,
		submitter:  submitter,
	}
}

func (n Normalizer) GetAllColumnNameAndTypes(database string, table string, rowJson string) (string, []string, error) {
	jsonData := []byte(rowJson)
	var dataMap map[string]interface{}
	if err := json.Unmarshal(jsonData, &dataMap); err != nil {
		fmt.Println("Error unmarshalling JSON:", err)
		return "", nil, err
	}
	flattenedJson, err := tools.Flatten(dataMap, "", tools.UnderscoreStyle)
	if err != nil {
		fmt.Println("Error flattening JSON:", err)
		return "", nil, err
	}

	columnsCreationString := ""
	columnsString := []string{}
	index := 0
	for key, value := range flattenedJson {
		inferredType := utils.GetDataType(utils.ConvertToString(value))
		if key == "id" {
			inferredType = "String" // Ensure id is always String
		}
		if key != "id" {
			columnsCreationString += fmt.Sprintf("%s Nullable(%s), ", key, inferredType)
		} else {
			columnsCreationString += fmt.Sprintf("%s %s, ", key, inferredType) // Ensure id is always String
		}
		columnsString = append(columnsString, key)
		index++
	}
	return columnsCreationString, columnsString, nil
}

func (n *Normalizer) ReplicateDatabase(sourceDatabase string, columnName string, newDatabase string) error {
	// Normalization logic for the database
	// We must fetch all the tables of the database (In our case we are normalizng JSON data)
	// Create Respective jsonl files with data
	// Infer schema from the jsonl files and Insert in to the respective Tables.

	n.logger.Info("Replication V2 has begun")

	tables, err := n.dataSource.GetAllTables(context.Background(), sourceDatabase)

	if err != nil {
		n.logger.Error("Error fetching tables", zap.Error(err))
		return err
	}

	err = n.dataSource.CreateDatabase(context.Background(), newDatabase)
	if err != nil {
		n.logger.Error("Error when creating database", zap.Error(err))
	}
	for _, table := range tables {

		newTableName := utils.RemovePrefix(table, "default_raw__stream_")

		tableExists, err := n.dataSource.IsTableExists(context.Background(), newDatabase, newTableName)

		if err != nil {
			n.logger.Error("Error checking if table exists", zap.String("table", newTableName), zap.Error(err))
			continue
		}
		n.logger.Info("Normalizing table", zap.String("table", table))
		uRowCount, err := n.dataSource.GetRowCount(context.Background(), sourceDatabase, table)
		rowCount := int(uRowCount)
		if err != nil {
			n.logger.Error("Error fetching row count", zap.String("table", table), zap.Error(err))
			continue
		}
		if rowCount == 0 {
			n.logger.Info("Skipping empty table", zap.String("table", table))
			continue
		}

		database := newDatabase
		extractedRowCount := 0

		if newTableName == "contacts" {
			continue
		}

		if tableExists {

			uCurrentRowCount, err := n.dataSource.GetRowCount(context.Background(), database, newTableName)
			if err != nil {
				n.logger.Error("Error fetching row count", zap.String("table", table), zap.Error(err))
				continue
			}
			currentRowCount := int(uCurrentRowCount)

			if currentRowCount >= rowCount {
				n.logger.Info("Skipping the table since current table contains all rows")
				continue
			}
		}

		fileName := database + "_" + newTableName + "_final.jsonl"

		for extractedRowCount < rowCount {
			rows, err := n.dataSource.GetRowJsonsWithLimit(context.Background(), sourceDatabase, table, columnName, "JSON", 5000, extractedRowCount)
			if err != nil {
				n.logger.Error("Error when getting row jsons from clickhouse", zap.Error(err))
				return err
			}
			extractedRowCount += len(rows)
			if !tableExists {
				n.logger.Info("uses 1000 rows for schema inference")
				err = n.dataSource.CreateTableFromJSONData(context.Background(), database, newTableName, "id", rows[:10])
				if err != nil {
					n.logger.Error("Error when creating table from JSON data", zap.Error(err))
					err = n.dataSource.CreateClickhouseTable(context.Background(), database, newTableName, rows[0])
					if err != nil {
						n.logger.Error("Error when creating table from JSON data", zap.Error(err))
						continue
					}
				}
				tableExists = true
			}
			err = n.finalizer.FinalizeJSONData(rows, fileName)
			if err != nil {
				n.logger.Error("Error when creating file", zap.Error(err))
				return err
			}
			n.logger.Info("extracted rows from database", zap.Int("extracted count", extractedRowCount))
		}
		err = n.submitter.SubmitToClickhouse(context.Background(), n.logger, database, newTableName, fileName, "JSONEachRow")
		if err != nil {
			n.logger.Error("Error when Inserting to Clickhouse", zap.Error(err))
			return err
		}
		err = n.dataSource.OptimizeTable(context.Background(), database, newTableName)
		if err != nil {
			n.logger.Error("Error when optimizing table", zap.Error(err))
		}
		n.logger.Info("Successfully Replicated " + newTableName)
	}
	return nil
}
