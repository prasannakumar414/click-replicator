package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prasannakumar414/flatsert/tools"

	"go.uber.org/zap"
)

type ClickhouseService struct {
	Conn   driver.Conn
	logger *zap.Logger
}

func NewClickhouseService(conn driver.Conn, logger *zap.Logger) *ClickhouseService {
	return &ClickhouseService{
		Conn:   conn,
		logger: logger,
	}
}

func (service *ClickhouseService) GetAllTables(ctx context.Context, database string) ([]string, error) {
	var tables []string
	//Fixme: The query should be modified to fetch the correct table names
	query := fmt.Sprintf("SELECT name FROM system.tables WHERE database = '%s'", database)

	rows, err := service.Conn.Query(ctx, query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, err
		}
		tables = append(tables, tableName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (service *ClickhouseService) IsTableExists(ctx context.Context, database string, tableName string) (bool, error) {
	query := fmt.Sprintf("SELECT count() FROM system.tables WHERE name = '%s' AND database = '%s'", tableName, database)

	var count uint64
	if err := service.Conn.QueryRow(ctx, query).Scan(&count); err != nil {
		fmt.Println("Error executing query:", err)
		return false, err
	}

	return count > 0, nil
}

func (service *ClickhouseService) GetRowCount(ctx context.Context, database string, tableName string) (uint64, error) {
	query := fmt.Sprintf("SELECT count() FROM %s.%s", database, tableName)

	var count uint64
	if err := service.Conn.QueryRow(ctx, query).Scan(&count); err != nil {
		fmt.Println("Error executing query:", err)
		return 0, err
	}

	return count, nil
}

func (service *ClickhouseService) GetRowJsons(ctx context.Context, database string, tableName string, columnName string, condition string, format string) ([]string, error) {
	query := fmt.Sprintf("SELECT %s FROM %s.%s %s FORMAT %s", columnName, database, tableName, condition, format)

	rows, err := service.Conn.Query(ctx, query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		return nil, err
	}
	defer rows.Close()

	var jsonData []string
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		jsonData = append(jsonData, data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jsonData, nil
}

func (service *ClickhouseService) GetRowJsonsWithLimit(ctx context.Context, database string, tableName string, columnName string, format string, limit int, offset int) ([]string, error) {
	query := fmt.Sprintf("SELECT %s FROM %s.%s limit %d offset %d FORMAT %s", columnName, database, tableName, limit, offset, format)

	rows, err := service.Conn.Query(ctx, query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		return nil, err
	}
	defer rows.Close()

	var jsonData []string
	for rows.Next() {
		var data string
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		jsonData = append(jsonData, data)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return jsonData, nil
}

func (service *ClickhouseService) CreateClickhouseTable(ctx context.Context, database string, tableName string, rowJson string) error {
	columns, _, err := service.GetAllColumnNameAndTypes(database, tableName, rowJson)
	if err != nil {
		fmt.Println("Error getting column names and types:", err)
	}
	// Construct the CREATE TABLE query
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s) ENGINE = MergeTree() ORDER BY tuple()", database, tableName, columns)

	if err := service.Conn.Exec(ctx, query); err != nil {
		fmt.Println("Error creating table:", err)
		return err
	}

	return nil
}

func (cs ClickhouseService) GetAllColumnNameAndTypes(database string, table string, rowJson string) (string, []string, error) {
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
	for key := range flattenedJson {
		inferredType := "String"
		columnsCreationString += fmt.Sprintf("%s Nullable(%s), ", key, inferredType)
		columnsString = append(columnsString, key)
		index++
	}
	return columnsCreationString, columnsString, nil
}

func (cs ClickhouseService) AlterTableColumnType(ctx context.Context, database string, tableName string, columnName string, newType string) error {
	query := fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s Nullable(%s)", tableName, columnName, newType)
	if err := cs.Conn.Exec(ctx, query); err != nil {
		fmt.Println("Error altering column type:", err)
		return err
	}
	return nil
}

func (cs ClickhouseService) AddColumns(ctx context.Context, database string, tableName string, columns []string) error {
	for _, column := range columns {
		query := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN %s Nullable(String)", database, tableName, column)
		if err := cs.Conn.Exec(ctx, query); err != nil {
			fmt.Println("Error adding column:", err)
			return err
		}
	}
	return nil
}

// Get column names from the Clickhouse table
func (cs ClickhouseService) GetColumnNames(ctx context.Context, database string, tableName string) ([]string, error) {
	query := fmt.Sprintf("SELECT name FROM system.columns WHERE table = '%s' AND database = '%s'", tableName, database)

	rows, err := cs.Conn.Query(ctx, query)
	if err != nil {
		fmt.Println("Error executing query:", err)
		return nil, err
	}
	defer rows.Close()

	var columnNames []string
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, err
		}
		columnNames = append(columnNames, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columnNames, nil
}

func (cs ClickhouseService) CreateTableFromJSONFile(ctx context.Context, database string, tableName string, orderBy string, fileName string) error {
	query := "CREATE TABLE %s.%s ENGINE = MergeTree ORDER BY %s AS SELECT * FROM file('%s') SETTINGS schema_inference_make_columns_nullable = 0"
	query = fmt.Sprintf(query, database, tableName, orderBy, fileName)
	err := cs.Conn.Exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (cs ClickhouseService) CreateTableFromJSONData(ctx context.Context, database string, tableName string, orderBy string, rows []string) error {
	rowsData := ""
	for _, s := range rows {
		s = strings.ReplaceAll(s, "'", "\\'")
		rowsData += s + " "
	}
	query := "CREATE TABLE %s.%s ENGINE = MergeTree ORDER BY %s AS SELECT * FROM format(JSONEachRow, '%s') SETTINGS schema_inference_make_columns_nullable = 0"
	query = fmt.Sprintf(query, database, tableName, orderBy, rowsData)
	err := cs.Conn.Exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (cs ClickhouseService) CreateDatabase(ctx context.Context, database string) error {
	query := "CREATE DATABASE IF NOT EXISTS %s;"
	query = fmt.Sprintf(query, database)
	err := cs.Conn.Exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}

func (cs ClickhouseService) OptimizeTable(ctx context.Context, database string, tableName string) error {
	query := "OPTIMIZE TABLE %s.%s"
	query = fmt.Sprintf(query, database, tableName)
	err := cs.Conn.Exec(ctx, query)
	if err != nil {
		return err
	}
	return nil
}
