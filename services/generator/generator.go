package generator

import (
	"os"
	"os/exec"

	"github.com/prasannakumar414/click-replicator/models"
	"go.uber.org/zap"
)

type Generator struct {
	logger       *zap.Logger
	sourceConfig models.ClickHouseConfig
}

func NewGenerator(logger *zap.Logger, config models.ClickHouseConfig) *Generator {
	return &Generator{
		logger:       logger,
		sourceConfig: config,
	}
}

func (f *Generator) GenerateFileFromJSON(rows []string, fileName string) error {
	finalData := ""
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		f.logger.Error("error when opening file", zap.Error(err))
		return err
	}
	defer file.Close()
	for _, data := range rows {
		finalData = data + "\n" // Ensure each entry is on a new line
	}
	// Process the data as needed, e.g., write to a file or return it.
	_, err = file.WriteString(finalData)
	if err != nil {
		f.logger.Error("error when writing to file", zap.Error(err))
		return err
	}
	return nil
}

func (f *Generator) GenerateJSONlFromTable(tableName string) (string, error) {
	cmd := exec.Command("clickhouse-client","--host", f.sourceConfig.Host,"--query", "SELECT * FROM "+f.sourceConfig.Database+"."+tableName+" FORMAT JSONEachRow")
	fileName := tableName + "_final.jsonl"
	file, err := os.Create(fileName)
	if err != nil {
		return "",err
	}
	defer file.Close()

	cmd.Stdout = file

	err = cmd.Run()
	if err != nil {
		return "",err
	}

	return fileName, nil
}
