package generator

import (
	"os"

	"go.uber.org/zap"
)

type Generator struct {
	logger *zap.Logger
}

func NewGenerator(logger *zap.Logger) *Generator {
	return &Generator{
		logger: logger,
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
