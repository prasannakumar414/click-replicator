package submitter

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/prasannakumar414/click-replicator/models"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type Submitter struct {
	clickhouseConfig models.ClickHouseConfig
}

func NewSubmitter(config models.ClickHouseConfig) *Submitter {
	return &Submitter{
		clickhouseConfig: config,
	}
}

func (submitter *Submitter) SubmitToClickhouse(ctx context.Context, logger *zap.Logger, table string, ingestionFilePath string, format string) error {
	commandTemplate := `
#!/bin/bash
set -euf -o pipefail
cat %s | clickhouse-client --host=%s \
				  --input_format_skip_unknown_fields=1 \
				  --database=%s \
				  --http_send_timeout=3600 \
				  --receive_timeout=30000 \
				  --tcp_keep_alive_timeout=2000 \
				  --http_receive_timeout=600 \
				  --max_insert_block_size=80000 \
				  --min_compress_block_size=262144 \
				  --max_memory_usage=55000000000 \
				  --query="INSERT INTO %s Format %s" \
				  --stacktrace
`
	submitCommand := fmt.Sprintf(commandTemplate, ingestionFilePath, submitter.clickhouseConfig.Host, submitter.clickhouseConfig.Database, table, format)
	logger.Info("Executing command", zap.String("command", submitCommand))
	cmd := exec.CommandContext(ctx, "bash", "-c", submitCommand)
	stdout := &strings.Builder{}
	stderr := &strings.Builder{}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Run(); err != nil {
		return &SubmissionError{
			Stdout:   stdout.String(),
			Stderr:   stderr.String(),
			URI:      ingestionFilePath,
			ExitCode: cmd.ProcessState.ExitCode(),
		}

	}
	err := os.Remove(ingestionFilePath)
	if err != nil {
		logger.Error("Error deleting file:", zap.Error(err))
		return err
	}

	logger.Info("File deleted successfully")
	return nil
}

type SubmissionError struct {
	Stdout   string
	Stderr   string
	URI      string
	ExitCode int
}

func (s SubmissionError) Error() string {
	return fmt.Sprintf("submission failed %s: exit code %d\nStdout:\n%s\nStderr:\n%s", s.URI, s.ExitCode, s.Stdout, s.Stderr)
}
