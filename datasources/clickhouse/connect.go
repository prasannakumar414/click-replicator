package clickhouse

import (
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/prasannakumar414/flatsert/models"
)

func Connect(clickhouseConfig models.ClickHouseConfig) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", clickhouseConfig.Host, clickhouseConfig.Port)},
		Auth: clickhouse.Auth{
			Username: clickhouseConfig.Username,
			Password: clickhouseConfig.Password,
		},
		Debug:           false,
		DialTimeout:     time.Second * 30,
		ReadTimeout:     time.Second * time.Duration(1800),
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		Settings: clickhouse.Settings{
			"distributed_ddl_task_timeout":      1800,
			"replication_alter_columns_timeout": 1800,
			"max_query_size":                    1000000000,
		},
	})
	if err != nil {
		return nil, err
	}
	return conn, nil
}
