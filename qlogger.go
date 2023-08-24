package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/tracelog"
)

type qLogger struct{}

func (q *qLogger) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	if level == tracelog.LogLevelInfo && msg == "Query" {
		log.Printf("SQL:\n%s\nARGS:%v\n", data["sql"], data["args"])
	}
}
