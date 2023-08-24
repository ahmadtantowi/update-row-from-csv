package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/joho/godotenv"
)

var conn *pgx.Conn

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal(err)
	}

	conn = initDb()
	defer conn.Close(context.Background())

	ctx, line := context.Background(), 0
	csvFile, csvReader, setColIdx, whereColIdx := readCSV()
	defer csvFile.Close()

	for {
		line++
		rec, err := csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Printf("ERR on line %d: %v", line, err)
			continue
		}

		setVal := rec[setColIdx]
		whereVal := rec[whereColIdx]

		affected, err := updateRow(ctx, setVal, whereVal)
		if err != nil {
			log.Printf("ERR on line %d: %v", line, err)
			continue
		}
		log.Printf("Successfully update line %d with %d rows affected", line, affected)
	}
}

func getEnvOrFatal(envKey string) string {
	env := os.Getenv(envKey)
	if env == "" {
		log.Fatalf("%s must be provided!", envKey)
	}

	return env
}

func initDb() *pgx.Conn {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s",
		getEnvOrFatal("POSTGRE_UNAME"),
		getEnvOrFatal("POSTGRE_PWD"),
		getEnvOrFatal("POSTGRE_HOST"),
		getEnvOrFatal("POSTGRE_PORT"),
		getEnvOrFatal("POSTGRE_DB"),
	)

	cfg, err := pgx.ParseConfig(connStr)
	if err != nil {
		log.Fatal(err)
	}
	cfg.Tracer = &tracelog.TraceLog{
		Logger:   &qLogger{},
		LogLevel: tracelog.LogLevelDebug,
	}

	conn, err := pgx.ConnectConfig(context.Background(), cfg)
	if err != nil {
		log.Fatal(err)
	}

	return conn
}

func readCSV() (*os.File, *csv.Reader, int, int) {
	path := getEnvOrFatal("CSV_FILE_PATH")
	csvFile, err := os.Open(path)
	if err != nil {
		log.Fatal(err)
	}

	csvReader := csv.NewReader(csvFile)
	rec, err := csvReader.Read()
	if err == io.EOF {
		log.Fatal("CSV file is empty")
	}

	setColIdx := searchCSVColIdx(rec, "CSV_SET_COLUMN")
	whereColIdx := searchCSVColIdx(rec, "CSV_WHERE_COLUMN")

	return csvFile, csvReader, setColIdx, whereColIdx
}

func searchCSVColIdx(src []string, envKey string) int {
	col := getEnvOrFatal(envKey)
	colIdx := sort.SearchStrings(src, col)
	if colIdx == len(src) {
		log.Fatalf("Column %s is not found in CSV file!", col)
	}

	return colIdx
}

func updateRow(ctx context.Context, set, where string) (int64, error) {
	table := getEnvOrFatal("TABLE_NAME")
	setCol := getEnvOrFatal("TABLE_SET_COLUMN")
	whereCol := getEnvOrFatal("TABLE_WHERE_COLUMN")
	query := fmt.Sprintf(`UPDATE %s SET %s=$1 WHERE %s=$2`, table, setCol, whereCol)

	tag, err := conn.Exec(ctx, query, set, where)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected(), nil
}
