package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	_ "github.com/go-sql-driver/mysql"
)

var (
	instance *sql.DB
	once     sync.Once
)

// Get returns the singleton DB connection pool.
func Get() *sql.DB {
	once.Do(func() {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
			os.Getenv("DB_USER"),
			os.Getenv("DB_PASS"),
			os.Getenv("DB_HOST"),
			os.Getenv("DB_PORT"),
			os.Getenv("DB_NAME"),
		)

		conn, err := sql.Open("mysql", dsn)
		if err != nil {
			log.Fatalf("[db] failed to open connection: %v", err)
		}

		if err := conn.Ping(); err != nil {
			log.Fatalf("[db] failed to ping: %v", err)
		}

		conn.SetMaxOpenConns(10)
		conn.SetMaxIdleConns(5)

		log.Println("[db] connected")
		instance = conn
	})

	return instance
}

// Close closes the DB connection pool.
// Call this on graceful shutdown e.g. defer db.Close() in main.
func Close() {
	if instance != nil {
		if err := instance.Close(); err != nil {
			log.Printf("[db] error closing connection: %v", err)
			return
		}
		log.Println("[db] connection closed")
	}
}