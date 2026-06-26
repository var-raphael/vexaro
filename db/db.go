package db

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
)

var (
	instance *sql.DB
	once     sync.Once
)

func Get() *sql.DB {
	once.Do(func() {
		dsn := buildDSN()

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

func buildDSN() string {
	user := os.Getenv("DB_USER")
	pass := os.Getenv("DB_PASS")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	name := os.Getenv("DB_NAME")
	env  := os.Getenv("ENV")
	

	if env == "production" {
		certPool := x509.NewCertPool()
		caCert := os.Getenv("DB_CA_CERT")
		if caCert != "" {
			certPool.AppendCertsFromPEM([]byte(caCert))
		}

		tlsCfg := &tls.Config{
			RootCAs: certPool,
		}

		mysql.RegisterTLSConfig("aiven", tlsCfg)

		return fmt.Sprintf(
			"%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4&tls=aiven",
			user, pass, host, port, name,
		)
	}

	// local — no SSL
	return fmt.Sprintf(
		"%s:%s@tcp(%s:%s)/%s?parseTime=true&charset=utf8mb4",
		user, pass, host, port, name,
	)
}

func Close() {
	if instance != nil {
		if err := instance.Close(); err != nil {
			log.Printf("[db] error closing connection: %v", err)
			return
		}
		log.Println("[db] connection closed")
	}
}