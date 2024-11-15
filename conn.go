package conn

import (
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/microsoft/go-mssqldb"
)

type Config struct {
	User     string
	Password string
	Server   string
	Port     string
	Database string
}

type DB struct {
	DBconn *sqlx.DB
}

func openDB(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func Connect(config Config) (*DB, error) {
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s&encrypt=true&trustServerCertificate=true",
		url.QueryEscape(config.User),
		url.QueryEscape(config.Password),
		config.Server,
		config.Port,
		config.Database,
	)

	var counts int64
	for {
		connection, err := openDB(dsn)
		if err != nil {
			log.Println("Database not yet ready...")
			counts++
		} else {
			log.Println("Connected to the database!")
			return &DB{DBconn: connection}, nil
		}

		if counts > 10 {
			log.Println("Error connecting to the database:", err)
			return nil, err
		}

		log.Println("Retrying connection in 2 seconds...")
		time.Sleep(2 * time.Second)
		continue
	}
}
