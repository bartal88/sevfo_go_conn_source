package conn

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/joho/godotenv"
	_ "github.com/microsoft/go-mssqldb" // MSSQL Driver
)

// Config holds the database connection details.
type Config struct {
	User     string
	Password string
	Server   string
	Port     string
	Database string
}

// DB wraps the sqlx.DB connection.
type DB struct {
	DBconn *sqlx.DB
}

// DbConfig loads database configuration from environment variables.
func DbConfig() (Config, error) {
	// Load .env file if it exists
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: .env file not found, relying on environment variables")
	}

	// Fetch configuration from environment variables
	config := Config{
		User:     os.Getenv("USER"),
		Password: os.Getenv("PASSWORD"),
		Server:   os.Getenv("SRV"),
		Port:     os.Getenv("PORT"),
		Database: os.Getenv("DB"),
	}

	// Validate required fields
	if config.User == "" || config.Password == "" || config.Server == "" || config.Port == "" || config.Database == "" {
		return Config{}, fmt.Errorf("missing required environment variables")
	}

	return config, nil
}

// openDB opens a database connection using the provided DSN.
func openDB(dsn string) (*sqlx.DB, error) {
	db, err := sqlx.Connect("sqlserver", dsn)
	if err != nil {
		return nil, err
	}
	return db, nil
}

// Connect establishes a database connection with retries.
func Connect(config Config) (*DB, error) {
	// Construct the DSN (Data Source Name) for the database.
	dsn := fmt.Sprintf("sqlserver://%s:%s@%s:%s?database=%s&encrypt=true&trustServerCertificate=true",
		url.QueryEscape(config.User),
		url.QueryEscape(config.Password),
		config.Server,
		config.Port,
		config.Database,
	)

	println(dsn)

	var retryCount int64
	for {
		// Attempt to open the database connection
		connection, err := openDB(dsn)
		if err != nil {
			log.Println("Database not yet ready...")
			retryCount++
		} else {
			log.Println("Connected to the database!")
			return &DB{DBconn: connection}, nil
		}

		// Exit if retries exceed the limit
		if retryCount > 10 {
			log.Println("Error connecting to the database:", err)
			return nil, err
		}

		// Retry after a short delay
		log.Println("Retrying connection in 2 seconds...")
		time.Sleep(2 * time.Second)
		continue
	}
}
