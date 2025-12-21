package storage

import (
	"database/sql"
	"log"
)

// InitSchema создает таблицы, если их нет.
func InitSchema(db *sql.DB) {
	query := `
    CREATE TABLE IF NOT EXISTS orders (
        id UUID PRIMARY KEY,
        user_id UUID NOT NULL,
        amount BIGINT NOT NULL,
        description TEXT,
        status VARCHAR(50) NOT NULL, -- NEW, PAID, FAILED
        created_at TIMESTAMP DEFAULT NOW()
    );

    CREATE TABLE IF NOT EXISTS outbox (
        id UUID PRIMARY KEY,
        topic VARCHAR(100) NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        processed BOOLEAN DEFAULT FALSE
    );
    `
	_, err := db.Exec(query)
	if err != nil {
		log.Fatalf("Ошибка инициализации схемы БД: %v", err)
	}
	log.Println("Схема БД успешно инициализирована (Orders + Outbox)")
}
