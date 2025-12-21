package storage

import (
	"database/sql"
	"log"
)

func InitSchema(db *sql.DB) {
	query := `
    CREATE TABLE IF NOT EXISTS accounts (
        user_id UUID PRIMARY KEY,
        balance BIGINT NOT NULL CHECK (balance >= 0)
    );

    CREATE TABLE IF NOT EXISTS inbox (
        msg_id UUID PRIMARY KEY,
        processed_at TIMESTAMP DEFAULT NOW()
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
		log.Fatalf("Ошибка схемы Payments: %v", err)
	}
	log.Println("Схема Payments (Accounts + Inbox + Outbox) готова")
}
