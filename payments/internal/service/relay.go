package service

import (
	"context"
	"database/sql"
	"gozon/payments/internal/broker"
	"log"
	"time"

	"github.com/google/uuid"
)

type OutboxMsg struct {
	ID      uuid.UUID
	Topic   string
	Payload []byte
}

// StartRelay запускает бесконечный цикл проверки outbox
func StartRelay(ctx context.Context, db *sql.DB, producer *broker.Producer) {
	ticker := time.NewTicker(500 * time.Millisecond) // Проверяем каждые 0.5 сек
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Println("Stopping Message Relay...")
			return
		case <-ticker.C:
			processBatch(ctx, db, producer)
		}
	}
}

func processBatch(ctx context.Context, db *sql.DB, producer *broker.Producer) {
	// Используем FOR UPDATE SKIP LOCKED, чтобы если запустим несколько инстансов,
	// они не обрабатывали одни и те же строки.
	rows, err := db.QueryContext(ctx, `
		SELECT id, topic, payload 
		FROM outbox 
		WHERE processed = false 
		ORDER BY created_at ASC 
		LIMIT 10
        FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		log.Printf("Error reading outbox: %v", err)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var msg OutboxMsg
		if err := rows.Scan(&msg.ID, &msg.Topic, &msg.Payload); err != nil {
			log.Printf("Error scanning outbox msg: %v", err)
			continue
		}
		// Отправляем в Kafka
		err := producer.SendMessage(ctx, msg.ID.String(), msg.Payload)
		if err != nil {
			log.Printf("Failed to send to Kafka: %v", err)
			continue
		}

		// Помечаем как обработанное (processed = true)
		_, err = db.ExecContext(ctx, "UPDATE outbox SET processed = true WHERE id = $1", msg.ID)
		if err != nil {
			log.Printf("Failed to update outbox status: %v", err)
		} else {
			log.Printf("Message %s sent to Kafka", msg.ID)
		}
	}
}
