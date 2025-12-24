package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type OrderCreatedEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	UserID  uuid.UUID `json:"user_id"`
	Amount  int64     `json:"amount"`
}

type PaymentProcessor struct {
	db     *sql.DB
	reader *kafka.Reader
}

func NewPaymentProcessor(brokers string, db *sql.DB) *PaymentProcessor {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    "orders.created",
		GroupID:  "payments-group",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  10 * time.Millisecond,
	})
	return &PaymentProcessor{db: db, reader: reader}
}

func (p *PaymentProcessor) Start(ctx context.Context) {
	log.Println("Payments Consumer started...")
	for {
		// Читаем сообщение из Kafka
		m, err := p.reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("Error fetching message: %v", err)
			continue
		}
		if err := p.processMessage(ctx, m); err != nil {
			log.Printf("Error processing message: %v", err)
		} else {
			p.reader.CommitMessages(ctx, m)
			log.Printf("Order processed successfully. Offset: %d", m.Offset)
		}
	}
}

func (p *PaymentProcessor) processMessage(ctx context.Context, m kafka.Message) error {
	var event OrderCreatedEvent
	if err := json.Unmarshal(m.Value, &event); err != nil {
		return fmt.Errorf("bad json: %w", err)
	}
	msgKey := event.OrderID
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Inbox
	// Проверяем, обрабатывали ли мы этот заказ
	var exists int
	err = tx.QueryRowContext(ctx, "SELECT 1 FROM inbox WHERE msg_id = $1", msgKey).Scan(&exists)
	if err == nil {
		log.Printf("Duplicate message ignored: %s", msgKey)
		return tx.Commit()
	}

	// Бизнес-логика
	// Пытаемся списать деньги. Возвращаем user_id, если списание прошло.
	// balance >= $2 гарантирует, что мы не уйдем в минус.
	var uid string
	err = tx.QueryRowContext(ctx, `
		UPDATE accounts 
		SET balance = balance - $1 
		WHERE user_id = $2 AND balance >= $1
		RETURNING user_id`,
		event.Amount, event.UserID,
	).Scan(&uid)

	status := "FINISHED"
	if err == sql.ErrNoRows {
		status = "CANCELLED"
		log.Printf("Payment failed for order %s: Insufficient funds or no user", event.OrderID)
	} else if err != nil {
		return fmt.Errorf("db error: %w", err)
	}

	// Outbox
	// Готовим ответ для Order Service
	replyPayload, _ := json.Marshal(map[string]interface{}{
		"order_id": event.OrderID,
		"status":   status,
	})
	_, err = tx.ExecContext(ctx, `
		INSERT INTO outbox (id, topic, payload) VALUES ($1, $2, $3)`,
		uuid.New(), "payments.processed", replyPayload,
	)
	if err != nil {
		return fmt.Errorf("outbox error: %w", err)
	}

	// Запись в Inbox
	_, err = tx.ExecContext(ctx, "INSERT INTO inbox (msg_id) VALUES ($1)", msgKey)
	if err != nil {
		return fmt.Errorf("inbox write error: %w", err)
	}
	return tx.Commit()
}
