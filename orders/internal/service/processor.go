package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type PaymentStatusEvent struct {
	OrderID uuid.UUID `json:"order_id"`
	Status  string    `json:"status"`
}

type OrderProcessor struct {
	db     *sql.DB
	reader *kafka.Reader
}

func NewOrderProcessor(brokers string, db *sql.DB) *OrderProcessor {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    "payments.processed",
		GroupID:  "orders-group",
		MinBytes: 10e3,
		MaxBytes: 10e6,
	})
	return &OrderProcessor{db: db, reader: reader}
}

func (p *OrderProcessor) Start(ctx context.Context) {
	log.Println("Order Response Consumer started...")
	for {
		m, err := p.reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("Consumer error: %v", err)
			continue
		}

		// Обработка
		var event PaymentStatusEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Printf("Bad JSON: %v", err)
			continue
		}
		_, err = p.db.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", event.Status, event.OrderID)
		if err != nil {
			log.Printf("DB Error updating order %s: %v", event.OrderID, err)
		} else {
			log.Printf("Order %s updated to status: %s", event.OrderID, event.Status)

			// SendNotification(event.UserID, event.Status)
		}

		p.reader.CommitMessages(ctx, m)
	}
}
