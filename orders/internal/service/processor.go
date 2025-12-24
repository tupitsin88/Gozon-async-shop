package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"time"

	"gozon/orders/internal/handler"

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
	hub    *handler.WSHub
}

func NewOrderProcessor(brokers string, db *sql.DB, hub *handler.WSHub) *OrderProcessor {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{brokers},
		Topic:    "payments.processed",
		GroupID:  "orders-group",
		MinBytes: 1,
		MaxBytes: 10e6,
		MaxWait:  10 * time.Millisecond,
	})
	return &OrderProcessor{db: db, reader: reader, hub: hub}
}

func (p *OrderProcessor) Start(ctx context.Context) {
	log.Println("Order Response Consumer started...")
	for {
		m, err := p.reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("Consumer error: %v", err)
			continue
		}

		var event PaymentStatusEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			continue
		}
		_, err = p.db.ExecContext(ctx, "UPDATE orders SET status = $1 WHERE id = $2", event.Status, event.OrderID)
		if err == nil {
			log.Printf("Order %s updated to status: %s", event.OrderID, event.Status)
			// Узнаем чей это заказ
			var userID string
			err := p.db.QueryRowContext(ctx, "SELECT user_id FROM orders WHERE id = $1", event.OrderID).Scan(&userID)
			if err == nil {
				p.hub.SendNotification(userID, map[string]string{
					"type":     "ORDER_UPDATED",
					"order_id": event.OrderID.String(),
					"status":   event.Status,
				})
			}
		}
		p.reader.CommitMessages(ctx, m)
	}
}
