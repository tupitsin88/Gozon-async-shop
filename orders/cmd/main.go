package main

import (
	"context"
	"database/sql"
	"fmt"
	"gozon/orders/internal/broker"
	"gozon/orders/internal/service"
	"log"
	"net/http"
	"os"

	"gozon/orders/internal/handler"
	"gozon/orders/internal/storage"

	_ "github.com/lib/pq"

	_ "gozon/orders/docs"

	httpSwagger "github.com/swaggo/http-swagger"
)

// @title           Gozon Orders API
// @version         1.0
// @description     Сервис управления заказами.
// @description     Реализует паттерн Transactional Outbox для гарантированной отправки событий в Kafka.
// @description     Поддерживает Real-time уведомления клиентов через WebSockets.

// @host      localhost:8000
// @BasePath  /

// @externalDocs.description  OpenAPI
// @externalDocs.url          https://swagger.io/resources/open-api/
func main() {
	dbConnStr := os.Getenv("DATABASE_URL")
	if dbConnStr == "" {
		dbConnStr = "postgres://user:password@localhost:5432/orders_db?sslmode=disable"
	}
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err != nil {
		log.Fatalf("Cannot connect to DB: %v", err)
	}

	storage.InitSchema(db)
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	producer := broker.NewProducer(kafkaBrokers, "orders.created")
	defer producer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wsHub := handler.NewWSHub()
	http.HandleFunc("/ws", wsHub.HandleConnection)
	go service.StartRelay(ctx, db, producer)
	processor := service.NewOrderProcessor(kafkaBrokers, db, wsHub)
	go processor.Start(ctx)

	repo := storage.NewOrderRepository(db)
	h := handler.NewHandler(repo)
	http.HandleFunc("/api/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			h.CreateOrder(w, r)
		} else if r.Method == http.MethodGet {
			h.GetOrders(w, r)
		} else {
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	})

	http.HandleFunc("/swagger/", httpSwagger.WrapHandler)
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}
	fmt.Printf("Orders Service started on port %s\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
