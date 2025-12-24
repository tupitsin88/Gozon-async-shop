package main

import (
	"context"
	"database/sql"
	"gozon/payments/internal/handler"
	"log"
	"net/http"
	"os"

	_ "gozon/payments/docs"
	"gozon/payments/internal/service"
	"gozon/payments/internal/storage"

	"gozon/payments/internal/broker"

	_ "github.com/lib/pq"
	httpSwagger "github.com/swaggo/http-swagger"
)

// @title           Gozon Payments API
// @version         1.0
// @description     Сервис платежей с поддержкой идемпотентности (Inbox Pattern).
// @description     Обрабатывает транзакции, хранит балансы пользователей и защищает от двойных списаний.

// @host      localhost:8081
// @BasePath  /

// @externalDocs.description  OpenAPI
// @externalDocs.url          https://swagger.io/resources/open-api/
func main() {
	dbConnStr := os.Getenv("DATABASE_URL")
	if dbConnStr == "" {
		dbConnStr = "postgres://user:password@localhost:5433/payments_db?sslmode=disable"
	}
	db, err := sql.Open("postgres", dbConnStr)
	if err != nil {
		log.Fatal(err)
	}
	storage.InitSchema(db)

	// Kafka Consumer
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "localhost:9092"
	}
	processor := service.NewPaymentProcessor(kafkaBrokers, db)
	go processor.Start(context.Background())
	// Kafka Producer + Relay
	producer := broker.NewProducer(kafkaBrokers, "payments.processed")
	go service.StartRelay(context.Background(), db, producer)

	// HTTP Handler
	h := handler.NewHandler(db)

	// Маршруты
	http.HandleFunc("/api/payments/create_account", h.CreateAccount)
	http.HandleFunc("/api/payments/deposit", h.Deposit)
	http.HandleFunc("/api/payments/balance", h.GetBalance)

	// Swagger
	http.HandleFunc("/swagger/", httpSwagger.WrapHandler)

	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8081"
	}

	log.Printf("Payments Service started on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
