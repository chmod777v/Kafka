package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"net/http"

	"github.com/segmentio/kafka-go"
)

type Hash struct {
	HachFunc int
	Value    any
}

func Handler(w http.ResponseWriter, r *http.Request) {
	var req Hash
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		slog.Error("Erro while receiving data", "ERROR", err.Error())
		return
	}
	slog.Info("RequestGet", "Data", req)
}

func main() {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9095"},
		Topic:   "my-topic",
	})
	defer writer.Close()

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("Hello, Kafka!"),
	})

	if err != nil {
		log.Fatal("Ошибка при отправке:", err)
	}

	http.HandleFunc("/", Handler)
	fmt.Println("server start: 'localhost:8080'")
	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		slog.Error("Error starting server", "ERROR", err.Error())
	}
}
