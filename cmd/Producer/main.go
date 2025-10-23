package main

import (
	"encoding/json"
	"fmt"
	my_kafka "kafka/pkg/kafka"
	"log/slog"
	"net/http"

	"github.com/segmentio/kafka-go"
)

type Hash struct {
	HachFunc int
	Value    any
}
type Handler struct {
	writer *kafka.Writer
}

func (h *Handler) Handler(w http.ResponseWriter, r *http.Request) {
	var req Hash
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		slog.Error("Erro while receiving data", "ERROR", err.Error())
		return
	}
	slog.Info("RequestGet", "Data", req)
	value, _ := json.Marshal(req)
	slog.Info("Send kafka1")
	my_kafka.Write(h.writer, value)
	slog.Info("Send kafka2")
}

func main() {
	writer := my_kafka.NewWriter()
	defer writer.Close()

	handler := Handler{writer: writer}
	http.HandleFunc("/", handler.Handler)

	fmt.Println("server start: 'localhost:8080'")
	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		slog.Error("Error starting server", "ERROR", err.Error())
	}
}
