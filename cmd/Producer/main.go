package main

import (
	"encoding/json"
	"fmt"
	my_kafka "kafka/pkg/kafka"
	"log/slog"
	"net/http"

	"github.com/segmentio/kafka-go"
)

type Handler struct {
	writer *kafka.Writer
}

func (h *Handler) Handler(w http.ResponseWriter, r *http.Request) {
	var req my_kafka.Hash
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		slog.Error("Erro while receiving data", "ERROR", err.Error())
		return
	}
	slog.Info("RequestGet", "Data", req)
	value, _ := json.Marshal(req)
	my_kafka.Write(h.writer, value)
}

func main() {
	writer := my_kafka.NewWriter()
	defer writer.Close()

	handler := Handler{writer: writer}
	http.HandleFunc("/", handler.Handler)

	fmt.Println("server start: '0.0.0.0:8080'")
	if err := http.ListenAndServe("0.0.0.0:8080", nil); err != nil {
		slog.Error("Error starting server", "ERROR", err.Error())
	}
}
