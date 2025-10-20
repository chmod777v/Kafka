package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
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
	http.HandleFunc("/", Handler)

	if err := http.ListenAndServe("localhost:8080", nil); err != nil {
		slog.Error("Error starting server", "ERROR", err.Error())
	}
}
