package main

import (
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func HachFunc(hashfunc int, value []byte) ([]byte, error) {
	switch hashfunc {
	case 0:
		data := sha1.Sum(value)
		return data[:], nil

	case 1:
		data := sha256.Sum256(value)
		return data[:], nil

	case 2:
		data := sha512.Sum512(value)
		return data[:], nil

	default:
		return nil, fmt.Errorf("no hashfunc %d", hashfunc)
	}
}

func main() {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9095"},
		Topic:   "my-topic",
		GroupID: "my-groupID",
	})
	defer reader.Close()

	fmt.Println("Read")
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		log.Fatal("Ошибка при получении:", err)
	}
	fmt.Println(string(msg.Value))

	value, err := HachFunc(2, []byte("hellow"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(value)
}
