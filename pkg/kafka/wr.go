package my_kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func Write(writer *kafka.Writer, value []byte) error {
	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: value,
	})
	if err != nil {
		fmt.Println("ERROR:", err.Error())
		return err
	}
	return nil
}
func NewWriter() *kafka.Writer {
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9095"},
		Topic:   "my-topic",

		BatchSize: 1,
	})
	return writer
}

//

func NewReader() *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9095"},
		Topic:   "my-topic",
		GroupID: "my-groupID",
	})
	return reader
}
func Read(reader *kafka.Reader) (string, error) {
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		fmt.Println("Ошибка при получении:", err)
		return "", err
	}
	fmt.Println(string(msg.Value))
	return string(msg.Value), nil
}
