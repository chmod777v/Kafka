package my_kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type Hash struct {
	Name     string
	HachFunc int
	Value    any
}

func (h *Hash) ValueToBytes() ([]byte, error) {
	switch v := h.Value.(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(strconv.Itoa(v)), nil
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case bool:
		return []byte(strconv.FormatBool(v)), nil
	default:
		return json.Marshal(v)
	}
}

//

func Write(writer *kafka.Writer, value []byte) error {
	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: value,
		//Key:   []byte("user-123"), //все сообщения с ключом "user-123" попадут в одну и ту же партицию,
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

		RequiredAcks: 1,                      //0: at most once; 1: At least once; -1: At least once, но с дополнительными подтверждениями (по умолчанию), самые надежные гарантии, сообщения подтверждаются всеми репликами
		MaxAttempts:  10,                     //кол-во попыток доставки(по умолчанию 10)
		BatchSize:    1,                      //кол-во сообщений которые накапливает kafka прежде чем отправить (по умолчанию 100)
		WriteTimeout: 3 * time.Second,        //время, которое Writer ждет ответа от Kafka после отправки(по умолчанию 10сек)
		BatchTimeout: 100 * time.Millisecond, //время ожидания накопления батча
		Balancer:     &kafka.RoundRobin{},    //балансировщик(решает, в какую партицию отправить сообщение)

	})
	return writer
}

//

func NewReader() *kafka.Reader {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9095"},
		Topic:   "my-topic",
		GroupID: "my-groupID", //позволяет объединять консьюмеров в группу для распределения партиций между ними.

		//Partition: 0,    // Указываем конкретную партицию
		//MinBytes:  10e3, // Минимальный объём данных для чтения(установка не обязательна)
		//MaxBytes:  10e6, // Максимальный объём данных(установка не обязательна)
	})
	return reader
}
func Read(reader *kafka.Reader) ([]byte, error) {
	msg, err := reader.ReadMessage(context.Background())
	if err != nil {
		return nil, err
	}
	fmt.Println(string(msg.Value))
	return msg.Value, nil
}
