package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/json"
	"fmt"
	my_kafka "kafka/pkg/kafka"
)

var Base = make(map[string][]byte)

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
	reader := my_kafka.NewReader()
	defer reader.Close()
	fmt.Println("server start")

	var value my_kafka.Hash
	for {
		req, err := my_kafka.Read(reader)
		if err != nil {
			fmt.Println("Ошибка при получении:", err)
			continue
		}

		json.Unmarshal(req, &value)

		val, err := value.ValueToBytes()
		if err != nil {
			fmt.Println("Ошибка ValueToBytes:", err.Error())
			continue
		}

		hash, err := HachFunc(value.HachFunc, val)
		if err != nil {
			fmt.Println(err.Error())
			continue
		}
		fmt.Println(hash)
		Base[value.Name] = hash
	}

}
