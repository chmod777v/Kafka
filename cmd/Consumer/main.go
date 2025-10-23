package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	my_kafka "kafka/pkg/kafka"
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
	reader := my_kafka.NewReader()
	defer reader.Close()

	for {
		my_kafka.Read(reader)
	}

	value, err := HachFunc(2, []byte("hellow"))
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(value)
}
