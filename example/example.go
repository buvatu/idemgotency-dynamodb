package main

import (
	idempotency "github.com/buvatu/idemgotency-dynamodb"
	"github.com/google/uuid"
	"log"
)

func main() {
	err := idempotency.Init("config.yaml")
	if err != nil {
		log.Fatal(err)
		return
	}
	idempotencyKey := uuid.MustParse("ec33361b-fb67-45c2-af28-7c8860c9fda1")
	result1, err := idempotency.ExecuteExactlyOne("test-operation-1", idempotencyKey, func() (string, error) {
		log.Println("test-operation-1")
		return "test-result-1", nil
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Println(result1)
	result2, err := idempotency.ExecuteExactlyOne("test-operation-2", idempotencyKey, func() (int, error) {
		log.Println("test-operation-2")
		return 2, nil
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Println(result2)
	result3, err := idempotency.ExecuteExactlyOne("test-operation-3", idempotencyKey, func() (TestStruct, error) {
		log.Println("test-operation-3")
		return TestStruct{
			Name:  "Tung",
			Email: "buvatu@gmail.com",
			Age:   36,
		}, nil
	})
	if err != nil {
		log.Fatal(err)
		return
	}
	log.Println(result3)
}

type TestStruct struct {
	Name  string
	Email string
	Age   int
}
