package main

import (
	"fmt"
	"os"
	"os/signal"
	"log"
	
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	cUrl := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(cUrl)
	if err != nil {
		log.Fatalf("Error connecting to server: %s", err)
	}
	defer connection.Close()
	fmt.Println("Connection successful.")
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Program shutting down.")
}
