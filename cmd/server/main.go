package main

import (
	"fmt"
	"os"
	"os/signal"
	"log"
	
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("Error establishing channel: %s", err)
	}
	defer ch.Close()
	fmt.Printf("error: %s", err)

	playState := routing.PlayingState{
		IsPaused: true,
	}
	fmt.Println("------ATTEMPTING TO PUBLISH------")
	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, playState)
	if err != nil {
		log.Fatalf("Error publishing JSON: %s", err)
	}
	fmt.Printf("------PUBLISH SUCCESSFUL------ error: %s", err)

	// accept CTRL+C to end program
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Program shutting down.")
}
