package main

import (
	"fmt"
	"log"
	
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	gamelogic.PrintServerHelp()

	for {
		command := gamelogic.GetInput()
		if len(command) == 0 {
			continue
		}
		switch command[0] {
		case "pause":
			fmt.Print("sending pause message")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			)
			if err != nil {
				log.Fatalf("Error pausing: %s", err)
			}
		case "resume":
			fmt.Print("sending resume message")
			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			)
			if err != nil {
				log.Fatalf("Error resuming: %s", err)
			}
		case "quit":
			fmt.Print("exiting game...")
			return
		default:
			fmt.Print("unknown command")
		}
	}
}
