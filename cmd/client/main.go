package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
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

	// prompt for username
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Error acquiring username: %s", err)
	}

	// create new game state
	gamestate := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
		handlerPause(gamestate),
	)
	if err != nil {
		log.Fatalf("Error subscribing: %s", err)
	}

	for {
		command := gamelogic.GetInput()
		if len(command) == 0 {
			continue
		}
		switch command[0] {
		case "spawn":
			err = gamestate.CommandSpawn(command)
			if err != nil {
				fmt.Print(err)
			}
		case "move":
			_, err := gamestate.CommandMove(command)
			if err != nil {
				fmt.Print(err)
			}
		case "status":
			gamestate.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			return
		default:
			fmt.Print("unknown command")
		}
	}
}
