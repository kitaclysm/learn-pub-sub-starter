package main

import (
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
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

	// declare and bind
	_, _, err = pubsub.DeclareAndBind(
		connection,
		routing.ExchangePerilDirect,
		fmt.Sprintf("%s.%s", routing.PauseKey, username),
		routing.PauseKey,
		pubsub.SimpleQueueTransient,
	)
	if err != nil {
		log.Fatalf("Error with declare and bind: %s", err)
	}

	// create new game state
	gamestate := gamelogic.NewGameState(username)

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
