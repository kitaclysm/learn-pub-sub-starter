package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.Acktype {
	return func(move gamelogic.ArmyMove) pubsub.Acktype {
		defer fmt.Print("> ")
		moveOutcome := gs.HandleMove(move)
		switch moveOutcome {
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		case gamelogic.MoveOutcomeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix+"."+move.Player.Username,
				gamelogic.RecognitionOfWar{
					Attacker: move.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				log.Printf("error publishing: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		fmt.Println("error: unknown move outcome")
		return pubsub.NackDiscard
	}
}

func publishWar(ch *amqp.Channel, gl routing.GameLog) error {

}

func handlerWar(gs *gamelogic.GameState) func(gamelogic.RecognitionOfWar) pubsub.Acktype {
	return func(rw gamelogic.RecognitionOfWar) pubsub.Acktype {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		// rw.Attacker, rw.Defender
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			err := gamelogic.WriteLog(routing.GameLog{
				CurrentTime:	time.Now(),
				Message:		winner+" won a war against "+loser,
				Username:		rw.Attacker.Username,
			})
			if err != nil {
				log.Printf("error logging: %v", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			err := gamelogic.WriteLog(routing.GameLog{
				CurrentTime:	time.Now(),
				Message:		winner+" won a war against "+loser,
				Username:		rw.Attacker.Username,
			})
			if err != nil {
				log.Printf("error logging: %v", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			err := gamelogic.WriteLog(routing.GameLog{
				CurrentTime:	time.Now(),
				Message:		"A war between "+winner+" and "+loser+" resulted in a draw",
				Username:		rw.Attacker.Username,
			})
			if err != nil {
				log.Printf("error logging: %v", err)
				return pubsub.NackDiscard
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.Acktype {
	return func(ps routing.PlayingState) pubsub.Acktype {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}
