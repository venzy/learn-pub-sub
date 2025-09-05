package main

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/venzy/learn-pub-sub/internal/gamelogic"
	"github.com/venzy/learn-pub-sub/internal/pubsub"
	"github.com/venzy/learn-pub-sub/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, warCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(
				warCh,
				routing.ExchangePerilTopic,
				routing.WarRecognitionsPrefix + "." + gs.GetPlayerSnap().Username,
				gamelogic.RecognitionOfWar{
					Attacker: am.Player,
					Defender: gs.GetPlayerSnap(),
				},
			)
			if err != nil {
				fmt.Printf("Failed to publish war recognition: %s\n", err)
				return pubsub.NackRequeue
			}
			fmt.Printf("War recognition published successfully\n")
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			// Nothing further to process
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, logCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeYouWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := logMessage(msg, rw.Attacker.Username, logCh)
			if err != nil {
				fmt.Printf("Failed to log war outcome: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeOpponentWon:
			msg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := logMessage(msg, rw.Attacker.Username, logCh)
			if err != nil {
				fmt.Printf("Failed to log war outcome: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			msg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := logMessage(msg, rw.Attacker.Username, logCh)
			if err != nil {
				fmt.Printf("Failed to log war outcome: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func logMessage(msg string, warInitiator string, logCh *amqp.Channel) error {
	gamelog := routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    warInitiator,
	}

	return pubsub.PublishGob(
		logCh,
		routing.ExchangePerilTopic,
		routing.GameLogSlug + "." + warInitiator,
		gamelog,
	)
}