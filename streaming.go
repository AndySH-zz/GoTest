package main

import (
	"encoding/json"
	"github.com/nats-io/go-nats-streaming"
	"log"
)

//Message func for subscribing
type Message func([]byte)

//Client structure implementation
type Client struct {
	connection    stan.Conn
	subscriptions map[string]stan.Subscription
}

//NewStreamingClient instance entity
func NewStreamingClient(sc stan.Conn) *Client {
	return &Client{sc, make(map[string]stan.Subscription)}
}

//Subscribe subscribe to event
func (cl *Client) Subscribe(msg Message, event string) error {
	hdl := func(m *stan.Msg) {
		msg(m.Data)
	}

	startOpt := stan.StartAt(0)
	sub, err := cl.connection.QueueSubscribe(event, "", hdl, startOpt)
	if err != nil {
		log.Println("Error! Can't subscribe to event", event, ":", err)
		return err
	}

	cl.subscriptions[event] = sub
	return nil
}

//Publish publishes event
func (cl *Client) Publish(data interface{}, event string) error {
	res, err := json.Marshal(data)
	if err != nil {
		log.Println("Can't marshal object", data)
		return err
	}

	err = cl.connection.Publish(event, res)
	if err != nil {
		log.Println("Error during publish: ", err)
		return err
	}

	return nil
}

// Close closes connection and unsubscribe
func (cl *Client) Close() {
	if cl.subscriptions != nil {
		for _, s := range cl.subscriptions {
			s.Unsubscribe()
		}
	}

	cl.connection.Close()
}
