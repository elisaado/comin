package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nlewo/comin/internal/broker"
	"github.com/nlewo/comin/internal/manager"
	"github.com/sirupsen/logrus"
)

type Nats struct {
	manager *manager.Manager
	broker  *broker.Broker
	events  jetstream.Stream
}

func New(m *manager.Manager, b *broker.Broker) *Nats {
	return &Nats{
		manager: m,
		broker:  b,
	}
}

func (n *Nats) listen() (err error) {
	subscriber := n.broker.Subscribe()
	state := n.manager.GetState()

}

func (n *Nats) Start() (err error) {
	logrus.Info("nats: starting the client")

	nc, err := nats.Connect("nats://92.243.27.85:4222",
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ConnectHandler(func(c *nats.Conn) {
			logrus.Info("nats: initial connection")
			ctx := context.Background()
			js, err := jetstream.New(c)
			if err != nil {
				logrus.Errorf("nats: %s", err)
			}
			n.events, err = js.CreateStream(ctx, jetstream.StreamConfig{
				Name: "events",
			})
			if err != nil {
				logrus.Errorf("nats: %s", err)
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logrus.Info("nats: reconnection")
		}),
		nats.ReconnectErrHandler(func(_ *nats.Conn, err error) {
			fmt.Printf("nats: reconnection failed: %s\n", err)
		}))
	if err != nil {
		return err
	}

	if nc.IsConnected() {
		logrus.Infof("nats: is connected")
	} else {
		logrus.Infof("nats: is not connected (but it will retry every minute)")
	}

	return
}
