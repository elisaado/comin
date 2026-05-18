package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nlewo/comin/internal/broker"
	"github.com/nlewo/comin/internal/manager"
	"github.com/nlewo/comin/internal/protobuf"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type Nats struct {
	manager   *manager.Manager
	broker    *broker.Broker
	js        jetstream.JetStream
	streamErr error
	pqueue    *PersistentQueue
}

func New(m *manager.Manager, b *broker.Broker) *Nats {
	n := &Nats{
		manager: m,
		broker:  b,
	}

	// Initialize persistent queue with a worker that publishes to NATS
	var initErr error
	n.pqueue, initErr = NewPersistentQueue("/tmp/comin-nats-pqueue.db", n.pqueueWorker)
	if initErr != nil {
		logrus.Errorf("nats: failed to initialize persistent queue: %s", initErr)
	}

	return n
}

func (n *Nats) pqueueWorker(ctx context.Context, stream, subject string, payload []byte) error {
	if n.js == nil || n.streamErr != nil {
		return fmt.Errorf("jetstream not initialized")
	}

	_, err := n.js.Publish(ctx, subject, payload)
	if err != nil {
		return fmt.Errorf("failed to publish to nats: %w", err)
	}

	return nil
}

func (n *Nats) listen() (err error) {
	subscriber := n.broker.Subscribe()
	defer n.broker.Unsubscribe(subscriber)

	for event := range subscriber {
		data, marshalErr := proto.Marshal(event)
		if marshalErr != nil {
			logrus.Errorf("nats: failed to marshal event: %s", marshalErr)
			continue
		}

		err := n.pqueue.Add("events", getEventType(event), data)
		if err != nil {
			logrus.Errorf("nats: failed to add event to persistent queue: %s", err)
		}
	}

	return nil
}

func getEventType(event *protobuf.Event) string {
	switch event.GetType().(type) {
	case *protobuf.Event_EvalStartedType:
		return "eval.started"
	case *protobuf.Event_EvalFinishedType:
		return "eval.finished"
	case *protobuf.Event_BuildStartedType:
		return "build.started"
	case *protobuf.Event_BuildFinishedType:
		return "build.finished"
	case *protobuf.Event_ConfirmationSubmittedType:
		return "confirmation.submitted"
	case *protobuf.Event_ConfirmationCancelledType:
		return "confirmation.cancelled"
	case *protobuf.Event_ConfirmationConfirmedType:
		return "confirmation.confirmed"
	case *protobuf.Event_Resume_:
		return "resume"
	case *protobuf.Event_Suspend_:
		return "suspend"
	case *protobuf.Event_DeploymentStartedType:
		return "deployment.started"
	case *protobuf.Event_DeploymentFinishedType:
		return "deployment.finished"
	case *protobuf.Event_RebootRequired_:
		return "reboot.required"
	case *protobuf.Event_ManagerState_:
		return "manager.state"
	case *protobuf.Event_Fetched_:
		return "fetched"
	default:
		return "events"
	}
}

func (n *Nats) Start() (err error) {
	logrus.Info("nats: starting the client and listening to the event stream")
	go n.listen()

	nc, err := nats.Connect("nats://92.243.27.85:4222",
		nats.Token("xg17i0WGrTHa0milW.qrA"),
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
		nats.ConnectHandler(func(c *nats.Conn) {
			logrus.Info("nats: initial connection")
			ctx := context.Background()
			js, jsErr := jetstream.New(c)
			if jsErr != nil {
				logrus.Errorf("nats: failed to create jetstream: %s", jsErr)
				n.streamErr = jsErr
				return
			}
			n.js = js
			stream, err := js.Stream(ctx, "events")
			if err != nil {
				if err != jetstream.ErrStreamNotFound {
					logrus.Errorf("nats: failed to get stream: %s", err)
					n.streamErr = err
					return
				}
				// Stream doesn't exist, create it
				_, n.streamErr = js.CreateStream(ctx, jetstream.StreamConfig{
					Name: "events",
					Subjects: []string{
						"eval.started",
						"eval.finished",
						"build.started",
						"build.finished",
						"confirmation.submitted",
						"confirmation.cancelled",
						"confirmation.confirmed",
						"resume",
						"suspend",
						"deployment.started",
						"deployment.finished",
						"reboot.required",
						"manager.state",
						"fetched",
					},
				})
				if n.streamErr != nil {
					logrus.Errorf("nats: failed to create stream: %s", n.streamErr)
				}
			} else {
				n.streamErr = nil
				_ = stream
			}
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logrus.Info("nats: reconnection")
			js, jsErr := jetstream.New(c)
			if jsErr != nil {
				logrus.Errorf("nats: failed to recreate jetstream on reconnect: %s", jsErr)
				n.streamErr = jsErr
				return
			}
			n.js = js
		}),
		nats.ReconnectErrHandler(func(_ *nats.Conn, reconnectErr error) {
			fmt.Printf("nats: reconnection failed: %s\n", reconnectErr)
		}))
	if err != nil {
		return err
	}

	if nc.IsConnected() {
		logrus.Infof("nats: is connected")
	} else {
		logrus.Infof("nats: is not connected (but it will retry every minute)")
	}

	return nil
}
