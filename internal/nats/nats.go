package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nlewo/comin/internal/broker"
	"github.com/nlewo/comin/internal/manager"
	"github.com/nlewo/comin/pkg/protobuf"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

type Nats struct {
	manager   *manager.Manager
	broker    *broker.Broker
	jsEvents  jetstream.JetStream
	jsFetched jetstream.JetStream
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
	switch subject {
	case "fetched":
		_, err := n.jsFetched.Publish(ctx, subject, payload)
		if err != nil {
			return fmt.Errorf("failed to publish to stream %s: %w", stream, err)
		}
	default:
		_, err := n.jsEvents.Publish(ctx, subject, payload)
		if err != nil {
			return fmt.Errorf("failed to publish to stream %s: %w", stream, err)
		}
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

		subject := getEventType(event)
		stream := "events"
		if subject == "fetched" {
			stream = "fetched"
		}
		err := n.pqueue.Add(stream, subject, data)
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

func (n *Nats) ensureStreams(ctx context.Context, jsEvents jetstream.JetStream, jsFetched jetstream.JetStream) {
	// Ensure events stream exists
	_, err := jsEvents.Stream(ctx, "events")
	if err != nil {
		if err != jetstream.ErrStreamNotFound {
			logrus.Errorf("nats: failed to get events stream: %s", err)
			return
		}
		// Stream doesn't exist, create it
		_, err := jsEvents.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
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
			},
		})
		if err != nil {
			logrus.Errorf("nats: failed to create events stream: %s", err)
		}
	}

	// Ensure fetched stream exists
	_, err = jsFetched.Stream(ctx, "fetched")
	if err != nil {
		if err != jetstream.ErrStreamNotFound {
			logrus.Errorf("nats: failed to get fetched stream: %s", err)
			return
		}
		// Stream doesn't exist, create it
		_, err = jsFetched.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
			Name:     "fetched",
			Subjects: []string{"fetched"},
		})
		if err != nil {
			logrus.Errorf("nats: failed to create fetched stream: %s", err)
		}
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
			n.jsEvents, err = jetstream.New(c)
			if err != nil {
				logrus.Errorf("nats: failed to create jetstream: %s", err)
				return
			}
			n.jsFetched, err = jetstream.New(c)
			if err != nil {
				logrus.Errorf("nats: failed to create jetstream: %s", err)
				return
			}
			n.ensureStreams(ctx, n.jsEvents, n.jsFetched)
		}),
		nats.ReconnectHandler(func(c *nats.Conn) {
			logrus.Info("nats: reconnection")
			ctx := context.Background()
			n.jsEvents, err = jetstream.New(c)
			if err != nil {
				logrus.Errorf("nats: failed to create jetstream: %s", err)
				return
			}
			n.jsFetched, err = jetstream.New(c)
			if err != nil {
				logrus.Errorf("nats: failed to create jetstream: %s", err)
				return
			}
			n.ensureStreams(ctx, n.jsEvents, n.jsFetched)
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
