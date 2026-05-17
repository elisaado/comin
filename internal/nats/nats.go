package nats

import (
	"context"
	"fmt"
	"os"
	"sync"
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
	manager      *manager.Manager
	broker       *broker.Broker
	js           jetstream.JetStream
	streamErr    error
	fileMu       sync.Mutex
	pendingCount int
}

func New(m *manager.Manager, b *broker.Broker) *Nats {
	return &Nats{
		manager: m,
		broker:  b,
	}
}

func (n *Nats) listen() (err error) {
	subscriber := n.broker.Subscribe()
	defer n.broker.Unsubscribe(subscriber)

	for event := range subscriber {
		if n.js == nil || n.streamErr != nil {
			logrus.Warn("nats: jetstream not initialized, writing event to disk")
			err := n.writeEventToDisk(event)
			if err != nil {
				logrus.Errorf("nats: failed to write event to disk: %s", err)
			}
			continue
		}

		err := n.publishEvent(event)
		if err != nil {
			logrus.Errorf("nats: %s", err)
			// Write to file on disk
			err := n.writeEventToDisk(event)
			if err != nil {
				logrus.Errorf("nats: failed to write event to disk: %s", err)
			}
		}
	}

	return nil
}

func (n *Nats) publishEvent(event *protobuf.Event) error {
	if n.js == nil || n.streamErr != nil {
		return fmt.Errorf("jetstream not initialized")
	}

	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	subject := getEventType(event)
	_, err = n.js.Publish(context.Background(), subject, data)
	if err != nil {
		return fmt.Errorf("failed to publish event: %w", err)
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

func (n *Nats) writeEventToDisk(event *protobuf.Event) error {
	data, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	filename := "/tmp/comin-events.protobuf"

	n.fileMu.Lock()
	defer n.fileMu.Unlock()

	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	if _, err := f.Write(data); err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	n.pendingCount++
	logrus.Infof("nats: appended event to disk: %s (pending: %d)", filename, n.pendingCount)
	return nil
}

func (n *Nats) purgeEventsFile() error {
	filename := "/tmp/comin-events.protobuf"

	// TODO: when the file is huge, this could lock the mutex for
	// too long. We should find a way to reduce the lock time.
	n.fileMu.Lock()
	defer n.fileMu.Unlock()

	data, err := os.ReadFile(filename)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("failed to read file: %w", err)
	}

	if len(data) == 0 {
		return nil
	}

	var remainingEvents []*protobuf.Event
	count := 0
	buf := data
	for len(buf) > 0 {
		var event protobuf.Event
		err := proto.Unmarshal(buf, &event)
		if err != nil {
			logrus.Errorf("nats: failed to unmarshal event from file: %s", err)
			break
		}

		eventData, marshalErr := proto.Marshal(&event)
		if marshalErr != nil {
			logrus.Errorf("nats: failed to marshal event: %s", marshalErr)
			// Keep the event in the file since we couldn't process it
			remainingEvents = append(remainingEvents, &event)
			buf = buf[len(eventData):]
			event.Reset()
			continue
		}

		err = n.publishEvent(&event)
		if err != nil {
			logrus.Errorf("nats: %s", err)
			// Keep the event in the file since we couldn't publish it
			remainingEvents = append(remainingEvents, &event)
		} else {
			count++
		}

		buf = buf[len(eventData):]
		event.Reset()
	}

	// Write back any events that failed to publish
	if len(remainingEvents) > 0 {
		var remainingData []byte
		for _, e := range remainingEvents {
			data, marshalErr := proto.Marshal(e)
			if marshalErr != nil {
				logrus.Errorf("nats: failed to marshal remaining event: %s", marshalErr)
				continue
			}
			remainingData = append(remainingData, data...)
		}
		err = os.WriteFile(filename, remainingData, 0644)
		if err != nil {
			return fmt.Errorf("failed to write remaining events to file: %w", err)
		}
		n.pendingCount = len(remainingEvents)
		logrus.Infof("nats: purged %d events from file, %d remaining: %s", count, len(remainingEvents), filename)
	} else {
		// Empty the file
		err = os.WriteFile(filename, []byte{}, 0644)
		if err != nil {
			return fmt.Errorf("failed to empty file: %w", err)
		}
		n.pendingCount = 0
		logrus.Infof("nats: purged %d events from file: %s", count, filename)
	}

	return nil
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
			err = n.purgeEventsFile()
			if err != nil {
				logrus.Errorf("nats: failed to purge events file: %s", err)
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
			err := n.purgeEventsFile()
			if err != nil {
				logrus.Errorf("nats: failed to purge events file on reconnect: %s", err)
			}
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
