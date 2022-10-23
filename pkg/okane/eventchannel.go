package okane

import (
	"context"
	"github.com/gomodule/redigo/redis"
	"github.com/grammaton76/g76golib/sjson"
	"github.com/shopspring/decimal"
	"reflect"
	"time"
)

type Event interface{}

type EvMarketUpdate struct {
	Event
	Market *MarketDef
}

type EvOrderUpdate struct {
	Event
	Market *MarketDef
	Uuid   string
	Max    decimal.Decimal
	Filled decimal.Decimal
	Active bool
}

type EvOrderOpen struct {
	Event
	Market MarketDef
}

type EventChannel struct {
	redis      redis.Conn
	pubsub     redis.PubSubConn
	FpReceiver func(channel string, data []byte) error
	FpOnStart  func() error
}

const (
	redisServerAddr   string        = "localhost:6379"
	healthCheckPeriod time.Duration = time.Minute
)

func NewEventChannel() *EventChannel {
	return &EventChannel{
		FpReceiver: PrintAll,
	}
}

func (a *Account) Publish(reason EventReason, object interface{}) error {
	var err error
	channel := a.User.Username
	Type := reflect.TypeOf(object).String()
	Rec := sjson.NewJsonFromObjectPtr(&object)
	//	log.ErrorIff(Rec.IngestFromObject(object), "error ingesting for publication\n")
	(*Rec)["ObjType"] = Type
	(*Rec)["Reason"] = reason
	if a.User.Events != nil {
		if a.User.Events.redis != nil {
			_, err = a.User.Events.redis.Do("PUBLISH", channel, Rec.Bytes())
			log.ErrorIff(err, "redis publish error")
		} else {
			log.Printf("nil Redis handle for %s; skipping publish.\n", a.Identifier())
		}
	}
	return err
}

func (ec *EventChannel) Connect() error {
	var err error
	ec.redis, err = redis.Dial("tcp", redisServerAddr,
		// Read timeout on server should be greater than ping period.
		redis.DialReadTimeout(healthCheckPeriod+10*time.Second),
		redis.DialWriteTimeout(10*time.Second))
	if err != nil {
		log.Errorf("redis dial")
		return err
	}
	log.FatalIff(err, "redis connection failed")
	return err
}

func PrintAll(channel string, data []byte) error {
	log.Printf("Redis message on channel '%s'\n", channel)
	sjson := sjson.NewJsonFromString(string(data))
	log.Printf("Payload: %s\n", sjson)
	return nil
}

func (ec *EventChannel) Publish(channel string, data ...interface{}) error {
	var caw []interface{}
	caw = append(caw, channel)
	for _, v := range data {
		caw = append(caw, v)
	}
	_, err := ec.redis.Do("PUBLISH", caw...)
	log.ErrorIff(err, "publish error")
	return err
}

func (ec *EventChannel) PublishObj(channel string, object interface{}) error {
	Type := reflect.TypeOf(object).String()
	Rec := sjson.NewJson()
	Rec.IngestFromObject(object)
	Rec["ObjType"] = Type
	_, err := ec.redis.Do("PUBLISH", channel, Rec.Bytes())
	log.Printf("Published a '%s' to '%s'\n", Type, channel)
	log.ErrorIff(err, "publish error")
	return err
}

func (ec *EventChannel) SubscribeChannels(channels ...string) error {
	log.Printf("Subscribing to channels: '%s'\n", channels)
	if err := ec.pubsub.Subscribe(redis.Args{}.AddFlat(channels)...); err != nil {
		return err
	}
	return nil
}

func (ec *EventChannel) StartListener(channels ...string) error {
	var err error
	defer log.Printf("Listen() terminated.\n")
	ctx, _ := context.WithCancel(context.Background())
	ec.pubsub = redis.PubSubConn{
		Conn: ec.redis,
	}

	log.Printf("Subscribed.\n")
	done := make(chan error, 1)

	// Start a goroutine to receive notifications from the server.
	go func() {
		for {
			switch n := ec.pubsub.Receive().(type) {
			case error:
				done <- n
				return
			case redis.Message:
				if err := ec.FpReceiver(n.Channel, n.Data); err != nil {
					done <- err
					return
				}
			case redis.Subscription:
				switch n.Count {
				case len(channels):
					// Notify application when all channels are subscribed.
					if ec.FpOnStart != nil {
						if err := ec.FpOnStart(); err != nil {
							done <- err
							return
						}
					}
				case 0:
					// Return from the goroutine when all channels are unsubscribed.
					done <- nil
					return
				}
			}
		}
	}()

	ticker := time.NewTicker(healthCheckPeriod)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			// Send ping to test health of connection and server. If
			// corresponding pong is not received, then receive on the
			// connection will timeout and the receive goroutine will exit.
			if err = ec.pubsub.Ping(""); err != nil {
				break loop
			}
		case <-ctx.Done():
			break loop
		case err := <-done:
			// Return error from the receive goroutine.
			log.Printf("Listen terminated; error given: %s\n", err)
			return err
		}
	}

	// Signal the receiving goroutine to exit by unsubscribing from all channels.
	ec.pubsub.Unsubscribe()

	// Wait for goroutine to complete.
	return <-done
}

func (ec *EventChannel) WriteJson(Rec *sjson.JSON) error {
	return nil
}
