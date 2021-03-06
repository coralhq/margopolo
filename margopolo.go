package margopolo

import (
	"crypto/sha256"
	"encoding/hex"

	"gopkg.in/redis.v5"
)

const (
	// SubAccess an access level that user can only subscribe to certain topic
	SubAccess = 1

	// PubAccess an access level that user can only publish to certain topic
	PubAccess = 2

	// PubSubAccess an access level that user can publish and subscribe to certain topic
	PubSubAccess = 3

	// QosFireAndForget at most once
	QosFireAndForget = 0

	// QosAtLeastOnce might create duplicate deliveries
	QosAtLeastOnce = 1

	// QosExactlyOnce exactly once deliveries guaranteed
	QosExactlyOnce = 2
)

var client *redis.Client

// PasswordHash hash the password using sha256
func PasswordHash(password string) string {
	h := sha256.New()
	h.Write([]byte(password))
	return hex.EncodeToString(h.Sum(nil))
}

// SetRedisURL sets URL to be used by Redis connection
// sample: redis://username:password@host:port/0
func SetRedisURL(url string) error {
	var err error

	if opts, err := redis.ParseURL(url); err != nil {
		client = redis.NewClient(opts)
	}

	return err
}

// SetRedisOptions sets options to be used by Redis connection
func SetRedisOptions(addr string, password string, db int) {
	client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	})
}

// SetUser sets mqtt user and password
func SetUser(username string, password string) error {
	return client.HSet("mqtt_user:"+username, "password", PasswordHash(password)).Err()
}

// SetRule sets ACL rule
// topic: e.g. chats/+/messages/#
// accessLevel: 1=Sub 2=Pub 3=PubSub
func SetRule(username string, topic string, accessLevel int) error {
	return client.HSet("mqtt_acl:"+username, topic, accessLevel).Err()
}

// SetSubscription sets static subscription for user
// topic: e.g. chats/+/messages/#
// accessLevel: 0=AtMostOnce 1=AtLeastOnce 2=ExactlyOnce
func SetSubscription(username string, topic string, qos int) error {
	return client.HSet("mqtt_sub:"+username, topic, qos).Err()
}
