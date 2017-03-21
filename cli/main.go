package main

import (
	"bufio"
	"errors"
	"flag"
	"log"
	"os"
	"strconv"
	"strings"

	"fmt"
	"io"

	ACL "github.com/coralhq/margopolo"
)

var (
	redisAddr string
	redisHost string
	redisPort int
	redisPass string
	redisDb   int

	fromStdin bool
	config    string
)

func getAccessLevel(str string) (int, error) {
	switch str {
	case "sub":
		return ACL.SubAccess, nil
	case "pub":
		return ACL.PubAccess, nil
	case "pubsub":
		return ACL.PubSubAccess, nil
	default:
		return -1, errors.New("invalid acl rule: " + str)
	}
}

func processConfig(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := scanner.Text()
		if err := processLine(strings.TrimSpace(line)); err != nil {
			log.Fatalf("%s: %s", line, err)
		}
	}
}

func processLine(line string) error {
	if line == "" {
		return nil
	}

	tokens := strings.Split(line, " ")

	switch {
	case len(tokens) == 0:
		return nil
	case len(tokens) < 4:
		return errors.New("invalid format")
	}

	ks := tokens[0]
	username := tokens[1]
	key := tokens[2]

	switch ks {
	case "user":
		if key != "password" {
			return errors.New("invalid format")
		}

		var password = tokens[3]
		return ACL.SetUser(username, password)

	case "acl":
		var topic = key
		var accessLevel, err = getAccessLevel(tokens[3])

		if err != nil {
			return err
		}
		return ACL.SetRule(username, topic, accessLevel)

	case "subs":
		var topic = key
		var qos, err = strconv.Atoi(tokens[3])

		if err != nil {
			return err
		}
		return ACL.SetSubscription(username, topic, qos)
	}

	return nil
}

func main() {
	flag.StringVar(&redisAddr, "addr", "", "Redis address")
	flag.StringVar(&redisHost, "host", "localhost", "Redis Host")
	flag.IntVar(&redisPort, "port", 6379, "Redis Port")
	flag.StringVar(&redisPass, "pass", "", "Redis Password")
	flag.IntVar(&redisDb, "db", 0, "Redis DB")
	flag.BoolVar(&fromStdin, "stdin", false, "Read from stdin")
	flag.StringVar(&config, "config", "", "Config definition")
	flag.Parse()

	if redisAddr == "" {
		redisAddr = fmt.Sprintf("%s:%d", redisHost, redisPort)
	}

	log.Printf("redis://:***@%s/%d", redisAddr, redisDb)
	ACL.SetRedisOptions(redisAddr, redisPass, redisDb)

	if fromStdin {
		processConfig(os.Stdin)
	} else if config != "" {
		processConfig(strings.NewReader(config))
	} else {
		flag.PrintDefaults()
	}
}
