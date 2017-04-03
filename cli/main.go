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
	e = log.New(os.Stderr, "ERROR: ", log.LstdFlags)
	w = log.New(os.Stderr, "WARNING: ", log.LstdFlags)
	i = log.New(os.Stdout, "INFO: ", log.LstdFlags)

	redisAddr string
	redisHost string
	redisPort int
	redisPass string
	redisDb   int

	fromStdin bool
	config    string
	file      string
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

func expandWithLog(key string) string {
	val := os.Getenv(key)

	if val == "" {
		w.Printf("%s env var is not set\n", key)
	}

	return val
}

func processConfig(r io.Reader) {
	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line := os.Expand(scanner.Text(), expandWithLog)

		if logid, err := processLine(strings.TrimSpace(line)); err == nil {
			i.Println(logid)
		} else {
			e.Fatalf("%s: %s", logid, err)
		}
	}
}

func processLine(line string) (string, error) {
	if line == "" {
		return "<emptyline>", nil
	}

	tokens := strings.Split(line, " ")

	switch {
	case len(tokens) == 0:
		return "<emptyline>", nil
	case len(tokens) < 4:
		return "", errors.New("invalid format")
	}

	logid := strings.Join(tokens[0:3], " ")
	ks := tokens[0]
	username := tokens[1]
	key := tokens[2]

	switch ks {
	case "user":
		if key != "password" {
			return logid, errors.New("currently only supports password field")
		}

		var password = tokens[3]
		return logid, ACL.SetUser(username, password)

	case "acl":
		var topic = key
		var accessLevel, err = getAccessLevel(tokens[3])

		if err != nil {
			return logid, err
		}
		return logid, ACL.SetRule(username, topic, accessLevel)

	case "subs":
		var topic = key
		var qos, err = strconv.Atoi(tokens[3])

		if err != nil {
			return logid, err
		}
		return logid, ACL.SetSubscription(username, topic, qos)
	}

	return logid, nil
}

func main() {
	redisAddr = os.Getenv("REDIS_ADDR")
	redisHost = os.Getenv("REDIS_HOST")
	redisPort, _ = strconv.Atoi(os.Getenv("REDIS_PORT"))
	redisPass = os.Getenv("REDIS_PASS")
	redisDb, _ = strconv.Atoi(os.Getenv("REDIS_DB"))

	if redisHost == "" {
		redisHost = "localhost"
	}
	if redisPort == 0 {
		redisPort = 6379
	}

	flag.StringVar(&redisAddr, "addr", redisAddr, "Redis address")
	flag.StringVar(&redisHost, "host", redisHost, "Redis Host")
	flag.IntVar(&redisPort, "port", redisPort, "Redis Port")
	flag.StringVar(&redisPass, "pass", redisPass, "Redis Password")
	flag.IntVar(&redisDb, "db", redisDb, "Redis DB")
	flag.BoolVar(&fromStdin, "stdin", false, "Read from stdin")
	flag.StringVar(&config, "config", "", "Config definition")
	flag.StringVar(&file, "file", "", "Config definition")
	flag.Parse()

	if redisAddr == "" {
		redisAddr = fmt.Sprintf("%s:%d", redisHost, redisPort)
	}
	i.Printf("redis://:***@%s/%d", redisAddr, redisDb)
	ACL.SetRedisOptions(redisAddr, redisPass, redisDb)

	if fromStdin {
		processConfig(os.Stdin)
	} else if file != "" {
		f, _ := os.Open(file)
		defer f.Close()
		processConfig(f)
	} else if config != "" {
		processConfig(strings.NewReader(config))
	} else {
		flag.PrintDefaults()
	}
}
