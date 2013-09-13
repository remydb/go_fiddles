package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"github.com/titanous/go-riak"
	"log"
	"os"
	"strings"
	//"time"
)

var (
	amqpURI      = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queueName    = flag.String("queue", "sagan-queue", "Ephemeral AMQP queue name")
	routingKey   = flag.String("key", "sagan-key", "AMQP routing key")
	reliable     = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
	input        = flag.String("i", "", "Input file")
)

func init() {
	flag.Parse()
}

func main() {
	//	fmt.Println(*reliable)
	//	return
	// Set up RabbitMQ connection
	log.Printf("dialing %q", *amqpURI)
	connection, err := amqp.Dial(*amqpURI)
	if err != nil {
		panic(err)
	}
	defer connection.Close()

	log.Printf("got Connection, getting Channel")
	channel, err := connection.Channel()
	if err != nil {
		panic(err)
	}

	log.Printf("got Channel, declaring %q Exchange (%q)", *exchangeType, *exchangeName)
	if err := channel.ExchangeDeclare(
		*exchangeName, // name
		*exchangeType, // type
		true,          // durable
		false,         // auto-deleted
		false,         // internal
		false,         // noWait
		nil,           // arguments
	); err != nil {
		panic(err)
	}

	log.Printf("declared Exchange, declaring Queue %q", *queueName)
	queue, err := channel.QueueDeclare(
		*queueName, // name of the queue
		true,       // durable
		false,      // delete when usused
		false,      // exclusive
		false,      // noWait
		nil,        // arguments
	)
	if err != nil {
		panic(err)
	}

	log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)",
		queue.Name, queue.Messages, queue.Consumers, *routingKey)

	if err = channel.QueueBind(
		queue.Name,    // name of the queue
		*routingKey,   // bindingKey
		*exchangeName, // sourceExchange
		false,         // noWait
		nil,           // arguments
	); err != nil {
		panic(err)
	}

	ack, nack := channel.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

	// Set up Riak connection
	client := riak.New("127.0.0.1:8087")
	errr := client.Connect()
	if errr != nil {
		fmt.Println("Cannot connect, is Riak running?")
		return
	}

	// Close Riak connection upon finishing
	defer func() {
		client.Close()
	}()

	// Use this bucket
	bucket, _ := client.Bucket("tstriak2")

	// Open directory to read rule files from
	dir, err := os.Open(*input)
	if err != nil {
		panic(err)
	}

	// List all files in directory
	info, err := dir.Readdir(-1)
	if err != nil {
		panic(err)
	}

	// Loop through all the rule files
	for _, info := range info {
		if info.Mode().IsRegular() {
			if strings.HasSuffix(info.Name(), ".rules") {
				// Open input file
				path := strings.Join([]string{*input, info.Name()}, "")
				fi, err := os.Open(path)
				if err != nil {
					panic(err)
				}

				// close input file on exit and check for its returned error
				defer func() {
					if err := fi.Close(); err != nil {
						panic(err)
					}
				}()

				// Scan each rule file for lines containing rules
				fmt.Println("Scanning:", path)
				scanner := bufio.NewScanner(fi)
				for scanner.Scan() {
					if err := scanner.Err(); err != nil {
						log.Fatal(err)
					}
					if strings.HasPrefix(scanner.Text(), "alert") == true {
						// Extract rule ID from full rule and write in JSON format
						part := strings.Split(scanner.Text(), "sid:")
						sid := strings.Replace(strings.Split(part[1], ";")[0], " ", "", -1)
						data := strings.Join([]string{"{'rule':'", scanner.Text(), "'}"}, "")

						// Try to grab corresponding key from Riak DB if exist
						dbobj, err := bucket.Get(sid)
						if err != nil {
							panic(err)
						}
						if dbobj != nil {
							if string(dbobj.Data) == data {
								fmt.Printf("Rule with key %s is identical, not updating\n", dbobj.Key)
							} else {
								fmt.Printf("Rule with key %s has new version, sending message to queue\n", dbobj.Key)

								if *reliable {
									if err := channel.Confirm(false); err != nil {
										panic(err)
										fmt.Println(err)
										return
									}
								}

								if err := channel.Publish(
									*exchangeName, // publish to an exchange
									*routingKey,   // routing to 0 or more queues
									false,         // mandatory
									false,         // immediate
									amqp.Publishing{
										Headers:         amqp.Table{},
										ContentType:     "application/json",
										ContentEncoding: "",
										Body:            []byte(data),
										DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
										Priority:        0,              // 0-9
										// a bunch of application/implementation-specific fields
									},
								); err != nil {
									panic(err)
									return
								}

								go confirmOne(ack, nack)
							}
						} else {
							// If the key doesn't exist, add it
							obj := bucket.New(sid)
							obj.ContentType = "application/json"
							obj.Data = []byte(data)
							obj.Store()
							fmt.Printf("New rule with key %s added\n", obj.Key)
						}
					}
				}
			}
		}
	}
}
func confirmOne(ack, nack chan uint64) {
	select {
	case tag := <-ack:
		log.Printf("confirmed delivery with delivery tag: %d", tag)
	case tag := <-nack:
		log.Printf("failed delivery of delivery tag: %d", tag)
	}
}
