package main

import (
	"bufio"
	"flag"
	"fmt"
	//	"github.com/streadway/amqp"
	"github.com/titanous/go-riak"
	"log"
	"os"
	"strings"

//	"time"
)

var (
	input = flag.String("i", "", "Input file")
)

func init() {
	flag.Parse()

}

func main() {
	// Set up Riak connection
	client := riak.New("127.0.0.1:8087")
	err := client.Connect()
	if err != nil {
		fmt.Println("Cannot connect, is Riak running?")
		return
	}
	// Close Riak connection upon finishing
	defer func() {
		client.Close()
	}()
	// Use this bucket
	bucket, _ := client.Bucket("tstriak2")

	dir, err := os.Open(*input)
	if err != nil {
		panic(err)
	}

	info, err := dir.Readdir(-1)
	if err != nil {
		panic(err)
	}
	//fmt.Println(info)
	for _, info := range info {
		if info.Mode().IsRegular() {
			if strings.HasSuffix(info.Name(), ".rules") {
				// open input file
				path := strings.Join([]string{*input, info.Name()}, "")
				fi, err := os.Open(path)
				if err != nil {
					panic(err)
				}
				// close fi on exit and check for its returned error
				defer func() {
					if err := fi.Close(); err != nil {
						panic(err)
					}
				}()

				fmt.Println("Scanning:", path)
				// Scan through the file used for
				scanner := bufio.NewScanner(fi)
				for scanner.Scan() {
					if strings.HasPrefix(scanner.Text(), "alert") == true {
						part := strings.Split(scanner.Text(), "sid:")
						sid := strings.Replace(strings.Split(part[1], ";")[0], " ", "", -1)
						data := strings.Join([]string{"{'rule':'", scanner.Text(), "'}"}, "")
						// fmt.Println(data)
						fmt.Println(sid)
						dbobj, err := bucket.Get(sid)
						string(obj.Data)
						//fmt.Println(scanner.Text())
						obj := bucket.New(sid)
						obj.ContentType = "application/json"
						obj.Data = []byte(data)
						obj.Store()
					}
					if err := scanner.Err(); err != nil {
						log.Fatal(err)
					}
				}
			}
		}
	}
}
