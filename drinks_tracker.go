package main

import (
	"bytes"
	"fmt"
	"github.com/hoisie/web"
	"github.com/titanous/go-riak"
	"io"
	"log"
)

func getlist(ctx *web.Context) {
	rclient := riak.New("127.0.0.1:8087")
	err := rclient.Connect()
	if err != nil {
		log.Fatal("Cannot connect, is Riak running?")
		return
	}
	bucket, err := rclient.Bucket("users")
	if err != nil {
		log.Fatal("Cannot connect to bucket: ", err)
	}
	list, err := bucket.ListKeys()
	if err != nil {
		log.Fatal("Failed to list keys: %s", err)
	}
	//Create buffer to write html to
	var buf bytes.Buffer
	//numdrinks := 1
	//Fill buffer with html
	buf.WriteString("<table><tr><th>Employee</th><th>Drinks</th><th>Owed</th></tr>")
	for _, user := range list {
		count, err := rclient.CounterGet("users", string(user))
		if err != nil {
			log.Fatal("Failed to retrieve counter: ", err)
		}

		buf.WriteString(fmt.Sprintf("<tr><td>%s</td><td>%d</td><td>%.2f</td></tr>\n", user, count, float64(count)*float64(1.4)))
	}
	buf.WriteString("</table>")
	//copy buf directly into the HTTP response
	io.Copy(ctx, &buf)
}

func addDrink(ctx *web.Context, user string) {
	rclient := riak.New("127.0.0.1:8087")
	err := rclient.Connect()
	if err != nil {
		log.Fatal("Cannot connect, is Riak running?")
		return
	}
	errr := rclient.CounterUpdate("users", user, 1)
	if errr != nil {
		log.Fatal("Failed to increment counter: ", err)
	}
}

func delDrink(ctx *web.Context, user string) {
	rclient := riak.New("127.0.0.1:8087")
	err := rclient.Connect()
	if err != nil {
		log.Fatal("Cannot connect, is Riak running?")
		return
	}
	errr := rclient.CounterUpdate("users", user, -1)
	if errr != nil {
		log.Fatal("Failed to increment counter: ", err)
	}
}

func main() {
	web.Get("/getuserinfo", getlist)
	web.Get("/add/(.*)", addDrink)
	web.Get("/del/(.*)", delDrink)
	web.Run("127.0.0.1:9999")

}
