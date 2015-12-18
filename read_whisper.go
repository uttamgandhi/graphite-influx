package main

import (
	"flag"
	"fmt"
	"github.com/kisielk/whisper-go/whisper"
	"log"
	"time"
)

func usage() {
	log.Fatal(`read_whisper.go -file=somefile.wsp -from=intervalStart -until=intervalEnd`)
}

var from, until *uint
var filename *string

func main() {
	now := uint(time.Now().Unix())
	yesterday := uint(time.Now().Add(-240 * time.Hour).Unix())

	from = flag.Uint("from", yesterday, "Unix epoch time of the beginning of the requested interval. (default: 24 hours ago)")
	until = flag.Uint("until", now, "Unix epoch time of the end of the requested interval. (default: now)")
	filename = flag.String("file", "NULL", "Graphite whisper filename")
	flag.Parse()

	if *filename == "NULL" {
		usage()
	}

	fromTime := uint32(*from)
	untilTime := uint32(*until)

	w, err := whisper.Open(*filename)
	if err != nil {
		log.Fatal(err)
	}

	interval, points, err := w.FetchUntil(fromTime, untilTime)

	fmt.Printf("Values in interval %+v\n", interval)
	for i, p := range points {
		fmt.Printf("%d %v\n", i, p)
	}

	return
}
