package main

import (
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
	"log"
	"os"
)

func usage() {
	log.Fatal(`read_tsm.go -file=somefile.tsm -key=measurementname+tag+field
      e.g. read_tsm.go -file=file.tsm -key=cpu_usage,tag2=value2#!~#idle`)
}

var filename, keyname *string

func main() {

	filename = flag.String("file", "NULL", "InfluxDB TSM filename")
	keyname = flag.String("key", "NULL", "Key name")

	flag.Parse()

	if *filename == "NULL" {
		usage()
	}

	f, err := os.Open(*filename)
	if err != nil {
		fmt.Println("unexpected error open file: %v", err)
	}

	tsmReader, err := tsm1.NewTSMReaderWithOptions(
		tsm1.TSMReaderOptions{
			MMAPFile: f,
		})
	if err != nil {
		fmt.Println("unexpected error created reader: %v", err)
	}
	defer tsmReader.Close()

	if *keyname == "NULL" {
		fmt.Println("Keys", tsmReader.Keys())
		t1, t2 := tsmReader.TimeRange()
		fmt.Println("TimeRange", t1, t2)
		return
	}

	//fmt.Println(tsmReader.Stats())

	var count int
	readValues, err := tsmReader.ReadAll(*keyname)
	if err != nil {
		fmt.Println("unexpected error readin: %v", err)
	}

	for i, _ := range readValues {
		fmt.Println("read value", i, readValues[i].Value())
	}
	count++
}
