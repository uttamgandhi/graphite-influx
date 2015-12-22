package main

import (
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
	"log"
	"os"
	"time"
)

func usage() {
	log.Fatal(`write_tsm.go -file=somefile.tsm -key=measurementName+tag+field -ts=unixtime -val=value`)
}

var filename, keyname *string
var timestamp *uint
var value *float64

func main() {
	filename = flag.String("file", "NULL", "InfluxDB TSM filename")
	keyname = flag.String("key", "NULL", "Key name")

	now := uint(time.Now().Unix())
	timestamp = flag.Uint("ts", now, "Unix epoch time, default:Now")
	value = flag.Float64("val", 0.0, "Value to be stored")
	flag.Parse()

	/*var data = []struct {
	    key    string
	    values []tsm1.Value
	  }{
	    {"cpu_usage,tag2=value2#!~#idle", []tsm1.Value{
	      tsm1.NewValue(time.Now().UTC(), 11.9)},
	    },
	  }*/

	if *filename == "NULL" || *keyname == "NULL" || *value == 0.0 {
		fmt.Println(*filename, *keyname, *value)
		usage()
	}

	file, err := os.OpenFile(*filename, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		fmt.Println("File Open Error")
		return
	}
	defer file.Close()

	tsmWriter, err := tsm1.NewTSMWriter(file)
	if err != nil {
		panic(fmt.Sprintf("create TSM writer: %v", err))
	}

	type TSMData struct {
		key    string
		values []tsm1.Value
	}

	var data TSMData
	data.key = *keyname
	data.values = append(data.values, tsm1.NewValue(time.Unix(int64(*timestamp), 0), *value))

	//for _, d := range data {
	if err := tsmWriter.Write(data.key, data.values); err != nil {
		panic(fmt.Sprintf("write TSM value: %v", err))
	}
	//}

	if err := tsmWriter.WriteIndex(); err != nil {
		panic(fmt.Sprintf("write TSM index: %v", err))
	}

	if err := tsmWriter.Close(); err != nil {
		panic(fmt.Sprintf("write TSM close: %v", err))
	}
}
