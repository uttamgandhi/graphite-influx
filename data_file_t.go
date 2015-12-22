package main

import (
  "github.com/influxdb/influxdb/tsdb/engine/tsm1"
  "fmt"
  "time"
  "os"
)
func main() {
//data_file test
  f, err := os.OpenFile("/tmp/tsmtest", os.O_CREATE|os.O_RDWR, 0666)
  tsw, err := tsm1.NewTSMWriter(f)
  if err != nil {
    fmt.Println("unexpected error creating writer: %v", err)
  }

  var data = []struct {
    key    string
    values []tsm1.Value
  }{
    {"cpp_usage,tag1=value1#:!", []tsm1.Value{
      tsm1.NewValue(time.Unix(1, 0), 190.08)},
    },
    {"int", []tsm1.Value{
      tsm1.NewValue(time.Unix(1, 0), int64(1))},
    },
    {"bool", []tsm1.Value{
      tsm1.NewValue(time.Unix(1, 0), true)},
    },
    {"string", []tsm1.Value{
      tsm1.NewValue(time.Unix(1, 0), "foo")},
    },
  }

  for _, d := range data {
    if err := tsw.Write(d.key, d.values); err != nil {
      fmt.Println("unexpected error writing: %v", err)
    }
  }

  if err := tsw.WriteIndex(); err != nil {
    fmt.Println("unexpected error writing index: %v", err)
  }

  if err := tsw.Close(); err != nil {
    fmt.Println("unexpected error closing: %v", err)
  }

  f, err = os.Open(f.Name())
  if err != nil {
    fmt.Println("unexpected error open file: %v", err)
  }

  r, err := tsm1.NewTSMReaderWithOptions(
    tsm1.TSMReaderOptions{
      MMAPFile: f,
    })
  if err != nil {
    fmt.Println("unexpected error created reader: %v", err)
  }
  defer r.Close()

  var count int
  for _, d := range data {
    readValues, err := r.ReadAll(d.key)
    if err != nil {
      fmt.Println("unexpected error readin: %v", err)
    }

    if exp := len(d.values); exp != len(readValues) {
      fmt.Println("read values length mismatch: got %v, exp %v", len(readValues), exp)
    }

    for i, v := range d.values {
      if v.Value() != readValues[i].Value() {
        fmt.Println("read value mismatch(%d): got %v, exp %d", i, readValues[i].Value(), v.Value())
      }
    }
    count++
  }

  if got, exp := count, len(data); got != exp {
    fmt.Println("read values count mismatch: got %v, exp %v", got, exp)
  }
}
