/* The file is used to create dummy data of a month in influxdb database
The database has to be created in influxdb before running this utility
*/
package main

import (
	"fmt"
	"github.com/influxdb/influxdb/client/v2"
	"time"
)

func main() {
	const MyDB string = "mydb2"

	c, _ := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	defer c.Close()

	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  MyDB,
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"tag1": "value1"}
	fields := map[string]interface{}{
		"idle":   10.1,
		"system": 53.3,
		"user":   46.6,
	}
	//Create and parse
	const shortForm = "2006-Jan-02"
	for i := 1; i < 31; i++ {

		day := fmt.Sprintf("2015-Nov-%02d", i)
		t, _ := time.Parse(shortForm, day)
		fmt.Println(t)

		pt, _ := client.NewPoint("edepu3_usage", tags, fields, t)
		bp.AddPoint(pt)
	}
	// Write the batch
	c.Write(bp)

	return
}
