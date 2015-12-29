package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/client/v2"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
	"github.com/kisielk/whisper-go/whisper"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func usage() {
	log.Fatal(`migration.go -wspPath=whisper folder -influxDataDir=influx data folder
		-info -month=<1-12> -year=<1970> -dbname=migrated`)
}

type ShardInfo struct {
	id    json.Number
	from  time.Time
	until time.Time
}

type MigrationData struct {
	wspPath       string
	influxDataDir string
	from          time.Time
	until         time.Time
	dbName        string
	wspFiles      []string
	tags          []string
	measurement   string
	shards        []ShardInfo
}

var wspPath *string
var influxDataDir *string
var info *bool
var month *int
var year *int
var dbName *string

func main() {
	wspPath = flag.String("wspPath", "NULL", "Whisper files folder path")
	influxDataDir = flag.String("influxDataDir", "NULL", "InfluxDB data directory")
	info = flag.Bool("info", false, "Just information no migration")
	month = flag.Int("month", 0, "Month of the year in numeric form")
	year = flag.Int("year", 0, "Year in YYYY format, e.g 1970")
	dbName = flag.String("dbname", "migrated", "Database name (default: migrated")

	flag.Parse()
	if *wspPath == "NULL" || *influxDataDir == "NULL" || *month == 0 || *year == 0 {
		usage()
	}

	migrationData := MigrationData{}
	migrationData.wspPath = *wspPath
	migrationData.influxDataDir = *influxDataDir

	migrationData.dbName = *dbName
	fromday := fmt.Sprintf("%d-%02d-%02d", *year, *month, 1)
	migrationData.from, _ = time.Parse("2006-01-02", fromday)
	untilday := fmt.Sprintf("%d-%02d-%02d", *year, *month, 31)
	migrationData.until, _ = time.Parse("2006-01-02", untilday)

	migrationData.CreateShards(*month, *year)
	migrationData.MapWhisperTSM(*wspPath, *info)

	//TODO: create and validate tags

}
func IdentifyTags(filename string) string {
	//TODO: Identify tags
	return "tag1=value1"
}

func (migrationData *MigrationData) CreateShards(month, year int) {
	c, _ := client.NewHTTPClient(client.HTTPConfig{
		Addr: "http://localhost:8086",
	})
	defer c.Close()

	createDBString := fmt.Sprintf("Create Database %v", migrationData.dbName)
	createDBQuery := client.NewQuery(createDBString, "", "")
	_, err := c.Query(createDBQuery)
	if err != nil {
		fmt.Println(err)
		return
	}

	// Create a new point batch
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  migrationData.dbName,
		Precision: "s",
	})

	// Create a point and add to batch
	tags := map[string]string{"tag1": "value1"}
	fields := map[string]interface{}{
		"value": 10.1,
	}
	//Create and parse
	for i := 1; i <= 28; i++ {
		day := fmt.Sprintf("%d-%02d-%02d", year, month, i)
		t, _ := time.Parse("2006-01-02", day)
		pt, _ := client.NewPoint("dummy", tags, fields, t)
		bp.AddPoint(pt)
	}
	// Write the batch
	c.Write(bp)

	query := client.NewQuery("Show Shard Groups", "", "")
	response, err := c.Query(query)
	if err != nil {
		fmt.Println(err)
		return
	}
	var index int = 0
	var colname string
	for index, colname = range response.Results[0].Series[0].Columns {
		if colname == "database" {
			break
		}
	}
	for _, values := range response.Results[0].Series[0].Values {
		if values[index] == migrationData.dbName {
			shard := &ShardInfo{}
			shard.id = values[0].(json.Number)
			shard.from, _ = time.Parse(time.RFC3339, values[3].(string))
			shard.until, _ = time.Parse(time.RFC3339, values[4].(string))
			migrationData.shards = append(migrationData.shards, *shard)
		}
	}

	//Once shards are created, this measurement is not required
	dropMeasurementQuery := client.NewQuery("Drop Measurement dummy", "", "")
	_, err = c.Query(dropMeasurementQuery)
	if err != nil {
		fmt.Println(err)
		return
	}
	return
}

func (migrationData *MigrationData) GetWSPPoints(
	from time.Time, until time.Time) []whisper.Point {

	migrationData.FindWhisperFiles()

	var allwspPoints []whisper.Point
	for _, wspFile := range migrationData.wspFiles {
		fmt.Println("Reading .. ", wspFile)
		w, err := whisper.Open(wspFile)
		if err != nil {
			log.Fatal(err)
		}
		interval, wspPoints, err := w.FetchUntilTime(from, until)
		if err != nil {
			log.Fatal(err)
		}
		t1 := time.Unix(int64(interval.FromTimestamp), 0)
		t2 := time.Unix(int64(interval.UntilTimestamp), 0)

		fmt.Printf("Values in interval %v-%v No. of points %v \n", t1, t2,
			len(wspPoints))
		allwspPoints = append(allwspPoints, wspPoints...)
	}
	return allwspPoints
}
func (migrationData *MigrationData) MapWhisperTSM(folderName string, info bool) {
	//FetchTime iterates through all points, need to check if this is of any use
	//for large files, the function may take long time just to show TimeRange
	var from, until time.Time
	for _, shard := range migrationData.shards {
		from = shard.from
		if shard.from.Before(migrationData.from) {
			from = migrationData.from
		}
		until = shard.until
		if shard.until.After(migrationData.until) {
			until = migrationData.until
		}
		wspPoints := migrationData.GetWSPPoints(from, until)
		/*if !info {
			var toWrite string
			fmt.Println("The data will be written to TSM files, Do you want to continue? Y/N")
			fmt.Scanf("%s", &toWrite)
			if toWrite != "Y" {
				return
			}
		}*/
		//Write the TSM data
		filename := migrationData.GetTSMFileName(shard)
		migrationData.WriteTSMPoints(filename, wspPoints)
	}
	return
}

func (migrationData *MigrationData) GetTSMFileName(shard ShardInfo) string {
	retentionPolicy := "/default/" //TODO:...
	shardName := shard.id.String() + "/"
	filename := "000000001-000000002.tsm" // TODO:..
	filePath := migrationData.influxDataDir + migrationData.dbName +
		retentionPolicy + shardName + filename
	//fmt.Println("filename", filePath)
	return filePath
}

func (migrationData *MigrationData) WriteTSMPoints(filename string,
	wspPoints []whisper.Point) {

	if len(wspPoints) == 0 {
		fmt.Println("No data to migrate")
		return
	}

	// Open tsm file for writing
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0666)
	fmt.Println("OpenTSM", filename)
	if err != nil {
		fmt.Println("File Open Error")
		return
	}
	defer f.Close()

	//Create TSMWriter with filehandle
	tsmW, err := tsm1.NewTSMWriter(f)
	if err != nil {
		panic(fmt.Sprintf("create TSM writer: %v", err))
	}

	type tsmPoint struct {
		key    string
		values []tsm1.Value
	}
	var tsmPt tsmPoint
	// Create key using measurement name and tags
	//tsmPoint.key = migrationData.measurement + tags + #!~# + fieldname
	tsmPt.key = "cpu_usage,tag2=value2#!~#value" //TODO:...

	//Read whisper points and create tsm points
	for _, wspPoint := range wspPoints {
		//fmt.Println(i, wspPoint.Value, wspPoint.Timestamp)
		tsmPt.values = append(tsmPt.values,
			tsm1.NewValue(time.Unix(int64(wspPoint.Timestamp), 0), wspPoint.Value))
	}
	//Write the points in batch
	if err := tsmW.Write(tsmPt.key, tsmPt.values); err != nil {
		panic(fmt.Sprintf("write TSM value: %v", err))
	}

	//Write index
	if err := tsmW.WriteIndex(); err != nil {
		panic(fmt.Sprintf("write TSM index: %v", err))
	}

	if err := tsmW.Close(); err != nil {
		panic(fmt.Sprintf("write TSM close: %v", err))
	}
}

func (migrationData *MigrationData) FindWhisperFiles() {
	searchDir := migrationData.wspPath

	fileList := []string{}
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if strings.HasSuffix(f.Name(), "wsp") {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		fmt.Println("Err")
	}
	//for _, file := range fileList {
		//fmt.Println(file)
	//}
	migrationData.wspFiles = fileList
}
