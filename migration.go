package main

import (
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/tsdb/engine/tsm1"
	"github.com/kisielk/whisper-go/whisper"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func usage() {
	//TODO: Add other arguments as dbname, timestamp
	log.Fatal(`migration.go -infile=somefile.wsp -info`)
}

type MigrationData struct {
	whisperFile    string
	fromTimestamp  uint32
	untilTimestamp uint32
	dbName         string
	tsmFiles       []string
	targetFile     string
	tags           []string
	measurement    string
}

var filename *string
var info *bool

func main() {
	filename = flag.String("infile", "NULL", "InfluxDB TSM filename")
	info = flag.Bool("info", false, "Just information no migration")

	flag.Parse()
	if *filename == "NULL" {
		usage()
	}

	migrationData := MigrationData{}
	migrationData.whisperFile = *filename

	//TODO: dbname and timestamp values should be input from user
	migrationData.dbName = "mydb2"
	migrationData.fromTimestamp = 1449729960  //10dec start
	migrationData.untilTimestamp = 1449731880 //10 dec end

	if *info {
		migrationData.WhisperInfo(*filename)
		return
	}

	//TODO: create and validate tags

	migrationData.FindTSMFiles()
	migrationData.FindTheFileToWrite()
	migrationData.ReadWSPWriteTSMRecords()

}
func IdentifyTags(filename string) string {
	//TODO: Identify tags
	return "tag1=value1"
}

func (migrationData *MigrationData) WhisperInfo(filename string) {
	w, err := whisper.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	//FetchTime iterates through all points, need to check if this is of any use
	//for large files, the function may take long time just to show TimeRange
	interval, points, err := w.FetchUntil(migrationData.fromTimestamp,
		migrationData.untilTimestamp)

	fmt.Printf("Values in interval %v No. of points %v\n", interval, len(points))
	t1 := time.Unix(int64(interval.FromTimestamp), 0)
	fmt.Println(t1)
	t2 := time.Unix(int64(interval.UntilTimestamp), 0)
	fmt.Println(t2)
	return
}

func (migrationData *MigrationData) ReadWSPWriteTSMRecords() {
	// Open whisper file for reading
	w, err := whisper.Open(migrationData.whisperFile)
	if err != nil {
		log.Fatal(err)
	}
	//TODO: explore w.readPoints, fetchUntil reads all data at once
	interval, wspPoints, err := w.FetchUntil(migrationData.fromTimestamp,
		migrationData.untilTimestamp)

	fmt.Printf("Values in interval %v No. of points %v\n", interval, len(wspPoints))

	if len(wspPoints) == 0 {
		fmt.Println("No data to migrate")
		return
	}

	// Open tsm file for writing
	f, err := os.OpenFile(migrationData.targetFile, os.O_CREATE|os.O_RDWR, 0666)
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
	tsmPt.key = "cpu_usage,tag2=value2#!~#user"

	//Read whisper points and create tsm points
	for i, wspPoint := range wspPoints {
		fmt.Println(i, wspPoint.Value, wspPoint.Timestamp)
		tsmPt.values = append(tsmPt.values,
			tsm1.NewValue(time.Unix(int64(wspPoint.Timestamp), 0), wspPoint.Value))
	}
	//Write the points in batch

	//TODO: Writing the old values( tested with values having 10-DEC-2015 ts) is
	// not getting written to TSM files, However, values with time.Now() are getting
	// written
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

func (migrationData *MigrationData) FindTSMFiles() {
	searchDir := "/Users/uttam/.influxdb/data/" + migrationData.dbName

	fileList := []string{}
	err := filepath.Walk(searchDir, func(path string, f os.FileInfo, err error) error {
		if strings.HasSuffix(f.Name(), "tsm") {
			fileList = append(fileList, path)
		}
		return nil
	})
	if err != nil {
		fmt.Println("Err")
	}
	for _, file := range fileList {
		fmt.Println(file)
	}
	migrationData.tsmFiles = fileList
}

// finds the file which has overlapping timerange as input data
// TODO: data may be written across multiple TSM files
func (migrationData *MigrationData) FindTheFileToWrite() {
	var file string
	for _, file = range migrationData.tsmFiles {
		f, err := os.Open(file)
		if err != nil {
			fmt.Println("unexpected error open file: %v", err)
		}
		defer f.Close()

		tsmReader, err := tsm1.NewTSMReaderWithOptions(
			tsm1.TSMReaderOptions{
				MMAPFile: f,
			})
		if err != nil {
			fmt.Println("unexpected error created reader: %v", err)
		}
		defer tsmReader.Close()

		//fmt.Println("Keys", tsmReader.Keys())
		t1, t2 := tsmReader.TimeRange()
		fmt.Println("TimeRange", t1, t2)
		if t1.Unix() < int64(migrationData.fromTimestamp) &&
			t2.Unix() > int64(migrationData.untilTimestamp) {
			fmt.Println("File to write", file)
		}
	}
	migrationData.targetFile = file
}
