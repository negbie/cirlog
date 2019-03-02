package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/negbie/cirlog/parsers/htjson"

	"github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"

	"github.com/negbie/cirlog/tail"
)

var tailOptions = tail.TailOptions{
	ReadFrom: "start",
	Stop:     true,
}

// defaultOptions is a fully populated GlobalOptions with good defaults to start from
var defaultOptions = GlobalOptions{
	// each test will have to populate Host with the location of its test server
	Host:          "",
	SampleRate:       1,
	NumSenders:       1,
	BatchFrequencyMs: 1000, // Longer batch sends to accommodate for slower CI machines
	Reqs: RequiredOptions{
		// using the json parser for everything because we're not testing parsers here.
		ParserName: "json",
		WriteKey:   "abcabc123123",
		// each test will specify its own logfile
		// LogFiles:   []string{tmpdir + ""},
		Dataset: "pika",
	},
	Tail:           tailOptions,
	StatusInterval: 1,
}

// test testing framework
func TestHTTPtest(t *testing.T) {
	ts := &testSetup{}
	ts.start(t, &GlobalOptions{})
	defer ts.close()
	ts.rsp.responseBody = "whatup pikachu"
	res, err := http.Get(ts.server.URL)
	if err != nil {
		log.Fatal(err)
	}
	greeting, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	assert.Equal(t, res.StatusCode, 200)
	assert.Equal(t, string(greeting), "whatup pikachu")
	assert.Equal(t, ts.rsp.reqCounter, 1)
}

func TestMultipleFiles(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	logFile1 := ts.tmpdir + "/first.log"
	fh1, err := os.Create(logFile1)
	if err != nil {
		t.Fatal(err)
	}
	logFile2 := ts.tmpdir + "/second.log"
	fh2, err := os.Create(logFile2)
	if err != nil {
		t.Fatal(err)
	}
	defer fh1.Close()
	fmt.Fprintf(fh1, `{"key1":"val1"}`)
	defer fh2.Close()
	fmt.Fprintf(fh2, `{"key2":"val2"}`)
	opts.Reqs.LogFiles = []string{logFile1, logFile2}
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 1)
	assert.Equal(t, ts.rsp.evtCounter, 2)
	assert.Contains(t, ts.rsp.reqBody, `{"key1":"val1"}`)
	assert.Contains(t, ts.rsp.reqBody, `{"key2":"val2"}`)
	requestURL := ts.rsp.req.URL.Path
	assert.Equal(t, requestURL, "/1/batch/pika")
}

func TestAugmentField(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	logFileName := ts.tmpdir + "/augment.log"
	logfh, _ := os.Create(logFileName)
	defer logfh.Close()
	damapFileName := ts.tmpdir + "/damap.json"
	damapfh, _ := os.Create(damapFileName)
	defer damapfh.Close()
	fmt.Fprintf(logfh, `{"format":"json"}
{"format":"freetext"}
{"format":"csv","delimiter":"comma"}`)
	fmt.Fprintf(damapfh, `{"format":{
		"json":{"structured":true},
		"freetext":{"structured":false,"annoyance":5}
	},
	"color":{
		"red":{"nomatch":"wontappear"}
	}
}`)
	opts.Reqs.LogFiles = []string{logFileName}
	opts.DAMapFile = damapFileName
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 1, "failed count")
	// json should be identified as structured
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json","structured":true}`, "faild content")
	// free text gets two additional fields
	assert.Contains(t, ts.rsp.reqBody, `{"annoyance":5,"format":"freetext","structured":false}`, "faild content")
	// csv doesn't exist in the damap, so no change
	assert.Contains(t, ts.rsp.reqBody, `{"delimiter":"comma","format":"csv"}`, "faild content")
}

func TestDropField(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	logFileName := ts.tmpdir + "/drop.log"
	fh, _ := os.Create(logFileName)
	defer fh.Close()
	fmt.Fprintf(fh, `{"dropme":"chew","format":"json","reallygone":"notyet"}`)
	opts.Reqs.LogFiles = []string{logFileName}
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 1)
	assert.Contains(t, ts.rsp.reqBody, `{"dropme":"chew","format":"json","reallygone":"notyet"}`)
	opts.DropFields = []string{"dropme"}
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 2)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json","reallygone":"notyet"}`)
	opts.DropFields = []string{"dropme", "reallygone"}
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 3)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json"}`)
}

func TestScrubField(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	logFileName := ts.tmpdir + "/scrub.log"
	fh, _ := os.Create(logFileName)
	defer fh.Close()
	fmt.Fprintf(fh, `{"format":"json","name":"hidden"}`)
	opts.Reqs.LogFiles = []string{logFileName}
	opts.ScrubFields = []string{"name"}
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 1)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json","name":"e564b4081d7a9ea4b00dada53bdae70c99b87b6fce869f0c3dd4d2bfa1e53e1c"}`)
}

func TestAddField(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	logFileName := ts.tmpdir + "/add.log"
	logfh, _ := os.Create(logFileName)
	defer logfh.Close()
	fmt.Fprintf(logfh, `{"format":"json"}`)
	opts.Reqs.LogFiles = []string{logFileName}
	opts.AddFields = []string{`newfield=newval`}
	run(context.Background(), opts)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json","newfield":"newval"}`)
	opts.AddFields = []string{"newfield=newval", "second=new"}
	run(context.Background(), opts)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json","newfield":"newval","second":"new"}`)
}

func TestLinePrefix(t *testing.T) {
	opts := defaultOptions
	// linePrefix of "Nov 13 10:19:31 app23 process.port[pid]: "
	// let's capture timestamp and hostname, skip process.port and pid
	opts.PrefixRegex = `(?P<server_timestamp>... .. ..:..:..) (?P<hostname>[a-zA-Z0-9]+) [^:]*: `
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	logFileName := ts.tmpdir + "/linePrefix.log"
	logfh, _ := os.Create(logFileName)
	defer logfh.Close()
	fmt.Fprintf(logfh, `Nov 13 10:19:31 app23 process.port[pid]: {"format":"json"}`)
	opts.Reqs.LogFiles = []string{logFileName}
	run(context.Background(), opts)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json","hostname":"app23","server_timestamp":"Nov 13 10:19:31"}`)
}

func TestSampleRate(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	rand.Seed(1)
	sampleLogFile := ts.tmpdir + "/sample.log"
	logfh, _ := os.Create(sampleLogFile)
	defer logfh.Close()
	for i := 0; i < 50; i++ {
		fmt.Fprintf(logfh, `{"format":"json%d"}`+"\n", i)
	}
	opts.Reqs.LogFiles = []string{sampleLogFile}
	opts.TailSample = false

	run(context.Background(), opts)
	// with no sampling, 50 lines -> 50 events
	assert.Equal(t, ts.rsp.evtCounter, 50)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json49"}`)
	ts.rsp.reset()

	opts.SampleRate = 3
	opts.TailSample = true
	run(context.Background(), opts)
	// setting a sample rate of 3 gets 17 requests.
	// tail does the sampling
	assert.Equal(t, ts.rsp.evtCounter, 17)
	assert.Contains(t, ts.rsp.reqBody, `{"format":"json49"},"samplerate":3,`)
}

func TestReadFromOffset(t *testing.T) {
	opts := defaultOptions
	ts := &testSetup{}
	ts.start(t, &opts)
	defer ts.close()
	offsetLogFile := ts.tmpdir + "/offset.log"
	offsetStateFile := ts.tmpdir + "/offset.leash.state"
	logfh, _ := os.Create(offsetLogFile)
	defer logfh.Close()
	logStat := unix.Stat_t{}
	unix.Stat(offsetLogFile, &logStat)
	for i := 0; i < 10; i++ {
		fmt.Fprintf(logfh, `{"format":"json%d"}`+"\n", i)
	}
	opts.Reqs.LogFiles = []string{offsetLogFile}
	opts.Tail.ReadFrom = "last"
	opts.Tail.StateFile = offsetStateFile
	osf, _ := os.Create(offsetStateFile)
	defer osf.Close()
	fmt.Fprintf(osf, `{"INode":%d,"Offset":38}`, logStat.Ino)
	run(context.Background(), opts)
	assert.Equal(t, ts.rsp.reqCounter, 1)
	assert.Equal(t, ts.rsp.evtCounter, 8)
}

// TestLogRotation tests that cirlog continues tailing after log rotation,
// with different possible configurations:
// * when cirlog polls or uses inotify
// * when logs are rotated using rename/reopen, or using copy/truncate.
func TestLogRotation(t *testing.T) {
	for _, poll := range []bool{true, false} {
		for _, copyTruncate := range []bool{true, false} {
			t.Run(fmt.Sprintf("polling: %t; copyTruncate: %t", poll, copyTruncate), func(t *testing.T) {
				wg := &sync.WaitGroup{}
				opts := defaultOptions
				opts.BatchFrequencyMs = 1
				opts.Tail.Stop = false
				opts.Tail.Poll = poll
				ts := &testSetup{}
				ts.start(t, &opts)
				defer ts.close()
				logFileName := ts.tmpdir + "/test.log"
				fh, err := os.Create(logFileName)
				if err != nil {
					t.Fatal(err)
				}
				opts.Reqs.LogFiles = []string{logFileName}

				// Run cirlog in the background
				ctx, cancel := context.WithCancel(context.Background())
				wg.Add(1)
				go func() {
					run(ctx, opts)
					wg.Done()
				}()

				// Write a line to the log file, and check that cirlog reads it.
				fmt.Fprint(fh, "{\"key\":1}\n")
				fh.Close()
				sent := expectWithTimeout(func() bool { return ts.rsp.evtCounter == 1 }, time.Second)
				assert.True(t, sent, "Failed to read first log line")

				// Simulate log rotation
				if copyTruncate {
					err = exec.Command("cp", logFileName, ts.tmpdir+"/test.log.1").Run()
				} else {
					err = os.Rename(logFileName, ts.tmpdir+"/test.log.1")
				}
				assert.NoError(t, err)
				// Older versions of the inotify implementation in
				// github.com/hpcloud/tail would fail to reopen a log file
				// after a rename/reopen (https://github.com/hpcloud/tail/pull/115),
				// but this delay is necessary to provoke the issue. Don't know why.
				time.Sleep(100 * time.Millisecond)

				// Write lines to the new log file, and check that cirlog reads them.
				fh, err = os.Create(logFileName)
				assert.NoError(t, err)
				fmt.Fprint(fh, "{\"key\":2}\n")
				fmt.Fprint(fh, "{\"key\":3}\n")
				fh.Close()
				// TODO: when logs are rotated using copy/truncate, we lose the
				// first line of the new log file.
				sent = expectWithTimeout(func() bool { return ts.rsp.evtCounter >= 2 }, time.Second)
				assert.True(t, sent, "Failed to read log lines after rotation")

				// Stop cirlog.
				cancel()
				wg.Wait()
			})
		}
	}
}

// boilerplate to spin up a httptest server, create tmpdir, etc.
// to create an environment in which to run these tests
type testSetup struct {
	server *httptest.Server
	rsp    *responder
	tmpdir string
}

func (t *testSetup) start(tst *testing.T, opts *GlobalOptions) {
	logrus.SetOutput(ioutil.Discard)
	t.rsp = &responder{}
	t.server = httptest.NewServer(http.HandlerFunc(t.rsp.serveResponse))
	tmpdir, err := ioutil.TempDir(os.TempDir(), "test")
	if err != nil {
		tst.Fatal(err)
	}
	t.tmpdir = tmpdir
	opts.Host = t.server.URL
	t.rsp.responseCode = 200
}
func (t *testSetup) close() {
	t.server.Close()
	os.RemoveAll(t.tmpdir)
}

type responder struct {
	req          *http.Request // the most recent request answered by the server
	reqBody      string        // the body sent along with the request
	reqCounter   int           // the number of requests answered since last reset
	evtCounter   int           // the number of events (<= reqCounter, will be < if events are batched)
	responseCode int           // the http status code with which to respond
	responseBody string        // the body to send as the response
}

func (r *responder) serveResponse(w http.ResponseWriter, req *http.Request) {
	r.req = req
	r.reqCounter += 1

	var reader io.ReadCloser
	switch req.Header.Get("Content-Encoding") {
	case "gzip":
		buf := bytes.Buffer{}
		if _, err := io.Copy(&buf, req.Body); err != nil {
			panic(err)
		}
		gzReader, err := gzip.NewReader(&buf)
		if err != nil {
			panic(err)
		}
		req.Body.Close()
		reader = gzReader
	default:
		reader = req.Body
	}
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		panic(err)
	}

	payload := []map[string]interface{}{}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			r.evtCounter++ // likely not a batch request
		} else {
			r.evtCounter += len(payload)
		}
	}
	r.reqBody = string(body)
	w.WriteHeader(r.responseCode)
	fmt.Fprintf(w, r.responseBody)
}
func (r *responder) reset() {
	r.reqCounter = 0
	r.evtCounter = 0
	r.responseCode = 200
}

func expectWithTimeout(condition func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for deadline.After(time.Now()) {
		if condition() {
			return true
		}
	}
	return false

}

func TestGetEndLine(t *testing.T) {
	fileContents := `
{"key1": "value1"}
{"key1": "value2"}
{"key1": "value3"}
{"key1": "END"}`

	f, err := ioutil.TempFile(os.TempDir(), "cirlog-test")
	assert.Nil(t, err, "failed to open temp file")
	_, err = f.WriteString(fileContents)
	assert.Nil(t, err, "failed to write temp file")
	f.Close()
	defer syscall.Unlink(f.Name())

	line := getEndLine(f.Name())
	assert.Equal(t, `{"key1": "END"}`, line)
}

func TestRebaseTime(t *testing.T) {
	baseTime, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Wed Jul 3 15:04:05 -0700 PDT 2018")
	assert.Nil(t, err)
	nowTime, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Wed Jul 4 12:00:00 -0700 PDT 2018")
	assert.Nil(t, err)
	timestamp, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Wed Jul 3 12:00:05 -0700 PDT 2018")
	assert.Nil(t, err)
	// should be three hours, four minutes behind our nowTime
	expected, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Wed Jul 4 08:56:00 -0700 PDT 2018")
	assert.Nil(t, err)
	rebasedTime := rebaseTime(baseTime, nowTime, timestamp)
	assert.Equal(t, expected, rebasedTime)
}

func TestGetBaseTime(t *testing.T) {
	fileContents := `
{"key1": "value1", "timestamp": "Wed Jul 3 12:00:05 -0700 PDT 2018"}
{"key1": "value2", "timestamp": "Wed Jul 3 13:00:05 -0700 PDT 2018"}
{"key1": "value3", "timestamp": "Wed Jul 3 14:00:05 -0700 PDT 2018"}
{"key1": "value4", "timestamp": "Wed Jul 3 15:04:05 -0700 PDT 2018"}
`

	f, err := ioutil.TempFile(os.TempDir(), "cirlog-test")
	assert.Nil(t, err, "failed to open temp file")
	_, err = f.WriteString(fileContents)
	assert.Nil(t, err, "failed to write temp file")
	f.Close()
	defer syscall.Unlink(f.Name())

	options := GlobalOptions{
		Reqs: RequiredOptions{
			LogFiles:   []string{f.Name()},
			ParserName: "json",
		},
		JSON: htjson.Options{
			TimeFieldFormat: "Mon Jan 2 15:04:05 -0700 MST 2006",
			TimeFieldName:   "timestamp",
		},
	}

	expected, err := time.Parse("Mon Jan 2 15:04:05 -0700 MST 2006", "Wed Jul 3 15:04:05 -0700 PDT 2018")
	assert.Nil(t, err)
	baseTime, err := getBaseTime(options)
	assert.Nil(t, err)
	assert.Equal(t, expected.UTC(), baseTime.UTC())
}
