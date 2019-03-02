package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"

	"github.com/negbie/cirlog/event"
	"github.com/negbie/cirlog/parsers"
	"github.com/negbie/cirlog/parsers/csv"
	"github.com/negbie/cirlog/parsers/htjson"
	"github.com/negbie/cirlog/parsers/keyval"
	"github.com/negbie/cirlog/parsers/regex"
	"github.com/negbie/cirlog/parsers/syslog"
	"github.com/negbie/cirlog/tail"
)

// actually go and be leashy
func run(ctx context.Context, options GlobalOptions) {
	logrus.Info("Starting cirlog")

	sigs := make(chan os.Signal, 1)
	ctx, cancel := context.WithCancel(ctx)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// compile the prefix regex once for use on all channels
	var prefixRegex *parsers.ExtRegexp
	if options.PrefixRegex == "" {
		prefixRegex = nil
	} else {
		prefixRegex = &parsers.ExtRegexp{regexp.MustCompile(options.PrefixRegex)}
	}

	// get our lines channel from which to read log lines
	var linesChans []chan string
	var err error
	tc := tail.Config{
		Paths:       options.Reqs.LogFiles,
		FilterPaths: options.FilterFiles,
		Type:        tail.RotateStyleSyslog,
		Options:     options.Tail,
	}
	if options.TailSample {
		linesChans, err = tail.GetSampledEntries(ctx, tc, options.SampleRate)
	} else {
		linesChans, err = tail.GetEntries(ctx, tc)
	}
	if err != nil {
		logrus.WithFields(logrus.Fields{"err": err}).Fatal(
			"Error occurred while trying to tail logfile")
	}

	// set up our signal handler and support canceling
	go func() {
		sig := <-sigs
		fmt.Fprintf(os.Stderr, "Aborting! Caught signal \"%s\"\n", sig)
		fmt.Fprintf(os.Stderr, "Cleaning up...\n")
		cancel()
		// and if they insist, catch a second CTRL-C or timeout on 10sec
		select {
		case <-sigs:
			fmt.Fprintf(os.Stderr, "Caught second signal... Aborting.\n")
			os.Exit(1)
		case <-time.After(10 * time.Second):
			fmt.Fprintf(os.Stderr, "Taking too long... Aborting.\n")
			os.Exit(1)
		}
	}()

	// for each channel we got back from tail.GetEntries, spin up a parser.
	parsersWG := sync.WaitGroup{}
	responsesWG := sync.WaitGroup{}
	for _, lines := range linesChans {
		// get our parser
		parser, opts := getParserAndOptions(options)
		if parser == nil {
			logrus.WithFields(logrus.Fields{"parser": options.Reqs.ParserName}).Fatal(
				"Parser not found. Use --list to show valid parsers")
		}

		// and initialize it
		if err := parser.Init(opts); err != nil {
			logrus.Fatalf(
				"Error initializing %s parser module: %v", options.Reqs.ParserName, err)
		}

		// create a channel for sending events
		toBeSent := make(chan event.Event, options.NumSenders)
		doneSending := make(chan bool)

		// two channels to handle backing off when rate limited and resending failed
		// send attempts that are recoverable
		toBeResent := make(chan event.Event, 2*options.NumSenders)
		// time in milliseconds to delay the send
		delaySending := make(chan int, 2*options.NumSenders)

		// apply any filters to the events before they get sent
		modifiedToBeSent := modifyEventContents(toBeSent, options)

		realToBeSent := make(chan event.Event, 10*options.NumSenders)
		go func() {
			wg := sync.WaitGroup{}
			for i := uint(0); i < options.NumSenders; i++ {
				wg.Add(1)
				go func() {
					for ev := range modifiedToBeSent {
						realToBeSent <- ev
					}
					wg.Done()
				}()
			}
			wg.Wait()
			close(realToBeSent)
		}()

		// start up the sender
		go sendToHeplify(ctx, realToBeSent, toBeResent, delaySending, doneSending)

		parsersWG.Add(1)
		go func(plines chan string) {
			// ProcessLines won't return until lines is closed
			parser.ProcessLines(plines, toBeSent, prefixRegex)
			// trigger the sending goroutine to finish up
			close(toBeSent)
			// wait for all the events in toBeSent to be handed to sender
			<-doneSending
			parsersWG.Done()
		}(lines)
	}
	parsersWG.Wait()
	// print out what we've done one last time
	responsesWG.Wait()

	// Nothing bad happened, yay
	logrus.Info("cirlog is all done, goodbye!")
}

// getParserOptions takes a parser name and the global options struct
// it returns the options group for the specified parser
func getParserAndOptions(options GlobalOptions) (parsers.Parser, interface{}) {
	var parser parsers.Parser
	var opts interface{}
	switch options.Reqs.ParserName {
	case "regex":
		parser = &regex.Parser{}
		opts = &options.Regex
		opts.(*regex.Options).NumParsers = int(options.NumSenders)
	case "json":
		parser = &htjson.Parser{}
		opts = &options.JSON
		opts.(*htjson.Options).NumParsers = int(options.NumSenders)
	case "keyval":
		parser = &keyval.Parser{}
		opts = &options.KeyVal
		opts.(*keyval.Options).NumParsers = int(options.NumSenders)
	case "csv":
		parser = &csv.Parser{}
		opts = &options.CSV
		opts.(*csv.Options).NumParsers = int(options.NumSenders)
	case "syslog":
		parser = &syslog.Parser{}
		opts = &options.Syslog
		opts.(*syslog.Options).NumParsers = int(options.NumSenders)
	}
	parser, _ = parser.(parsers.Parser)
	return parser, opts
}

// modifyEventContents takes a channel from which it will read events. It
// returns a channel on which it will send the munged events. It is responsible
// for hashing or dropping or adding fields to the events and doing the dynamic
// sampling, if enabled
func modifyEventContents(toBeSent chan event.Event, options GlobalOptions) chan event.Event {
	// parse the addField bit once instead of for every event
	parsedAddFields := map[string]string{}
	for _, addField := range options.AddFields {
		splitField := strings.SplitN(addField, "=", 2)
		if len(splitField) != 2 {
			logrus.WithFields(logrus.Fields{
				"add_field": addField,
			}).Fatal("unable to separate provided field into a key=val pair")
		}
		parsedAddFields[splitField[0]] = splitField[1]
	}

	// initialize the data augmentation map
	// map contents are sourceFieldValue -> object containing new keys and values
	// {"sourceField":{"val1":{"newKey1":"newVal1","newKey2":"newVal2"},"val2":{"newKey1":"newValA"}}}
	type DataAugmentationMap map[string]map[string]map[string]interface{}
	var daMap DataAugmentationMap
	if options.DAMapFile != "" {
		raw, err := ioutil.ReadFile(options.DAMapFile)
		if err != nil {
			logrus.WithField("error", err).Fatal("failed to read Data Augmentation Map file")
		}
		err = json.Unmarshal(raw, &daMap)
		if err != nil {
			logrus.WithField("error", err).Fatal("failed to unmarshal Data Augmentation Map from JSON")
		}
	}

	var baseTime, startTime time.Time
	if options.RebaseTime {
		var err error
		baseTime, err = getBaseTime(options)
		if err != nil {
			logrus.WithError(err).Fatal("--rebase_time specified but cannot rebase")
		}
		startTime = time.Now()
	}

	// ok, we need to munge events. Sing up enough goroutines to handle this
	newSent := make(chan event.Event, options.NumSenders)
	go func() {
		wg := sync.WaitGroup{}
		for i := uint(0); i < options.NumSenders; i++ {
			wg.Add(1)
			go func() {
				for ev := range toBeSent {

					// do data augmentation. For each source column
					for sourceField, augmentableVals := range daMap {
						// does that column exist in the event?
						if val, ok := ev.Data[sourceField]; ok {
							// if it does exist, is it a string?
							if val, ok := val.(string); ok {
								// if we have fields to augment this value
								if newFields, ok := augmentableVals[val]; ok {
									// go ahead and insert new fields
									for k, v := range newFields {
										ev.Data[k] = v
									}
								}
							}
						}
					}
					// do dropping
					for _, field := range options.DropFields {
						delete(ev.Data, field)
					}
					// do scrubbing
					for _, field := range options.ScrubFields {
						if val, ok := ev.Data[field]; ok {
							// generate a sha256 hash and use the base16 for the content
							newVal := sha256.Sum256([]byte(fmt.Sprintf("%v", val)))
							ev.Data[field] = fmt.Sprintf("%x", newVal)
						}
					}
					// do adding
					for k, v := range parsedAddFields {
						ev.Data[k] = v
					}
					// get presampled field if it exists
					if options.PreSampledField != "" {
						var presampledRate int
						if psr, ok := ev.Data[options.PreSampledField]; ok {
							switch psr := psr.(type) {
							case float64:
								presampledRate = int(psr)
							case string:
								if val, err := strconv.Atoi(psr); err == nil {
									presampledRate = val
								}
							}
						}
						ev.SampleRate = presampledRate
					}
					if options.RebaseTime {
						ev.Timestamp = rebaseTime(baseTime, startTime, ev.Timestamp)
					}
					if len(options.JSONFields) > 0 {
						for _, field := range options.JSONFields {
							jsonVal, ok := ev.Data[field].(string)
							if !ok {
								logrus.WithField("field", field).
									Warn("Error asserting given field as string")
								continue
							}
							var jsonMap map[string]interface{}
							if err := json.Unmarshal([]byte(jsonVal), &jsonMap); err != nil {
								logrus.WithField("field", field).
									Warn("Error unmarshalling field as JSON")
								continue
							}

							ev.Data[field] = jsonMap
						}
					}
					newSent <- ev
				}
				wg.Done()
			}()
		}
		wg.Wait()
		close(newSent)
	}()
	return newSent
}

// makeDynsampleKey pulls in all the values necessary from the event to create a
// key for dynamic sampling
func makeDynsampleKey(ev *event.Event, options GlobalOptions) string {
	key := make([]string, len(options.DynSample))
	for i, field := range options.DynSample {
		if val, ok := ev.Data[field]; ok {
			switch val := val.(type) {
			case bool:
				key[i] = strconv.FormatBool(val)
			case int64:
				key[i] = strconv.FormatInt(val, 10)
			case float64:

				key[i] = strconv.FormatFloat(val, 'E', -1, 64)
			case string:
				key[i] = val
			default:
				key[i] = "" // skip it
			}
		}
	}
	return strings.Join(key, "_")
}

// sendToHeplify reads from the toBeSent channel and sends the events.
func sendToHeplify(ctx context.Context, toBeSent chan event.Event, toBeResent chan event.Event,
	delaySending chan int, doneSending chan bool) {
	for {
		// check and see if we need to back off the API because of rate limiting
		select {
		case delay := <-delaySending:
			time.Sleep(time.Duration(delay) * time.Millisecond)
		default:
		}
		// if we have events to retransmit, send those first
		select {
		case ev := <-toBeResent:
			// retransmitted events have already been sampled; always use
			// SendPresampled() for these
			sendEvent(ev)
			continue
		default:
		}
		// otherwise pick something up off the regular queue and send it
		select {
		case ev, ok := <-toBeSent:
			if !ok {
				// channel is closed
				// NOTE: any unrtransmitted retransmittable events will be dropped
				doneSending <- true
				return
			}
			sendEvent(ev)
			continue
		default:
		}
		// no events at all? chill for a sec until we get the next one
		time.Sleep(100 * time.Millisecond)
	}
}

// sendEvent does the actual handoff to sender
func sendEvent(ev event.Event) {
	if ev.SampleRate == -1 {
		// drop the event!
		logrus.WithFields(logrus.Fields{
			"event": ev,
		}).Debug("droppped event due to sampling")
		return
	}
	/* 	libhEv := //TODO
	   	libhEv.Metadata = ev
	   	libhEv.Timestamp = ev.Timestamp
	   	libhEv.SampleRate = uint(ev.SampleRate)
	   	if err := libhEv.Add(ev.Data); err != nil {
	   		logrus.WithFields(logrus.Fields{
	   			"event": ev,
	   			"error": err,
	   		}).Error("Unexpected error adding data to event")
	   	}
	   	if err := libhEv.SendPresampled(); err != nil {
	   		logrus.WithFields(logrus.Fields{
	   			"event": ev,
	   			"error": err,
	   		}).Error("Unexpected error event")
	   	} */
}

func getBaseTime(options GlobalOptions) (time.Time, error) {
	var baseTime time.Time

	// support multiple files and globs, although this is unlikely to be used
	searchFiles := []string{}
	for _, f := range options.Reqs.LogFiles {
		// can't work with stdin
		if f == "-" {
			continue
		}
		// can't work with files that don't exist
		if files, err := filepath.Glob(f); err == nil && files != nil {
			searchFiles = append(searchFiles, files...)
		}
	}
	if len(searchFiles) == 0 {
		return baseTime, fmt.Errorf("unable to get base time, no files found")
	}

	// we're going to have to parse lines, so get an instance of the parser
	parser, parserOpts := getParserAndOptions(options)
	parser.Init(parserOpts)
	lines := make(chan string)
	events := make(chan event.Event)
	var prefixRegex *parsers.ExtRegexp
	if options.PrefixRegex == "" {
		prefixRegex = nil
	} else {
		prefixRegex = &parsers.ExtRegexp{regexp.MustCompile(options.PrefixRegex)}
	}
	// read each file, throw the last line on the lines channel
	go getEndLines(searchFiles, lines)
	// the parser will parse each line and give us an event
	go func() {
		// ProcessLines will stop when the lines channel closes
		parser.ProcessLines(lines, events, prefixRegex)
		// Signal that we're done sending events
		close(events)
	}()

	// we read the event and find the latest timestamp
	// this is our base time (assuming the input files are sorted by time,
	// otherwise we'd have to parse *everything*)
	for ev := range events {
		if ev.Timestamp.After(baseTime) {
			baseTime = ev.Timestamp
		}
	}

	return baseTime, nil
}

func getEndLines(files []string, lines chan<- string) {
	for _, f := range files {
		lines <- getEndLine(f)
	}

	close(lines)
}

func getEndLine(file string) string {
	handle, err := os.Open(file)
	if err != nil {
		logrus.WithError(err).WithField("file", file).
			Fatal("unable to open file")
	}
	defer handle.Close()

	info, err := os.Stat(file)
	if err != nil {
		logrus.WithError(err).WithField("file", file).
			Fatal("unable to stat file")
	}
	// If we're bigger than 2m, zoom to the end of the file and go back 1mb
	// 2m is an arbitrary limit
	if info.Size() > 2*1024*1024 {
		_, err := handle.Seek(-1024*1024, io.SeekEnd)
		if err != nil {
			logrus.WithError(err).WithField("file", file).
				Fatal("unable to seek to last megabyte of file")
		}
	}

	// we use a scanner to read to the last line
	// not the most efficient
	scanner := bufio.NewScanner(handle)
	var line string
	for scanner.Scan() {
		line = scanner.Text()
	}

	if scanner.Err() != nil {
		logrus.WithError(err).WithField("file", file).
			Fatal("unable to read to end of file")
	}

	return line
}

func rebaseTime(baseTime, startTime, timestamp time.Time) time.Time {
	// Figure out the gap between the event and the end of our event window
	delta := baseTime.UnixNano() - timestamp.UnixNano()
	// Create a new time relative to the current time
	return startTime.Add(time.Duration(delta) * time.Duration(-1))
}
