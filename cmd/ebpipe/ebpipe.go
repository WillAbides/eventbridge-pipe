package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/alecthomas/kong"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/jmespath/go-jmespath"
)

var kongVars = kong.Vars{
	"batch_size_help":     `Number of events to send in a batch.`,
	"flush_interval_help": `Time in milliseconds to wait before sending a partial batch. Set to 0 to never send a partial batch.`,
	"region_help":         `The aws region to publish events to.`,
	"detail_type_help":    `Value for the DetailType field. JMESPath expressions allowed with "jp:" prefix.`,
	"event_bus_help":      `Value for the "EventBusName" field.`,
	"resource_help":       `An element for the list in the "Resources" array. JMESPath expressions allowed with "jp:" prefix.`,
	"source_help":         `Value for the "Source" field. JMESPath expressions allowed with "jp:" prefix.`,
	"time_help":           `Value for the "Time" field converted from epoch milliseconds. JMESPath expressions allowed with "jp:" prefix.`,
}

type cliOptions struct {
	Region        string   `kong:"default=us-east-1,help=${region_help}"`
	DetailType    string   `kong:"required,name=type,short=t,help=${detail_type_help}"`
	EventBus      string   `kong:"short=b,help=${event_bus_help}"`
	Resource      []string `kong:"short=r,help=${resource_help}"`
	Source        string   `kong:"required,short=s,help=${source_help}"`
	Time          string   `kong:"name=timestamp,short=T,help=${time_help}"`
	BatchSize     int      `kong:"default=10,help=${batch_size_help}"`
	FlushInterval int      `kong:"default=2000,help=${flush_interval_help}"`

	jmespaths map[string]*jmespath.JMESPath
	optDefs   map[string]string
	_putter   eventPutter
}

const helpDescription = `ebpipe posts events to AWS EventBridge.

example:
  $ AWS_ACCESS_KEY='AKIA****************'
  $ AWS_SECRET_KEY='shhh_this_is_a_secret'
  $ data="$(cat <<"EOF"
      {"action": "obj.add", "@timestamp": 1604953432032, "el_name": "foo", "doc_id": "asdf"}
      {"action": "obj.rem", "@timestamp": 1604953732032, "el_name": "bar", "doc_id": "fdsa"}
    EOF
    )"
  $ echo "$data" | \
    ebpipe -s 'test-source' -t 'jp:action' -b 'my-bus' -T 'jp:"@timestamp"' \
    -r 'jp:"el_name"' 

Learn about JMESPath syntax at https://jmespath.org
`

const jmespathPrefix = "jp:"

func main() {
	var cli cliOptions
	k := kong.Parse(&cli, kongVars, kong.Description(helpDescription))
	scanner := bufio.NewScanner(os.Stdin)
	ctx := context.Background()
	err := run(ctx, &cli, scanner)
	k.FatalIfErrorf(err)
}

type lineData struct {
	data  []byte
	iface interface{}
}

func (l lineData) unmarshalled() (interface{}, error) {
	if l.iface == nil {
		err := json.Unmarshal(l.data, &l.iface)
		if err != nil {
			return nil, err
		}
	}
	return l.iface, nil
}

func run(ctx context.Context, cli *cliOptions, scanner *bufio.Scanner) error {
	if cli.BatchSize > 10 {
		return fmt.Errorf("batch size exceeds aws maximum")
	}
	p, err := cli.putter()
	if err != nil {
		return err
	}
	publisher := &eventBridgePublisher{
		maxQueueSize: cli.BatchSize,
		putter:       p,
		resetTicker:  func() {},
	}

	doneMutex := new(sync.Mutex)
	done := false
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		doneMutex.Lock()
		done = true
		doneMutex.Unlock()
	}()

	if cli.FlushInterval != 0 {
		interval := time.Duration(cli.FlushInterval) * time.Millisecond
		ticker := time.NewTicker(interval)
		publisher.resetTicker = func() {
			ticker.Reset(interval)
		}
		go func() {
			for range ticker.C {
				err2 := publisher.flushIfNeeded(ctx, 0)
				if err2 != nil {
					os.Exit(1)
				}
			}
		}()
	}

	for scanner.Scan() {
		b := scanner.Bytes()
		b = bytes.TrimSpace(b)
		if len(b) == 0 {
			continue
		}
		var ev *eventbridge.PutEventsRequestEntry
		ev, err = buildEvent(cli, scanner.Bytes())
		if err != nil {
			return err
		}
		err = publisher.addEvent(ctx, ev)
		if err != nil {
			return err
		}
		if done {
			break
		}
	}

	err = publisher.flushIfNeeded(ctx, 0)
	if err != nil {
		return err
	}
	return scanner.Err()
}

func (c *cliOptions) jmespath(name, val string) (*jmespath.JMESPath, error) {
	var err error
	if !strings.HasPrefix(val, jmespathPrefix) {
		return nil, nil
	}
	if c.jmespaths == nil {
		c.jmespaths = map[string]*jmespath.JMESPath{}
	}
	if c.jmespaths[name] == nil {
		c.jmespaths[name], err = jmespath.Compile(strings.TrimPrefix(val, jmespathPrefix))
		if err != nil {
			return nil, err
		}
	}
	return c.jmespaths[name], nil
}

func (c *cliOptions) putter() (eventPutter, error) {
	if c._putter != nil {
		return c._putter, nil
	}
	config := aws.NewConfig()
	config = config.WithRegion(c.Region)
	config = config.WithCredentials(
		credentials.NewEnvCredentials(),
	)
	sess, err := session.NewSession(config)
	if err != nil {
		return nil, err
	}
	return &awsPutter{
		svc: eventbridge.New(sess),
	}, nil
}

func (c *cliOptions) optDef(name string) string {
	if c.optDefs == nil {
		c.optDefs = map[string]string{
			"DetailType": c.DetailType,
			"Source":     c.Source,
			"Time":       c.Time,
		}
	}
	return c.optDefs[name]
}

func (c *cliOptions) getVal(valName string, data lineData) (string, error) {
	optDef := c.optDef(valName)

	if strings.HasPrefix(optDef, jmespathPrefix) {
		jp, err := c.jmespath(valName, optDef)
		if err != nil {
			return "", err
		}
		jd, err := data.unmarshalled()
		if err != nil {
			return "", err
		}
		return jmespathString(jp, jd)
	}
	return optDef, nil
}

func buildEvent(cli *cliOptions, data []byte) (*eventbridge.PutEventsRequestEntry, error) {
	dataStr := string(data)
	ev := eventbridge.PutEventsRequestEntry{
		Detail: &dataStr,
	}
	if cli.EventBus != "" {
		ev.EventBusName = &cli.EventBus
	}

	ld := lineData{
		data: data,
	}

	detailType, err := cli.getVal("DetailType", ld)
	if err != nil {
		return nil, err
	}
	if detailType != "" {
		ev.DetailType = &detailType
	}

	source, err := cli.getVal("Source", ld)
	if err != nil {
		return nil, err
	}
	if source != "" {
		ev.Source = &source
	}

	eventTime, err := cli.eventTime(ld)
	if err != nil {
		return nil, err
	}
	if eventTime != nil {
		ev.Time = eventTime
	}

	resources, err := cli.resources(ld)
	if err != nil {
		return nil, err
	}
	if len(resources) != 0 {
		ev.Resources = resources
	}

	return &ev, nil
}

func (c *cliOptions) resources(ld lineData) ([]*string, error) {
	if len(c.Resource) == 0 {
		return nil, nil
	}
	result := make([]*string, len(c.Resource))
	for i := range c.Resource {
		r := c.Resource[i]
		if !strings.HasPrefix(r, jmespathPrefix) {
			result[i] = &r
			continue
		}
		jp, err := c.jmespath(fmt.Sprintf("r%d", i), r)
		if err != nil {
			return nil, err
		}
		jd, err := ld.unmarshalled()
		if err != nil {
			return nil, err
		}
		js, err := jmespathString(jp, jd)
		if err != nil {
			return nil, err
		}
		result[i] = &js
	}
	return result, nil
}

func (c *cliOptions) eventTime(ld lineData) (*time.Time, error) {
	strVal, err := c.getVal("Time", ld)
	if err != nil {
		return nil, err
	}
	switch strVal {
	case "":
		return nil, nil
	case "now":
		now := time.Now().UTC()
		return &now, nil
	}
	iVal, err := strconv.ParseInt(strVal, 10, 64)
	if err != nil {
		return nil, err
	}
	secs := iVal / 1000
	ms := iVal % 1000
	ns := ms * int64(time.Millisecond)
	tm := time.Unix(secs, ns).UTC()
	return &tm, nil
}

func jmespathString(jp *jmespath.JMESPath, data interface{}) (string, error) {
	got, err := jp.Search(data)
	if err != nil {
		return "", err
	}
	switch val := got.(type) {
	case string:
		return val, nil
	case float64:
		return fmt.Sprintf("%.0f", val), nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

type eventBridgePublisher struct {
	mutex        sync.Mutex
	maxQueueSize int
	cache        []*eventbridge.PutEventsRequestEntry
	putter       eventPutter
	resetTicker  func()
}

func (p *eventBridgePublisher) addEvent(ctx context.Context, ev *eventbridge.PutEventsRequestEntry) error {
	p.mutex.Lock()
	p.cache = append(p.cache, ev)
	if len(p.cache) == 1 {
		p.resetTicker()
	}
	p.mutex.Unlock()
	return p.flushIfNeeded(ctx, p.maxQueueSize)
}

func (p *eventBridgePublisher) flushIfNeeded(ctx context.Context, maxQueueSize int) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if len(p.cache) == 0 || len(p.cache) < maxQueueSize {
		return nil
	}
	err := p.putter.putEvents(ctx, p.cache)
	if err != nil {
		return err
	}
	p.cache = p.cache[:0]
	return nil
}

type awsPutter struct {
	svc *eventbridge.EventBridge
}

func (p *awsPutter) putEvents(ctx context.Context, cache []*eventbridge.PutEventsRequestEntry) error {
	resp, err := p.svc.PutEventsWithContext(ctx, &eventbridge.PutEventsInput{
		Entries: cache,
	})
	if err != nil {
		return err
	}
	if resp.FailedEntryCount != nil {
		if *resp.FailedEntryCount != 0 {
			return fmt.Errorf("one or more failed entries: %s", resp.String())
		}
	}
	return nil
}

type eventPutter interface {
	putEvents(ctx context.Context, cache []*eventbridge.PutEventsRequestEntry) error
}
