package main

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPutter struct {
	t    testing.TB
	want [][]*eventbridge.PutEventsRequestEntry
}

func newTestPutter(t testing.TB) *testPutter {
	t.Helper()
	p := &testPutter{
		t: t,
	}
	t.Cleanup(func() {
		assert.Empty(t, p.want)
	})
	return p
}

func (p *testPutter) expect(expect []*eventbridge.PutEventsRequestEntry) {
	p.want = append(p.want, expect)
}

func (p *testPutter) putEvents(_ context.Context, cache []*eventbridge.PutEventsRequestEntry) error {
	t := p.t
	t.Helper()
	if len(p.want) == 0 {
		err := fmt.Errorf("unexpected request")
		assert.NoError(t, err)
		return err
	}
	assert.Equal(t, p.want[0], cache)
	p.want = p.want[1:]
	return nil
}

func Test_run(t *testing.T) {
	ctx := context.Background()

	lines := []string{
		`{"id": "foo", "time": "1608309835000", "type": "foo"}`,
		``,
		` `,
		`{"id": "bar", "time": "1608309835000", "type": "bar"}`,
		`{"id": "baz", "time": "1608309835000", "type": "baz"}`,
		`{"id": "qux", "time": 1608309835000, "type": "qux"}`,
	}

	scanner := bufio.NewScanner(strings.NewReader(strings.Join(lines, "\n")))

	tm := time.Unix(1608309835, 0).UTC()

	p := newTestPutter(t)
	p.expect([]*eventbridge.PutEventsRequestEntry{
		{
			Detail:       &lines[0],
			DetailType:   aws.String("foo"),
			EventBusName: aws.String("a-bus"),
			Resources:    []*string{aws.String("a resource"), aws.String("foo")},
			Source:       aws.String("the-cloud"),
			Time:         &tm,
		}, {
			Detail:       &lines[3],
			DetailType:   aws.String("bar"),
			EventBusName: aws.String("a-bus"),
			Resources:    []*string{aws.String("a resource"), aws.String("bar")},
			Source:       aws.String("the-cloud"),
			Time:         &tm,
		}, {
			Detail:       &lines[4],
			DetailType:   aws.String("baz"),
			EventBusName: aws.String("a-bus"),
			Resources:    []*string{aws.String("a resource"), aws.String("baz")},
			Source:       aws.String("the-cloud"),
			Time:         &tm,
		},
	})
	p.expect([]*eventbridge.PutEventsRequestEntry{
		{
			Detail:       &lines[5],
			DetailType:   aws.String("qux"),
			EventBusName: aws.String("a-bus"),
			Resources:    []*string{aws.String("a resource"), aws.String("qux")},
			Source:       aws.String("the-cloud"),
			Time:         &tm,
		},
	})

	cli := &cliOptions{
		Region:        "us-east-1",
		DetailType:    "jp:type",
		EventBus:      "a-bus",
		Resource:      []string{"a resource", "jp:type"},
		Source:        "the-cloud",
		Time:          "1608309835000",
		BatchSize:     3,
		FlushInterval: 0,
		_putter:       p,
	}

	err := run(ctx, cli, scanner)
	require.NoError(t, err)
}
