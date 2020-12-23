# eventbridge-pipe

[![ci](https://github.com/WillAbides/eventbridge-pipe/workflows/ci/badge.svg?branch=main&event=push)](https://github.com/WillAbides/eventbridge-pipe/actions?query=workflow%3Aci+branch%3Amaster+event%3Apush)

## Install

`go get github.com/willabides/eventbridge-pipe/cmd/ebpipe`

## Usage

```
Usage: ebpipe --type=STRING --source=STRING

ebpipe posts events to AWS EventBridge.

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

Flags:
  -h, --help                     Show context-sensitive help.
      --region="us-east-1"       The aws region to publish events to.
  -t, --type=STRING              Value for the DetailType field. JMESPath expressions allowed
                                 with "jp:" prefix.
  -b, --event-bus=STRING         Value for the "EventBusName" field.
  -r, --resource=RESOURCE,...    An element for the list in the "Resources" array. JMESPath
                                 expressions allowed with "jp:" prefix.
  -s, --source=STRING            Value for the "Source" field. JMESPath expressions allowed with
                                 "jp:" prefix.
  -T, --timestamp=STRING         Value for the "Time" field converted from epoch milliseconds.
                                 JMESPath expressions allowed with "jp:" prefix.
      --batch-size=10            Number of events to send in a batch.
      --flush-interval=2000      Time in milliseconds to wait before sending a partial batch.
                                 Set to 0 to never send a partial batch.
```
