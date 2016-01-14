package main

import (
	"encoding/json"
	//"errors"
	//"flag"
	"fmt"
	s "github.com/andrew-sledge/redshift/sends"
	"github.com/fzzy/radix/extra/cluster"
	"github.com/fzzy/radix/redis"
	"menteslibres.net/gosexy/yaml"
	"reflect"
	//"io/ioutil"
	"os"
	//"strconv"
	"strings"
	"time"
)

func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func Pull(settings *yaml.Yaml, senders []string) {

	debug := settings.Get("debug").(bool)
	redis_connection := settings.Get("redis_connection").(string)
	redis_db := settings.Get("redis_db").(int)
	redis_list := settings.Get("redis_list").(string)
	redis_watch_interval := settings.Get("redis_watch_interval").(int)

	/*
		Using a FIFO queue
	*/
	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	rc, err := cluster.NewCluster(redis_connection)

	s.CheckError(err, true)
	r := rc.Cmd("SELECT", redis_db)

	for {
		t = time.Now()
		ts = t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
		r = rc.Cmd("RPOP", redis_list)

		switch r.Type {
		case redis.ErrorReply:
			fmt.Printf("[%s] ERROR ErrorReply received: %s\n", ts, r.Err.Error())
		case redis.NilReply:
			if debug {
				fmt.Printf("[%s] INFO NilReply reply received\n", ts)
			}
		case redis.StatusReply:
			if debug {
				fmt.Printf("[%s] INFO StatusReply reply received: not processing\n", ts)
			}
		case redis.BulkReply:
			data, err := r.Bytes()
			if err != nil {
				fmt.Printf("[%s] ERROR Error received: %s\n", ts, err)
			} else {
				var message s.Message
				err := json.Unmarshal(data, &message)
				if err != nil {
					fmt.Printf("[%s] ERROR Error received: %s\n", ts, err)
				}

				// Fan out to sends
				var sr s.SendRunner
				var payload s.Payload
				payload.Message = message
				payload.ProcessTime = ts
				payload.Settings = *settings
				var payloads []s.Payload

				payloads = append(payloads, payload)
				in := make([]reflect.Value, len(payloads))

				for i, param := range payloads {
					in[i] = reflect.ValueOf(param)
				}

				for _, send_func := range senders {
					reflect.ValueOf(&sr).MethodByName(send_func).Call(in)
					if debug {
						fmt.Printf("[%s] INFO Message %+v successfully sent to %s.", ts, message, send_func)
					}
				}
			}
		case redis.MultiReply:
			if debug {
				fmt.Printf("[%s] INFO MultiReply reply received: not processing\n", ts)
			}
		case redis.IntegerReply:
			if debug {
				fmt.Printf("[%s] INFO IntegerReply reply received: not processing\n", ts)
			}
		default:
			if debug {
				fmt.Printf("[%s] INFO Unknown reply received: not processing\n", ts)
			}
		}

		time.Sleep(time.Duration(redis_watch_interval) * time.Millisecond)
	}

}

func main() {
	var yml_config_file string
	y_c, e := os.LookupEnv("REDSHIFT_CONFIG")
	if e == false {
		yml_config_file = "config.yml"
	} else {
		yml_config_file = y_c
	}

	settings, err := yaml.Open(yml_config_file)
	s.CheckError(err, true)

	senders := make([]string, 0)

	k := settings.Get("sends")

	for m, _ := range k.(map[interface{}]interface{}) {
		n := fmt.Sprintf("%sGo", strings.Title(m.(string)))
		senders = append(senders, n)
	}

	Pull(settings, senders)
}
