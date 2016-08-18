package main

import (
	"encoding/json"
	"errors"
	"fmt"
	s "github.com/andrew-sledge/redshift/sends"
	"github.com/fzzy/radix/extra/cluster"
	"github.com/fzzy/radix/redis"
	"menteslibres.net/gosexy/yaml"
	"os"
	"reflect"
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

func Send(ts string, data []byte, settings *yaml.Yaml, senders []string) error {

	debug := settings.Get("debug").(bool)

	var message s.Message
	err := json.Unmarshal(data, &message)
	if err != nil {
		e := fmt.Sprintf("[%s] ERROR Error received: %s\n", ts, err)
		return errors.New(e)
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
	return nil
}

func PullNode(settings *yaml.Yaml) (rc *redis.Client) {
	redis_connection := settings.Get("redis_connection").(string)
	rc, err := redis.DialTimeout("tcp", redis_connection, time.Duration(10)*time.Second)
	s.CheckError(err, true)
	return rc
}

func PullCluster(settings *yaml.Yaml) (rc *cluster.Cluster) {
	redis_connection := settings.Get("redis_connection").(string)
	rc, err := cluster.NewClusterTimeout(redis_connection, time.Duration(10)*time.Second)
	s.CheckError(err, true)
	return rc
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
	debug := settings.Get("debug").(bool)
	senders := make([]string, 0)

	k := settings.Get("sends")

	for m, _ := range k.(map[interface{}]interface{}) {
		n := fmt.Sprintf("%sGo", strings.Title(m.(string)))
		senders = append(senders, n)
	}

	// There's probably a better way to do this
	redis_is_cluster := settings.Get("redis_is_cluster").(bool)
	redis_list := settings.Get("redis_list").(string)
	redis_watch_interval := settings.Get("redis_watch_interval").(int)
	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	var rClient *redis.Client
	var rCluster *cluster.Cluster
	if redis_is_cluster {
		if debug {
			fmt.Printf("[%s] INFO Starting up in cluster mode\n", ts)
		}
		rCluster = PullCluster(settings)
		defer rCluster.Close()
	} else {
		redis_db := settings.Get("redis_db").(int)
		if debug {
			fmt.Printf("[%s] INFO Starting up in single node mode\n", ts)
		}
		rClient = PullNode(settings)
		defer rClient.Close()
		rClient.Cmd("SELECT", redis_db)
	}
	var r *redis.Reply
	if redis_is_cluster {
		r = rCluster.Cmd("RPOP", redis_list)
	} else {
		r = rClient.Cmd("RPOP", redis_list)
	}
	for {
		if redis_is_cluster {
			r = rCluster.Cmd("RPOP", redis_list)
		} else {
			r = rClient.Cmd("RPOP", redis_list)
		}
		t = time.Now()
		ts = t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")

		// If the response from redis is actual data, process and then pop another response off
		// If not, who cares, go through the old switch case
		for (r.Type == redis.BulkReply) {
			t = time.Now()
			ts = t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
			data, err := r.Bytes()
			if debug {
				fmt.Printf("[%s] INFO BulkReply reply received. Data length %d\n", ts, len(data))
			}
			if err != nil {
				fmt.Printf("[%s] ERROR Error received: %s\n", ts, err)
			} else {
				err := Send(ts, data, settings, senders)
				if err != nil {
					fmt.Printf(err.Error())
				}
			}
			if redis_is_cluster {
				r = rCluster.Cmd("RPOP", redis_list)
			} else {
				r = rClient.Cmd("RPOP", redis_list)
			}
		}
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
