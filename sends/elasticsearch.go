package sends

import (
	"fmt"
	ego "github.com/mattbaird/elastigo/lib"
	r "github.com/parnurzeal/gorequest"
	"strings"
	"time"
)

type JSONTime time.Time

const defaultMapping string = "{ \"mappings\" : { \"_default_\" : { \"dynamic_templates\" : [ { \"string_fields\" : { \"mapping\" : { \"index\" : \"analyzed\", \"omit_norms\" : true, \"type\" : \"string\", \"fields\" : { \"raw\" : { \"index\" : \"not_analyzed\", \"ignore_above\" : 256, \"type\" : \"string\" } } }, \"match\" : \"*\", \"match_mapping_type\" : \"string\" } } ], \"_all\" : { \"enabled\" : true }, \"properties\" : { \"@version\" : { \"type\" : \"string\", \"index\" : \"not_analyzed\" }, \"@timestamp\" : { \"type\" : \"date\", \"format\" : \"dateOptionalTime\" } } } } }"

func (t JSONTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02T15:04:05.9999Z0700"))
	return []byte(stamp), nil
}

func (sr *SendRunner) ElasticsearchGo(payload Payload) {
	/*
		The elasticsearch sender makes a couple of assumptions:
		- The payload.message.Group is the index
		- the payload.message.Source is the type
	*/

	t := time.Now().Local()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")

	debug := payload.Settings.Get("debug").(bool)
	group := payload.Message.Group
	if debug {
		fmt.Printf("[%s] INFO Elasticsearch group: %s\n", ts, group)
	}
	source := payload.Message.Source
	if debug {
		fmt.Printf("[%s] INFO Elasticsearch source: %s\n", ts, source)
	}
	host := payload.Settings.Get("sends", "elasticsearch", group, "host").(string)
	if debug {
		fmt.Printf("[%s] INFO Elasticsearch host: %s\n", ts, host)
	}
	port := payload.Settings.Get("sends", "elasticsearch", group, "port").(int)
	if debug {
		fmt.Printf("[%s] INFO Elasticsearch port: %d\n", ts, port)
	}

	connect_string := fmt.Sprintf("http://%s:%d/%s", host, port, group)

	type ESMessage struct {
		Timestamp JSONTime `json:"@timestamp"`
		Timein    string   `json:"time_in"`
		Subject   string   `json:"subject"`
		Detail    string   `json:"detail"`
		Severity  int      `json:"severity"`
		Extra     string   `json:"extra"`
	}

	var esm ESMessage
	esm.Timestamp = JSONTime(t)
	esm.Timein = payload.Message.Timestamp
	esm.Subject = payload.Message.Subject
	esm.Detail = payload.Message.Detail
	esm.Severity = payload.Message.Severity
	esm.Extra = payload.Message.Extra

	e := ego.NewConn()
	e.Domain = host
	if debug {
		fmt.Printf("[%s] INFO Connected to Elasticsearch host %s\n", ts, host)
	}

	// Check to see if index exists, if not apply mapping
	index_exists, erri := e.IndicesExists(group)
	if erri != nil {
		fmt.Printf("[%s] ERROR Elasticsearch index search error received: %s\n", ts, erri)
	}
	if debug {
		fmt.Printf("[%s] INFO Response from Elasticsearch index search: %s\n", ts, index_exists)
	}

	if index_exists == false {
		// Elastigo is nice, but a pain for putting mappings. Just use a http call.
		request := r.New()
		resp, body, errm := request.Post(connect_string).Send(defaultMapping).End()
		if len(errm) > 0 {
			for _, m := range errm {
				fmt.Printf("[%s] ERROR Elasticsearch mapping error received: %s\n", ts, m)
			}
		} else {
			if strings.Contains(body, "{\"acknowledged\":true}") {
				if debug {
					fmt.Printf("[%s] INFO Elasticsearch mapping completed.\n", ts)
				}
			} else {
				fmt.Printf("[%s] ERROR Elasticsearch mapping error received: %s\n", ts, body)
			}
		}
	}

	response, errs := e.Index(group, source, "", nil, esm)
	if errs != nil {
		fmt.Printf("[%s] ERROR Elasticsearch indexing error received: %s\n", ts, errs)
	}
	if debug {
		fmt.Printf("[%s] INFO Response from Elasticsearch host: %+v\n", ts, response)
	}
}
