package sends

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
)

/*
path
translate_syslog
include_timestamp
*/

func TranslateSyslog(syslog int) (string, error) {
	switch syslog {
	case 0:
		return "EMERGENCY", nil
	case 1:
		return "ALERT", nil
	case 2:
		return "CRITICAL", nil
	case 3:
		return "ERROR", nil
	case 4:
		return "WARNING", nil
	case 5:
		return "NOTICE", nil
	case 6:
		return "INFORMATIONAL", nil
	case 7:
		return "DEBUG", nil
	}

	return "", errors.New("Unknown Syslog level")

}

func (sr *SendRunner) FileGo(payload Payload) {
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

	file_path_interface := payload.Settings.Get("sends", "file", group, "file_path")
	translate_syslog_interface := payload.Settings.Get("sends", "file", group, "translate_syslog")
	include_timestamp_interface := payload.Settings.Get("sends", "file", group, "include_timestamp")

	var file_path string
	var translate_syslog bool
	var include_timestamp bool

	if translate_syslog_interface == nil {
		translate_syslog = true
	} else {
		translate_syslog = translate_syslog_interface.(bool)
	}
	if include_timestamp_interface == nil {
		include_timestamp = true
	} else {
		include_timestamp = include_timestamp_interface.(bool)
	}

	if file_path_interface == nil {
		file_path_interface2 := payload.Settings.Get("sends", "file", "default", "file_path")
		translate_syslog_interface2 := payload.Settings.Get("sends", "file", "default", "translate_syslog")
		include_timestamp_interface2 := payload.Settings.Get("sends", "file", "default", "include_timestamp")

		if translate_syslog_interface2 == nil {
			translate_syslog = true
		} else {
			translate_syslog = translate_syslog_interface2.(bool)
		}
		if include_timestamp_interface2 == nil {
			include_timestamp = true
		} else {
			include_timestamp = include_timestamp_interface2.(bool)
		}

		if file_path_interface2 == nil {
			file_path = "redshift.out"
		} else {
			file_path = file_path_interface2.(string)
		}

	} else {
		file_path = file_path_interface.(string)
	}

	if len(file_path) > 0 {
		f, err := os.OpenFile(file_path, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)

		defer f.Close()

		var ts_text string = ""
		var syslog_text string = ""

		if err != nil {
			fmt.Printf("[%s] ERROR Error opening file %s\n", ts, file_path)
		}

		if include_timestamp {
			ts_text = fmt.Sprintf("%s ", ts)
		}

		if translate_syslog {
			st, err := TranslateSyslog(payload.Message.Severity)
			if err != nil {
				st = "UNKNOWN"
			}
			syslog_text = fmt.Sprintf("%s ", st)
		} else {
			syslog_text = fmt.Sprintf("Severity: %d ", payload.Message.Severity)
		}

		json_b, errjsb := json.Marshal(payload.Message)
		if errjsb != nil {
			fmt.Printf("[%s] ERROR Error marshaling JSON payload\n", ts)
		}

		json_text := string(json_b)
		t := fmt.Sprintf("%s%s%s\n", ts_text, syslog_text, json_text)
		f.WriteString(t)

	} else {
		fmt.Printf("[%s] ERROR Filepath not found\n", ts)
	}
}
