package sends

import (
	"errors"
	"fmt"
	"github.com/nlopes/slack"
	"os"
	"strings"
	"time"
)

func get_channel(rtm *slack.RTM, lookfor string) (string, error) {
	has_hash := strings.HasPrefix(strings.Trim(lookfor, " "), "#")
	if has_hash {
		lookfor = strings.TrimPrefix(strings.Trim(lookfor, " "), "#")
	}
	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")

	// Try public channels first
	l, err := rtm.GetChannels(false)
	if err != nil {
		fmt.Printf("[%s] ERROR Channel list error: %s\n", ts, err)
	}
	for _, v := range l {
		if v.Name == lookfor {
			return v.ID, nil
		}
	}

	// Now private channels, or groups in slack terms
	g, err := rtm.GetGroups(false)
	if err != nil {
		fmt.Printf("[%s] ERROR Channel list error: %s\n", ts, err)
	}
	for _, v := range g {
		if v.Name == lookfor {
			return v.ID, nil
		}
	}

	return "", errors.New("No channel found with this name")
}

func post(rtm *slack.RTM, channel string, message Message, debug bool) {
	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
	params := slack.PostMessageParameters{
		Username: "redshift",
	}
	attachment := slack.Attachment{
		Pretext: message.Subject,
		Text:    message.Detail,
	}
	params.Attachments = []slack.Attachment{attachment}

	title := fmt.Sprintf("Alert *%s* with Severity %d (Magnitude: %d)", message.Subject, message.Severity, message.Magnitude)

	channelID, timestamp, err := rtm.PostMessage(channel, title, params)
	if err != nil {
		fmt.Printf("[%s] ERROR Error received: %s\n", ts, err)
		return
	}
	if debug {
		fmt.Printf("[%s] INFO Message %+v successfully sent to channel %s at %s", ts, message, channelID, timestamp)
	}
}

func talk(rtm *slack.RTM, channel string, debug bool) {
	t := time.Now()
	ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
Loop:
	for {
		t = time.Now()
		ts = t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")
		select {
		case msg := <-rtm.IncomingEvents:
			switch ev := msg.Data.(type) {
			case *slack.HelloEvent:
				if debug {
					fmt.Printf("[%s] INFO Received hello: %v\n", ts, ev)
				}

			case *slack.ConnectedEvent:
				if debug {
					fmt.Printf("[%s] INFO Event information: %s\n", ts, ev.Info)
				}
				rtm.SendMessage(rtm.NewOutgoingMessage("Hello, Dave", channel))

			case *slack.MessageEvent:
				if debug {
					fmt.Printf("[%s] INFO Message event information: %v\n", ts, ev)
				}

			case *slack.PresenceChangeEvent:
				if debug {
					fmt.Printf("[%s] INFO Presence change: %v\n", ts, ev)
				}

			case *slack.LatencyReport:
				if debug {
					fmt.Printf("[%s] INFO Current latency: %v\n", ts, ev.Value)
				}

			case *slack.RTMError:
				fmt.Printf("[%s] ERROR Slack RTM Error: %s\n", ts, ev.Error())

			case *slack.InvalidAuthEvent:
				fmt.Printf("[%s] ERROR Invalid credentials\n", ts)
				break Loop

			default:
				if debug {
					fmt.Printf("[%s] INFO Unexpected event: %v\n", ts, msg.Data)
				}
			}
		}
	}
}

func (sr *SendRunner) SlackGo(payload Payload) {

	group := payload.Message.Group
	settings := payload.Settings
	debug := settings.Get("debug").(bool)

	channel_name_interface := settings.Get("sends", "slack", group, "slack_channel")
	token_interface := settings.Get("sends", "slack", group, "slack_token")

	var channel_name string
	var token string

	if channel_name_interface == nil || token_interface == nil {
		channel_name_interface2 := settings.Get("sends", "slack", "default", "slack_channel")
		token_interface2 := settings.Get("sends", "slack", "default", "slack_token")
		if channel_name_interface2 == nil || token_interface2 == nil {
			channel_name = ""
			token = ""
		} else {
			channel_name = channel_name_interface2.(string)
			token = token_interface2.(string)
		}
	} else {
		channel_name = channel_name_interface.(string)
		token = token_interface.(string)
	}

	if len(channel_name) > 0 && len(token) > 0 {
		api := slack.New(token)
		if debug {
			api.SetDebug(true)
		}

		rtm := api.NewRTM()
		go rtm.ManageConnection()

		t := time.Now()
		ts := t.Format("Mon Jan 2 15:04:05 -0700 MST 2006")

		channel, errc := get_channel(rtm, channel_name)
		if errc != nil {
			fmt.Printf("[%s] ERROR %s\n", ts, errc.Error())
			os.Exit(1)
		}

		if len(token) == 0 || len(channel) == 0 {
			fmt.Printf("[%s] ERROR Slack Token or Slack Channel not specified\n", ts)
			os.Exit(1)
		} else {
			if debug {
				fmt.Printf("[%s] INFO Joining Slack channel #%s with token %s\n", ts, channel_name, token)
			}
		}

		post(rtm, channel, payload.Message, debug)
	}

	// Coming soon!
	//go post(rtm, channel, payload.Message, debug)
	//talk(rtm, channel, debug)

}
