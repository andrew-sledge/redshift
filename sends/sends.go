package sends

import (
	"menteslibres.net/gosexy/yaml"
	"os"
)

type SendRunner struct{}

type Message struct {
	Timestamp string
	Group     string
	Source    string
	Subject   string
	Detail    string
	Magnitude int
	Severity  int
	Extra     string
}

type Payload struct {
	ProcessTime string
	Message     Message
	Settings    yaml.Yaml
}

func CheckError(e error, halt bool) {
	// Just system errors
	if e != nil {
		panic(e)
		if halt {
			os.Exit(1)
		}
	}
}
