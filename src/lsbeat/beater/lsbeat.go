package beater

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"lsbeat/config"
)

type Lsbeat struct {
	done          chan struct{}
	config        config.Config
	client        beat.Client
	lastIndexTime time.Time
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Lsbeat{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

func (bt *Lsbeat) Run(b *beat.Beat) error {
	logp.Info("lsbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	ticker := time.NewTicker(bt.config.Period)
	for {
		now := time.Now()
		bt.listDir(bt.config.Path, b.Info.Name)
		bt.lastIndexTime = now
		logp.Info("Event sent")
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
	}
	// counter := 1
	// for {
	// 	select {
	// 	case <-bt.done:
	// 		return nil
	// 	case <-ticker.C:
	// 	}

	// 	event := beat.Event{
	// 		Timestamp: time.Now(),
	// 		Fields: common.MapStr{
	// 			"type":    b.Info.Name,
	// 			"counter": counter,
	// 		},
	// 	}
	// 	bt.client.Publish(event)
	// 	logp.Info("Event sent")
	// 	counter++
	// }
}

func (bt *Lsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}

func (bt *Lsbeat) listDir(dirFile string, beatname string) {
	files, _ := ioutil.ReadDir(dirFile)
	for _, f := range files {
		t := f.ModTime()
		path := filepath.Join(dirFile, f.Name())
		if t.After(bt.lastIndexTime) {
			// event := common.MapStr{
			// 	"@timestamp": common.Time(time.Now()),
			// 	"type":       beatname,
			// 	"modtime":    common.Time(t),
			// 	"filename":   f.Name(),
			// 	"path":       path,
			// 	"directory":  f.IsDir(),
			// 	"filesize":   f.Size(),
			// }
			event := beat.Event{
				Timestamp: time.Now(),
				Fields: common.MapStr{
					"modtime":   common.Time(t),
					"filename":  f.Name(),
					"path":      path,
					"directory": f.IsDir(),
					"filesize":  f.Size(),
				},
				Meta: common.MapStr{
					"id": common.Time(t),
				},
				Private: nil,
			}
			event.SetID(f.Name())
			bt.client.Publish(event)
		}
		if f.IsDir() {
			bt.listDir(path, beatname)
		}
	}
}
