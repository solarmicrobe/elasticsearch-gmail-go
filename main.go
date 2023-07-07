package main

import (
	"bytes"
	"context"
	"elasticsearch-gmail-go/mail_utils"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/emersion/go-mbox"
	"github.com/gookit/config/v2"
	"github.com/gookit/config/v2/toml"
	"github.com/gookit/config/v2/yaml"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"io"
	"net/mail"
	"os"
	"strings"
	"sync"
	"time"
)

// Config Keys
var BatchSize = "batch"
var ThreadSize = "threads"
var MboxPath = "mbox"
var processed = 0
var Index = "index"
var Init = "init"
var Shards = "shards"

// Defaults
var IndexName = "email"
var NumOfShards = 2

type IndexRequest struct {
	Settings struct {
		NumberOfShards   int `json:"number_of_shards"`
		NumberOfReplicas int `json:"number_of_replicas"`
	} `json:"settings"`
	Mappings struct {
		Email struct {
			Source struct {
				Enabled interface{} `json:"enabled"`
			} `json:"_source"`
			Properties struct {
				From struct {
					Type  string `json:"type"`
					Index string `json:"index"`
				} `json:"from"`
				ReturnPath struct {
					Type  string `json:"type"`
					Index string `json:"index"`
				} `json:"return-path"`
				DeliveredTo struct {
					Type  string `json:"type"`
					Index string `json:"index"`
				} `json:"delivered-to"`
				MessageId struct {
					Type  string `json:"type"`
					Index string `json:"index"`
				} `json:"message-id"`
				To struct {
					Type  string `json:"type"`
					Index string `json:"index"`
				} `json:"to"`
				DateTs struct {
					Type string `json:"type"`
				} `json:"date_ts"`
			} `json:"properties"`
		} `json:"email"`
	} `json:"mappings"`
	Refresh interface{} `json:"refresh"`
}

type Email struct {
	From              string    `json:"from"`
	ReturnPath        string    `json:"return-path"`
	DeliveredTo       string    `json:"delivered-to"`
	MessageId         string    `json:"message-id"`
	To                string    `json:"to"`
	Domain            string    `json:"domain"`
	SecondLevelDomain string    `json:"second-level-domain"`
	DateTs            time.Time `json:"date_ts"`
}

func DeleteIndex(es *elasticsearch.Client) {
	log.Info().Msgf("Deleting index %s...", IndexName)
	response, err := es.Indices.Delete([]string{IndexName}, es.Indices.Delete.WithIgnoreUnavailable(true))
	if err != nil {
		log.Err(err).Msgf("Can not delete index %s", IndexName)
	} else {
		log.Trace().Msg(response.String())
	}
	log.Info().Msg("... done")
}

func CreateIndex(es *elasticsearch.Client) {
	x := fmt.Sprintf(`{
        "settings": {
            "number_of_shards": %d,
            "number_of_replicas": 0
        },
        "mappings": {
            "email": {
                "_source": {"enabled": True},
                "properties": {
                    "from": {"type": "string", "index": "not_analyzed"},
                    "return-path": {"type": "string", "index": "not_analyzed"},
                    "delivered-to": {"type": "string", "index": "not_analyzed"},
                    "message-id": {"type": "string", "index": "not_analyzed"},
                    "to": {"type": "string", "index": "not_analyzed"},
                    "domain": {"type": "string", "index": "not_analyzed"},
                    "second-level-domain": {"type": "string", "index": "not_analyzed"},
                    "date_ts": {"type": "date"},
                },
            }
        },
        "refresh": True
    }`, config.Int(Shards, 2))

	reader := strings.NewReader(x)
	create, err := es.Indices.Create(IndexName, es.Indices.Create.WithBody(reader))
	if err != nil {
		log.Panic().Err(err).Msg("Could not create index")
	}
	log.Trace().Msgf(create.String())
}

func readMbox(reader io.Reader, ch chan<- []*mail.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	mr := mbox.NewReader(reader)

	batchSize := config.Int(BatchSize)
	var done = false
	for !done {
		var messages = make([]*mail.Message, 0, batchSize)

		for idx := 0; idx < config.Int(BatchSize, 10); idx++ {
			messageReader, err := mr.NextMessage()
			if err != nil {
				if err == io.EOF {
					log.Info().Msg("Reached end of mbox file")
					done = true
					break
				}
				log.Err(err).Msgf("Could not retrieve next message (%d) from mbox", processed+idx)
			}
			message, err := mail.ReadMessage(messageReader)
			if err != nil {
				log.Err(err).Msgf("Can not read message")
			}
			messages = append(messages, message)
		}
		if len(messages) > 0 {
			log.Trace().Msgf("Sending [%d]mail.Message(%p) group to channel", len(messages), &messages)
			ch <- messages
			processed += len(messages)
		}
	}
	log.Debug().Msgf("Processed %d from %s", processed, "file")
}

func bulkIndex(es *elasticsearch.Client, ch <-chan []*mail.Message, wg *sync.WaitGroup) {
	defer wg.Done()

	indexer, _ := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{Client: es})

	for messages := range ch {
		log.Trace().Msgf("Received %d message(s) from channel", len(messages))
		for _, message := range messages {
			messageId := message.Header.Get("message-id")
			fromString := message.Header.Get("from")
			from, err := mail_utils.GetEmailAddress(fromString)
			if err != nil {
				log.Err(err).Str("from", fromString).Msg("Could not get email address")
				break
			}
			domainParts, err := mail_utils.ParseDomains(from.Domain)
			if err != nil {
				log.Err(err).Str("domain", from.Domain).Msgf("Could not parse domains")
				break
			}
			date, err := message.Header.Date()
			if err != nil {
				log.Err(err).Str("date", message.Header.Get("date")).Msg("Gould not parse date")
				break
			}

			byts, err := json.Marshal(Email{
				From:              strings.Join([]string{from.Username, from.Domain}, "@"),
				ReturnPath:        message.Header.Get("return-path"),
				DeliveredTo:       message.Header.Get("delivered-to"),
				MessageId:         messageId,
				To:                message.Header.Get("to"),
				Domain:            from.Domain,
				SecondLevelDomain: strings.Join((*domainParts)[len(*domainParts)-2:len(*domainParts)], "."),
				DateTs:            date,
			})
			if err != nil {
				log.Err(err).Msg("Could not marshal json")
			}
			err = indexer.Add(
				context.Background(),
				esutil.BulkIndexerItem{
					Action: "index",
					Index:  IndexName,
					Body:   bytes.NewReader(byts),
				})
			if err != nil {
				log.Err(err).Str("message-id", messageId).Msg("")
			}
		}
	}
	log.Debug().Interface("Stats", indexer.Stats()).Msgf("")
	indexer.Close(context.Background())
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	// Config processing
	log.Debug().Msg("Parsing environment")
	config.WithOptions(config.ParseEnv)

	// load flag info
	log.Debug().Msgf("Loading flags")
	keys := []string{"config", Init + ":bool", MboxPath, ThreadSize, BatchSize}
	err := config.LoadFlags(keys)
	if err != nil {
		log.Panic().Err(err).Msg("Error loading flags")
	}

	filePath := config.String("config")
	if filePath == "" {
		filePath = "config.yaml"
	}

	config.AddDriver(yaml.Driver)
	config.AddDriver(toml.Driver)

	err = config.LoadFiles(filePath)
	if err != nil {
		log.Err(err).Msg("Error loading files")
	}

	IndexName = config.String(Index, IndexName)
	NumOfShards = config.Int(Shards, NumOfShards)

	// Set up ES
	es, _ := elasticsearch.NewDefaultClient()

	// Init index
	if config.Bool(Init, false) {
		DeleteIndex(es)
	}
	CreateIndex(es)

	// Parallel writer setup
	var wg sync.WaitGroup
	messageChan := make(chan []*mail.Message)

	for idx := 0; idx < config.Int(ThreadSize, 1); idx++ {
		wg.Add(1)
		go bulkIndex(es, messageChan, &wg)
	}

	// Reader setup
	reader, err := os.Open(config.String(MboxPath))
	if err != nil {
		log.Panic().Err(err).Msgf("Error opening file '%s'", config.String(MboxPath))
	}

	wg.Add(1)
	readMbox(reader, messageChan, &wg)

	log.Info().Msg("Closing message channel")
	close(messageChan)
	log.Info().Msg("Waiting for goroutines to finish")
	wg.Wait()
	log.Info().Msgf("Processed %d messages, exiting...", processed)
}
