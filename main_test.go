package main

import (
	"github.com/gookit/config/v2"
	"net/mail"
	"os"
	"sync"
	"testing"
)

func fakeWriter(t *testing.T, ch <-chan []*mail.Message, wg *sync.WaitGroup) {
	defer wg.Done()
	for messages := range ch {
		t.Logf("Processing %d messages", len(messages))
	}
}

func setup() (chan []*mail.Message, *sync.WaitGroup) {
	config.WithOptions(config.ParseEnv)
	return make(chan []*mail.Message), &sync.WaitGroup{}
}

func TestReadMbox(t *testing.T) {
	reader, err := os.Open("/Users/parksrussell/Downloads/Takeout/Mail/Inbox.mbox")
	if err != nil {
		panic(err)
	}
	ch, wg := setup()
	wg.Add(1)

	for idx := 0; idx < 10; idx++ {
		wg.Add(1)
		go fakeWriter(t, ch, wg)
	}
	readMbox(reader, ch, wg)
	t.Logf("Closing message channel")
	close(ch)
	t.Logf("Waiting for goroutines to finish")
	wg.Wait()
	t.Logf("Processed %d messages, exiting...", processed)
}
