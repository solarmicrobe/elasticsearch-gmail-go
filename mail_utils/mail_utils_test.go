package mail_utils

import (
	"github.com/google/go-cmp/cmp"
	. "net/mail"
	"testing"
)

func TestMailParseAddress(t *testing.T) {
	want := Address{Name: "\"iTunes Store", Address: "do_not_reply@itunes.com"}
	address, err := ParseAddress("\"\\\"iTunes Store\\\"\" <do_not_reply@itunes.com>")
	if err != nil {
		t.Fatalf("ParseAddress returned a nil error: %s", err)
	}
	if cmp.Equal(want, address) {
		t.Fatalf("want address %s, got %s", want, address)
	}
}

func TestErroredMailParseAddress(t *testing.T) {
	_, err := ParseAddress("\"\\\"iTunes Store\\\"\"")
	if err == nil {
		t.Fatalf("ParseAddress returned a nil error: %s", err)
	}
}

func TestParseEmailAddress(t *testing.T) {
	want := EmailAddress{"do_not_reply", "itunes.com"}
	address := Address{Name: "iTunes Store", Address: "do_not_reply@itunes.com"}
	email, err := ParseEmailAddress(&address)
	if err != nil {
		t.Errorf("Error parsing EmailAddrss: %s", err)
	}
	if !cmp.Equal(&want, email) {
		t.Fatalf("want email address %s but got %s", want, email)
	}
}

func TestErrorParseEmailAddress(t *testing.T) {
	_, err := ParseEmailAddress(&Address{Address: "fake"})
	if err == nil {
		t.Errorf("")
	}
}

func TestParseDomains(t *testing.T) {
	want := []string{"s", "apple", "com"}
	domains, _ := ParseDomains("s.apple.com")
	if !cmp.Equal(&want, domains) {
		t.Fatalf("want list %v got %v", want, domains)
	}
}
