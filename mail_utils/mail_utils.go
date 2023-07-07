package mail_utils

import (
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"net/mail"
	"strings"
)

type EmailAddress struct {
	Username string
	Domain   string
}

func GetEmailAddress(str string) (*EmailAddress, error) {
	log.Trace().Str("String", str).Msgf("GetEmailAddress")
	address, err := mail.ParseAddress(str)
	if err != nil {
		if (strings.Count(str, "\"")%2 == 1) && (string(str[0]) == "\"") {
			address, err = mail.ParseAddress(str[1:])
		} else if strings.Contains(str, "windows-1252") {
			strings.Split(str, "=?")
			address, err = mail.ParseAddress(strings.Replace(str, "?windows1252", "", -1))
		}

		if err != nil {
			log.Trace().Err(err).Msgf("Could not parse mail.Address from string '%s'", str)
			return nil, err
		}
	}
	log.Trace().Str("name", address.Name).Str("address", address.Address).Msg("")
	emailAddress, err := ParseEmailAddress(address)
	if err != nil {
		log.Trace().Err(err).Msg("Could not parse email address")
		return nil, err
	}
	log.Trace().Str("username", emailAddress.Domain).Str("domain", emailAddress.Domain).Msg("")

	return emailAddress, nil
}

/*
*
Parse an email address into it's username and domain parts
*/
func ParseEmailAddress(address *mail.Address) (*EmailAddress, error) {
	at := strings.LastIndex(address.Address, "@")
	if at >= 0 {
		username, domain := address.Address[:at], address.Address[at+1:]
		return &EmailAddress{Username: username, Domain: domain}, nil
	} else {
		return nil, errors.New(fmt.Sprintf("Error: '%s' is an invalid email address", address))
	}
}

/*
*
Parse domain string into component domains
*/
func ParseDomains(domain string) (*[]string, error) {
	domainParts := strings.Split(domain, ".")
	return &domainParts, nil
}
