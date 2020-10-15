package main

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
)

type hcpBackend struct {
	// "https://namespace-name.tenant-name.hcp-domain-name/rest
	URL        string // namespace URL
	Insecure   bool
	client     *http.Client
	Username   string // optional - if auth token not provided
	Password   string // optional - if auth token not provided
	authToken  string
	hostHeader string
}

func (hcp *hcpBackend) Client() *http.Client {
	if hcp.client == nil {
		tr := &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: hcp.Insecure},
		}
		hcp.client = &http.Client{Transport: tr}

	}
	return hcp.client
}

const xHcpErrorMessage = "X-HCP-ErrorMessage"

func (hcp *hcpBackend) authenticationToken() string {
	if hcp.authToken != "" {
		return hcp.authToken
	}
	username := base64.StdEncoding.EncodeToString([]byte(hcp.Username))
	h := md5.New()
	io.WriteString(h, hcp.Password)
	password := fmt.Sprintf("%x", h.Sum(nil))
	return username + ":" + password
}
