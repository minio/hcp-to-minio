package main

import (
	"crypto/md5"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
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
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConnsPerHost:   256,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: mustGetSystemCertPool(),
				// Can't use SSLv3 because of POODLE and BEAST
				// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
				// Can't use TLSv1.1 because of RC4 cipher usage
				MinVersion:         tls.VersionTLS12,
				NextProtos:         []string{"http/1.1"},
				InsecureSkipVerify: hcp.Insecure,
			},
			// Set this value so that the underlying transport round-tripper
			// doesn't try to auto decode the body of objects with
			// content-encoding set to `gzip`.
			//
			// Refer:
			//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
			DisableCompression: true,
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
