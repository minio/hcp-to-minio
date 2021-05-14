package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strconv"
	"strings"
	"time"

	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio/pkg/console"
)

// closeWrapper converts a function to an io.Closer
type closeWrapper func() error

// Close calls the wrapped function.
func (c closeWrapper) Close() error {
	return c()
}
func closeResponse(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}
}

func (hcp *hcpBackend) GetObject(object string) (r io.ReadCloser, oi miniogo.ObjectInfo, err error) {
	u, err := url.Parse(namespaceURL)
	if err != nil {
		return r, oi, err
	}
	u.Path = object
	reqURL := u.String() // prints http://foo/bar.html

	data := url.Values{}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		logDMsg(fmt.Sprintf("Couldn't create a request with namespaceURL %s", reqURL), err)
		return r, oi, err
	}
	req.Header.Set("Authorization", authToken)
	req.Host = hostHeader
	req.URL.RawQuery = data.Encode()
	var start, connect, dns, tlsHandshake time.Time
	var dnsLatency, ttfb, connectLatency, handshakeLatency time.Duration
	trc := &httptrace.ClientTrace{
		DNSStart: func(dsi httptrace.DNSStartInfo) { dns = time.Now() },
		DNSDone: func(ddi httptrace.DNSDoneInfo) {
			dnsLatency = time.Since(dns)
		},

		TLSHandshakeStart: func() { tlsHandshake = time.Now() },
		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
			handshakeLatency = time.Since(tlsHandshake)
		},

		ConnectStart: func(network, addr string) { connect = time.Now() },
		ConnectDone: func(network, addr string, err error) {
			connectLatency = time.Since(connect)
		},

		GotFirstResponseByte: func() {
			ttfb = time.Since(start)
		},
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trc))
	start = time.Now()

	resp, err := hcp.Client().Do(req)
	if debugFlag {
		console.Println(trace(req, resp))
	}
	hcp.sumLatency.connectLatency.Add(connectLatency)
	hcp.sumLatency.dnsLatency.Add(dnsLatency)
	hcp.sumLatency.ttfb.Add(ttfb)
	hcp.sumLatency.handshakeLatency.Add(handshakeLatency)
	hcp.sumLatency.count.Inc()

	if err != nil {
		logDMsg(fmt.Sprintf("Get HCP object failed for %s", req.RequestURI), err)
		return r, oi, err
	}
	logMsg(fmt.Sprintf("HCP DNS Done: %s TLS Handshake: %s Connect time: %s TTFB: %s req# %d", dnsLatency, handshakeLatency, connectLatency, ttfb, hcp.sumLatency.count.Load()))

	if resp.StatusCode != http.StatusOK {
		closeResponse(resp)
		return r, oi, fmt.Errorf("bad request Status:%d", resp.StatusCode)
	}

	var (
		objSz int
	)
	objSz, err = strconv.Atoi(resp.Header.Get("X-Hcp-Size"))
	if err != nil {
		closeResponse(resp)
		return r, oi, fmt.Errorf("invalid X-HCP-Size header %w", err)
	}
	minioObjName := strings.TrimPrefix(object, "/rest/") // default MinIO object name to same as HCP
	date, err := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
	if err != nil {
		closeResponse(resp)
		return r, oi, fmt.Errorf("invalid date format for Last-Modified header %w", err)
	}

	return resp.Body, miniogo.ObjectInfo{
		Key:          minioObjName,
		Size:         int64(objSz),
		LastModified: date,
	}, nil
}
