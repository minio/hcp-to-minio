package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
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
func (hcp *hcpBackend) GetObject(object string) (r io.ReadCloser, oi miniogo.ObjectInfo, h http.Header, err error) {
	u, err := url.Parse(namespaceURL)
	if err != nil {
		return r, oi, h, err
	}
	u.Path = object
	reqURL := u.String() // prints http://foo/bar.html

	data := url.Values{}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		logDMsg(fmt.Sprintf("Couldn't create a request with namespaceURL %s", reqURL), err)
		return r, oi, h, err
	}
	req.Header.Set("Authorization", authToken)
	req.Header.Set("Expect", "100-continue")
	req.Host = hostHeader
	req.URL.RawQuery = data.Encode()

	resp, err := hcp.Client().Do(req)
	if debugFlag {
		console.Println(trace(req, resp))
	}
	if err != nil {
		logDMsg(fmt.Sprintf("Get HCP object failed for %s", req.RequestURI), err)
		return r, oi, h, err
	}
	if resp.StatusCode != http.StatusOK {
		closeResponse(resp)
		return r, oi, h, fmt.Errorf("bad request Status:%d", resp.StatusCode)
	}

	var (
		objSz int
	)
	objSz, err = strconv.Atoi(resp.Header.Get("X-Hcp-Size"))
	if err != nil {
		closeResponse(resp)
		return r, oi, h, fmt.Errorf("invalid X-HCP-Size header %w", err)
	}
	minioObjName := strings.TrimPrefix(object, "/rest/") // default MinIO object name to same as HCP
	metadata := make(map[string]string)
	date, err := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
	if err != nil {
		closeResponse(resp)
		return r, oi, h, fmt.Errorf("invalid date format for Last-Modified header %w", err)
	}
	contentType := resp.Header.Get("X-Hcp-Custommetadatacontenttype")
	if contentType == "" {
		contentType = resp.Header.Get("Content-Type")
	}
	etag := strings.TrimPrefix(resp.Header.Get("ETag"), "\"")
	etag = strings.TrimSuffix(etag, "\"")

	oi = miniogo.ObjectInfo{
		Key:          minioObjName,
		ETag:         etag,
		UserMetadata: metadata,
		Size:         int64(objSz),
		LastModified: date,
		ContentType:  contentType,
		Metadata:     resp.Header,
	}

	return resp.Body, oi, h, nil
}
