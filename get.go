package main

import (
	"encoding/xml"
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

func (hcp *hcpBackend) GetObject(object, annotation string) (r io.ReadCloser, oi miniogo.ObjectInfo, h http.Header, err error) {
	u, err := url.Parse(namespaceURL)
	if err != nil {
		return r, oi, h, err
	}
	u.Path = object
	reqURL := u.String() // prints http://foo/bar.html

	data := url.Values{}
	data.Set("type", "whole-object")
	if annotation != "" {
		data.Set("annotation", annotation)
	}
	req, err := http.NewRequest(http.MethodGet, reqURL, nil)
	if err != nil {
		logDMsg(fmt.Sprintf("Couldn't create a request with namespaceURL %s", reqURL), err)
		return r, oi, h, err
	}
	req.Header.Set("Authorization", authToken)
	req.Host = hostHeader
	req.URL.RawQuery = data.Encode()
	// specify that annotation precede the object data
	req.Header["X-HCP-CustomMetadataFirst"] = []string{"true"}

	resp, err := hcp.Client().Do(req)
	if debugFlag {
		console.Println(trace(req, resp))
	}
	if err != nil {
		logDMsg(fmt.Sprintf("Get HCP object failed for %s", req.RequestURI), err)
		return r, oi, h, err
	}
	if resp.StatusCode != http.StatusOK {
		return r, oi, h, fmt.Errorf("Bad request")
	}

	var (
		totSz   int
		objSz   int
		annotSz int
		doc     Document
	)
	szStr := resp.Header.Get("Content-Length")
	if szStr == "" {
		szStr = resp.Header.Get("X-Hcp-Contentlength")
	}
	totSz, err = strconv.Atoi(szStr)
	if err != nil {
		return r, oi, h, fmt.Errorf("invalid content-length header %w", err)
	}
	if annotation != "" {
		objSizeStr := resp.Header.Get("X-Hcp-Size")
		objSz, err = strconv.Atoi(objSizeStr)
		if err != nil {
			return r, oi, h, fmt.Errorf("invalid X-HCP-Size header %w", err)
		}
		annotSz = totSz - objSz
	}
	reader := io.LimitReader(resp.Body, 1*1024*1024)
	minioObjName := strings.TrimPrefix(object, "/rest/") // default MinIO object name to same as HCP, unless annotation dictates otherwise
	metadata := make(map[string]string)
	if annotSz > 0 {
		annotationBuf := make([]byte, annotSz)
		_, err := reader.Read(annotationBuf)
		if err != nil {
			return r, oi, h, fmt.Errorf("could not read annotation into buffer %w", err)
		}
		if err = xml.Unmarshal(annotationBuf, &doc); err != nil {
			return r, oi, h, fmt.Errorf("Annotation could not be unmarshalled %w", err)
		}
		minioObjName = doc.getMinIOObjectName()
		metadata = doc.getObjectMetadata()
	}
	// Parse Last-Modified has http time format.
	date, err := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
	if err != nil {
		return r, oi, h, fmt.Errorf("invalid date format for Last-Modified header %w", err)
	}
	contentType := resp.Header.Get("X-Hcp-Custommetadatacontenttype")
	if contentType == "" {
		contentType = resp.Header.Get("Content-Type")
	}
	oi = miniogo.ObjectInfo{
		Key:          minioObjName,
		ETag:         resp.Header.Get("ETag"),
		UserMetadata: metadata,
		Size:         int64(objSz),
		LastModified: date,
		ContentType:  contentType,
		Metadata:     resp.Header,
	}
	return ioutil.NopCloser(reader), oi, h, nil
}
