package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path"
	"strconv"
)

const maxSize = 2 << 20 // 2 * MiB

// Directory represents directory
type Directory struct {
	XMLNSXSI string `xml:"xmlns xsi,attr,omitempty"`
	//	XsiName  string `xml:"http://www.w3.org/2001/XMLSchema-instance name,attr,omitempty"`
	XMLName                string  `xml:"directory"`
	Path                   string  `xml:"path,attr,omitempty"`
	UTF8Path               string  `xml:"utf8Path,attr,omitempty"`
	ParentDir              string  `xml:"parentDir,attr,omitempty"`
	utf8ParentDir          string  `xml:"utf8ParentDir,attr,omitempty"`
	dirDeleted             bool    `xml:"dirDeleted,attr"`
	showDeleted            bool    `xml:"showDeleted,attr"`
	namespaceName          string  `xml:"namespaceName,attr,omitempty"`
	utf8NamespaceName      string  `xml:"utf8NamespaceName,attr,omitempty"`
	Entries                []Entry `xml:"entry"`
	changeTimeMilliseconds int64   `xml:""changeTimeMilliseconds,omitempty"`
	changeTimeString       string  `xml:"changeTimeString,omitempty"`
}

// Entry represents a object/sub dir/symlink
type Entry struct {
	XMLName                   xml.Name `xml:"entry"`
	URLName                   string   `xml:"urlName,attr"`
	Utf8Name                  string   `xml:"utf8Name,attr"`
	EntryType                 string   `xml:"type,attr"`
	Size                      int64    `xml:"size,attr,omitempty"`
	HashScheme                string   `xml:"hashScheme,attr,omitempty"`
	Hash                      string   `xml:"hash,attr,omitempty"`
	retentionString           string   `xml:"retentionString,attr,omitempty"`
	retentionClass            string   `xml:"retentionClass,attr,omitempty"`
	ingestTimeString          string   `xml:"ingestTimeString,attr,omitempty"`
	hold                      bool     `xml:"hold,attr"`
	shred                     bool     `xml:"shred,attr"`
	dpl                       string   `xml:"dpl,attr,omitempty"`
	customMetadata            bool     `xml:"customMetadata,attr"`
	customMetadataAnnotations string   `xml:"customMetadataAnnotations,attr,omitempty"`
	version                   string   `xml:"version,attr,omitempty"`
	replicated                bool     `xml:"replicated,attr"`
	changeTimeString          string   `xml:"changeTimeString,attr,omitempty"`
	domain                    string   `xml:"domain,attr,omitempty"`
	hasACL                    bool     `xml:"hasAcl,attr"`
	state                     string   `xml:"state,attr,omitempty"`
}

func downloadObjectList(ctx context.Context, bucket string) error {
	f, err := os.OpenFile(path.Join(dirPath, objListFile), os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	entryCh := make(chan Entry, 1000)
	go func(w io.WriteCloser) {
		for entry := range entryCh {
			logDMsg("received entry in channel>"+entry.URLName, nil)
			if _, err := w.Write([]byte(entry.URLName + "\n")); err != nil {
				return
			}
		}
		defer w.Close()
		return
	}(f)
	if err := hcp.List(ctx, bucket, entryCh); err != nil {
		return err
	}
	return nil
}

func (hcp *hcpBackend) List(ctx context.Context, bucket string, entryCh chan Entry) error {
	//data := url.Values{}
	//	data.Set("type", "directory")
	//	data.Set("deleted", "false")

	req, err := http.NewRequest(http.MethodGet, hcp.URL, nil)

	if err != nil {
		logDMsg(fmt.Sprintf("Couldn't create a request with namespaceURL %s", namespaceURL), err)
		return err
	}
	req.Header.Set("Authorization", authToken)
	req.Host = hostHeader

	resp, err := hcp.Client().Do(req)

	if err != nil {
		logDMsg("REQUEST URL>"+req.URL.String(), nil)
		if resp != nil {
			logDMsg("Resp statuscode =>"+strconv.Itoa(resp.StatusCode), nil)
		}
		logDMsg(fmt.Sprintf("Couldn't list namespace directory contents with namespace URL %s", namespaceURL), err)
		return err
	}
	reader := io.LimitReader(resp.Body, maxSize)
	if err := getDirListing2(ctx, reader, entryCh); err != nil {
		return err
	}
	return nil
}

func getDirListing2(ctx context.Context, r io.Reader, entryCh chan Entry) (err error) {
	decoder := xml.NewDecoder(r)
	defer close(entryCh)
	for {
		// Read tokens from the XML document in a stream.
		t, err := decoder.Token()
		// If we are at the end of the file, we are done
		if err == io.EOF {
			logDMsg("breaking Dir XML parsing because EOF", nil)
			break
		} else if err != nil {
			log.Fatalf("Error decoding token: %s", err)
		} else if t == nil {
			break
		}

		switch se := t.(type) {
		case xml.StartElement:
			switch se.Name.Local {
			case "entry":
				var entry Entry

				// We decode the element into entry...
				if err = decoder.DecodeElement(&entry, &se); err != nil {
					log.Fatalf("Error decoding item: %s", err)
				}
				entryCh <- entry

			// Found a dir, so we process it
			/*			case "directory":
						var dir Directory

						// We decode the element into our data model...
						if err = decoder.DecodeElement(&dir, &se); err != nil {
							log.Fatalf("Error decoding item: %s", err)
						}
						for _, entry := range dir.Entries {
							fmt.Println(dir.Entries)
							entryCh <- entry
						}
						// // And use it for whatever we want to
						log.Printf("'%s' : %v", dir.namespaceName, dir.Entries)
			*/
			// case "entry":
			// 	fmt.Println("saw entry...")
			// 	var entry Entry
			// 	if err = decoder.DecodeElement(&entry, &se); err != nil {
			// 		log.Fatalf("Error decoding entry: %s", err)
			// 	}
			default:
			}
		}
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	return nil
}

func printDirListing(ctx context.Context, fileName string) (err error) {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer f.Close()
	entryCh := make(chan Entry, 1000)
	if err := getDirListing2(ctx, f, entryCh); err != nil {
		return err
	}
	for entry := range entryCh {
		fmt.Println(entry.EntryType, " :", entry.URLName)
	}
	return nil
}
