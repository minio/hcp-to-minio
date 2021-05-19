package main

import (
	"bufio"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"sync"
	"time"

	"github.com/minio/minio/pkg/console"
)

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
	objectPath                string   // relative url holding path of this object
}

// Job for worker
type listWorkerJob struct {
	Root string
}

func getFileName(fname, prefix string) string {
	if prefix == "" {
		return fmt.Sprintf("%s%s", fname, time.Now().Format(".01-02-2006-15-04-05"))
	}
	return fmt.Sprintf("%s_%s%s", fname, prefix, time.Now().Format(".01-02-2006-15-04-05"))
}
func (hcp *hcpBackend) downloadObjectList(ctx context.Context, prefix string) error {
	f, err := os.OpenFile(path.Join(dirPath, getFileName(objListFile, prefix)), os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_SYNC, 0600)
	if err != nil {
		return err
	}
	datawriter := bufio.NewWriter(f)
	workerCount := 1

	jobs := make(chan listWorkerJob, workerCount)
	entryCh := make(chan Entry, 1000)
	readDone := make(chan bool)
	wg := &sync.WaitGroup{}
	// start N workers

	for i := 0; i < workerCount; i++ {
		go hcp.List(ctx, jobs, entryCh, wg)
	}

	// One initial job
	wg.Add(1)
	go func() {
		jobs <- listWorkerJob{
			Root: "",
		}
	}()

	// When all jobs finished, shutdown the system.
	go func() {
		wg.Wait()
		readDone <- true
	}()
readloop:
	for {
		select {
		case entry, ok := <-entryCh:
			if !ok {
				break readloop
			}
			if entry.EntryType != "object" {
				logDMsg("received non object entry in channel>"+entry.objectPath, nil)
				continue
			}
			if _, err := datawriter.WriteString(entry.objectPath + "\n"); err != nil {
				return err
			}
		case <-readDone:
			log.Printf(`got stop`)
			close(jobs)
			close(entryCh)
		}
	}
	datawriter.Flush()
	f.Close()
	return nil
}

func (hcp *hcpBackend) List(ctx context.Context, jobs chan listWorkerJob, entryCh chan Entry, wg *sync.WaitGroup) {
	for j := range jobs {
		u, err := url.Parse(hcp.URL)
		if err != nil {
			logDMsg(fmt.Sprintf("Couldn't create a request with hcp.URL %s", hcp.URL), err)
			continue
		}
		if j.Root != "" {
			u.Path = j.Root
		}
		logDMsg(fmt.Sprintf(`Directory: %#v`, u.Path), nil)
		req, err := http.NewRequest(http.MethodGet, u.String(), nil)
		if err != nil {
			logDMsg(fmt.Sprintf("Couldn't create a request with hcp.URL %s", hcp.URL), err)
			continue
		}
		req.Header.Set("Authorization", authToken)
		req.Host = hostHeader
		resp, err := hcp.Client().Do(req)
		// logDMsg("REQUEST:>"+req.URL.String(), nil)
		// if resp != nil {
		// 	logDMsg("Resp statuscode =>"+strconv.Itoa(resp.StatusCode), nil)
		// }
		if debugFlag {
			console.Println(trace(req, resp))
		}
		if err != nil {
			logDMsg(fmt.Sprintf("Couldn't list namespace directory contents with namespace URL %s", hcp.URL), err)
			continue
		}
		decoder := xml.NewDecoder(resp.Body)
		for {
			// Read tokens from the XML document in a stream.
			t, err := decoder.Token()
			// If we are at the end of the file, we are done
			if err == io.EOF {
				//	logDMsg("breaking Dir XML parsing because EOF", nil)
				resp.Body.Close()
				io.Copy(ioutil.Discard, resp.Body)
				break
			} else if err != nil {
				log.Fatalf("Error decoding token: %s", err)
			} else if t == nil {
				resp.Body.Close()
				io.Copy(ioutil.Discard, resp.Body)
				break
			}

			switch se := t.(type) {
			case xml.StartElement:
				switch se.Name.Local {
				// Found a dir, so we process it
				case "directory":
					var dir Directory

					// We decode the element into our data model...
					if err = decoder.DecodeElement(&dir, &se); err != nil {
						log.Fatalf("Error decoding item: %s", err)
					}
					for _, entry := range dir.Entries {
						entry.objectPath = path.Join(dir.Path, entry.URLName)
						logDMsg("read entry>"+entry.URLName+" at path>"+entry.objectPath, nil)

						switch entry.EntryType {
						case "object":
							entryCh <- entry
						case "directory":
							// Send directory to be processed by the worker
							nj := listWorkerJob{
								Root: entry.objectPath,
							}
							// logDMsg("sent new dir job: "+nj.Root, nil)

							// One more job, adds to wg
							wg.Add(1)

							// Do not block when sending jobs
							go func() {
								jobs <- nj
							}()
						default:
							logDMsg("Dropped Entry>>>"+entry.objectPath, nil)
						}
					}
					// // And use it for whatever we want to
					// log.Printf("'%s' : %v", dir.namespaceName, dir.Entries)

				default:
				}
			}
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
		// Done one job, let wg know.
		wg.Done()
	}
}
