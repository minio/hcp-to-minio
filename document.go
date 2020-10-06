package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"
)

const amzMetaPrefix = "x-amz-meta-"

// Document represents structure of annotation document in HCP
type Document struct {
	XMLName               xml.Name     `xml:"document"`
	Type                  string       `xml:"documenttype"`
	FileFormat            string       `xml:"documentfileformat,omitempty"`
	EncryptedAcctNum      string       `xml:"encryptedaccountnumber,omitempty"`
	ReportFamily          string       `xml:"reportfamily,omitempty"`
	Locale                string       `xml:"documentlocale,omitempty"`
	ReportPeriodStartDate DocumentDate `xml:"reportperiodstartdate"`
	ReportPeriodEndDate   DocumentDate `xml:"reportperiodenddate"`
	ReportRunDate         DocumentDate `xml:"reportrundate"`
	GenerationSchedule    string       `xml:"documentgenerationschedule"`
	ReportType            string       `xml:"reporttype,omitempty"`
	WindowName            string       `xml:"windowname,omitempty"`
	FileCount             int          `xml:"filecount"`
	TimeZone              string       `xml:"timezone,omitempty"`
	ReportFileName        string       `xml:"reportfilename"`
}

// DocumentDate is a embedded type containing time.Time to unmarshal
// Date in Document
type DocumentDate struct {
	time.Time
}

var errInvalidDocumentDate = fmt.Errorf("Invalid date format in document")

// UnmarshalXML parses date from Expiration and validates date format
func (dDate *DocumentDate) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	var dateStr string
	err := d.DecodeElement(&dateStr, &startElement)
	if err != nil {
		return err
	}
	docDate, err := time.Parse("2006-01-02T15:04:05", dateStr)
	if err != nil {
		return errInvalidDocumentDate
	}
	*dDate = DocumentDate{docDate}
	return nil
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (dDate DocumentDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if dDate.Time.IsZero() {
		return nil
	}
	return e.EncodeElement(dDate.Format("yyyy-MM-dd'T'HH:mm:ss"), startElement)
}

func getDocumentAnnotation(fileName string) (d *Document, err error) {
	var doc Document
	f, err := os.Open(fileName)
	if err != nil {
		return d, err
	}
	defer f.Close()
	documentXML, _ := ioutil.ReadAll(f)
	if err = xml.Unmarshal(documentXML, &doc); err != nil {
		return d, err
	}
	return &doc, nil
}

// getMinIOObjectName returns the object name derived from the Document annotation
//   /{enc_account}/report_type=STMT/subtype=MSR/YYYY/MM/DD/format=PDF/filename.ext

func (d *Document) getMinIOObjectName() string {
	return path.Join(d.EncryptedAcctNum, d.ReportType, d.Type, d.ReportRunDate.Format("2006/01/02"), d.FileFormat, d.ReportFileName)
}

func (d *Document) getObjectMetadata() map[string]string {
	m := make(map[string]string)
	m[amzMetaPrefix+"Report-Start-Date"] = d.ReportPeriodStartDate.Format("yyyy-MM-dd'T'HH:mm:ss")
	m[amzMetaPrefix+"Report-End-Date"] = d.ReportPeriodEndDate.Format("yyyy-MM-dd'T'HH:mm:ss")
	m[amzMetaPrefix+"Report-Run-Date"] = d.ReportRunDate.Format("yyyy-MM-dd'T'HH:mm:ss")
	m[amzMetaPrefix+"DocType"] = d.Type
	m[amzMetaPrefix+"Locale"] = d.Locale
	m[amzMetaPrefix+"ReportFamily"] = d.ReportFamily
	m[amzMetaPrefix+"ReportType"] = d.ReportType
	m[amzMetaPrefix+"ReportFileName"] = d.ReportFileName
	return m
}
