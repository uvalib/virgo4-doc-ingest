package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/antchfx/xmlquery"
)

// ErrMissingRecordId - got a record without an identifier
var ErrMissingRecordId = fmt.Errorf("missing record identifier")

// ErrBlankRecordId - got a blank identifier
var ErrBlankRecordId = fmt.Errorf("blank/empty record identifier")

// ErrFileNotOpen - file is not open
var ErrFileNotOpen = fmt.Errorf("file is not open")

// RecordLoader - the interface
type RecordLoader interface {
	Validate() error
	First() (Record, error)
	Next() (Record, error)
	Done()
}

// Record - the record interface
type Record interface {
	Id() string
	Raw() []byte
}

// this is our loader implementation
type recordLoaderImpl struct {
	File    *os.File
	Decoder *xml.Decoder
}

// this is our record implementation
type recordImpl struct {
	RawBytes []byte
	RecordId string
}

// how we extract raw XML from the decoder
type innerXml struct {
	Xml string `xml:",innerxml"`
}

// NewRecordLoader - the factory
func NewRecordLoader(filename string) (RecordLoader, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &recordLoaderImpl{File: file}, nil
}

// read all the records to ensure the file is valid
func (l *recordLoaderImpl) Validate() error {

	if l.File == nil {
		return ErrFileNotOpen
	}

	// get the first record and error out if bad. An EOF is OK, just means the file is empty
	_, err := l.First()
	if err != nil {
		// are we done
		if err == io.EOF {
			log.Printf("WARNING: EOF on first read, looks like an empty file")
			return nil
		} else {
			log.Printf("ERROR: validation failure on record index 0 (%s)", err.Error())
			return err
		}
	}

	// used for reporting
	recordIndex := 1

	// read all the records and bail on the first failure except EOF
	for {
		_, err = l.Next()

		if err != nil {
			// are we done
			if err == io.EOF {
				break
			} else {
				log.Printf("ERROR: validation failure on record index %d (%s)", recordIndex, err.Error())
				return err
			}
		}
		recordIndex++
	}

	// everything is OK
	return nil
}

func (l *recordLoaderImpl) First() (Record, error) {

	if l.File == nil {
		return nil, ErrFileNotOpen
	}

	// go to the start of the file and then get the next record
	_, err := l.File.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	// wrap a new decoder around the file
	l.Decoder = xml.NewDecoder(l.File)
	return l.Next()
}

func (l *recordLoaderImpl) Next() (Record, error) {

	if l.File == nil {
		return nil, ErrFileNotOpen
	}

	rec, err := l.recordRead()
	if err != nil {
		return nil, err
	}

	return rec, nil
}

func (l *recordLoaderImpl) Done() {

	if l.File != nil {
		l.File.Close()
		l.File = nil
	}
}

func (l *recordLoaderImpl) recordRead() (Record, error) {

	rawXml, err := l.xmlRead()
	if err != nil {
		return nil, err
	}

	//fmt.Printf( "%s", rawXml )

	// attempt to extract the ID from the XML payload
	id, err := l.extractId(rawXml)
	if err != nil {
		return nil, err
	}

	return &recordImpl{RecordId: id, RawBytes: []byte(rawXml)}, nil
}

func (l *recordLoaderImpl) xmlRead() (string, error) {

	for {
		// get the next token
		t, err := l.Decoder.Token()

		if err != nil {
			return "", err
		}

		// check out the type of token, we just want start elements
		switch se := t.(type) {
		case xml.StartElement:

			nodeName := se.Name.Local

			// basically ignore <add> tags
			if nodeName == "add" {
				continue
			}

			// these are the ones we are interested in
			if nodeName == "doc" {

				// extract the inner XML from the doc element
				var inner innerXml
				err = l.Decoder.DecodeElement(&inner, &se)
				if err != nil {
					return "", err
				}

				return fmt.Sprintf("%s%s</doc>\n", l.reconstructXmlNodeText(se), inner.Xml), nil
			}

		default:
			//do nothing
		}
	}
}

func (l *recordLoaderImpl) extractId(buffer string) (string, error) {

	// generate a query structure from the body
	doc, err := xmlquery.Parse(bytes.NewReader([]byte(buffer)))
	if err != nil {
		return "", err
	}

	// attempt to extract the statusNode field
	idNode := xmlquery.FindOne(doc, "//doc/field[@name='id']")
	if idNode == nil {
		return "", ErrMissingRecordId
	}

	// make sure the ID is not empty
	id := strings.TrimSpace(idNode.InnerText())
	if len(id) == 0 {
		return "", ErrBlankRecordId
	}

	return id, nil
}

func (l *recordLoaderImpl) reconstructXmlNodeText(token xml.StartElement) string {

	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("<%s", token.Name.Local))
	for _, r := range token.Attr {
		builder.WriteString(fmt.Sprintf(" %s=\"%s\"", r.Name.Local, r.Value))
	}
	builder.WriteString(">")
	return builder.String()
}

func (r *recordImpl) Id() string {
	return r.RecordId
}

func (r *recordImpl) Raw() []byte {
	return r.RawBytes
}

//
// end of file
//
