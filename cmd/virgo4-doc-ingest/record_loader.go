package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/antchfx/xmlquery"
)

var ErrBadRecord = fmt.Errorf("bad record encountered")
var ErrBadRecordId = fmt.Errorf("bad record identifier")
var ErrMissingRecordId = fmt.Errorf("missing record identifier")
var ErrFileNotOpen = fmt.Errorf("file is not open")

// the RecordLoader interface
type RecordLoader interface {
	Validate() error
	First() (Record, error)
	Next() (Record, error)
	Done()
}

// the record interface
type Record interface {
	Id() string
	Raw() []byte
}

// this is our loader implementation
type recordLoaderImpl struct {
	File   *os.File
	Reader *bufio.Reader
}

// this is our record implementation
type recordImpl struct {
	RawBytes []byte
	RecordId string
}

// and the factory
func NewRecordLoader(filename string) (RecordLoader, error) {

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	reader := bufio.NewReader(file)

	return &recordLoaderImpl{File: file, Reader: reader}, nil
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
			log.Printf("ERROR: validation failure on record index 0")
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
				log.Printf("ERROR: validation failure on record index %d", recordIndex)
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

	line, err := l.Reader.ReadString('\n')
	if err != nil {
		return nil, err
	}

	// remove the newline
	line = strings.TrimSuffix(line, "\n")

	if len(line) == 0 {
		return nil, ErrBadRecord
	}

	// attempt to extract the ID from the payload
	id, err := l.extractId(line)
	if err != nil {
		return nil, err
	}

	//if id[0] != 'u' {
	//	log.Printf("ERROR: record id is suspect (%s)", id)
	//	return nil, ErrBadRecordId
	//}

	return &recordImpl{RecordId: id, RawBytes: []byte(line)}, nil
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

	return idNode.InnerText(), nil
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
