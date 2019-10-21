package main

import (
	"log"
	"time"

	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
)

// time to wait for inbound messages before doing something else
var waitTimeout = 5 * time.Second

func worker(id int, config *ServiceConfig, aws awssqs.AWS_SQS, inbound <-chan awssqs.Message, outQueue awssqs.QueueHandle) {

	// keep a list of the messages queued so we can delete them once they are sent to SOLR
	queued := make([]awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT)
	var message awssqs.Message

	blocksize := uint(0)
	totalCount := uint(0)
	start := time.Now()

	for {

		arrived := false

		// process a message or wait...
		select {
		case message = <-inbound:
			arrived = true
			break
		case <-time.After(waitTimeout):
			break
		}

		// we have an inbound message to process
		if arrived == true {

			// update counts
			blocksize++
			totalCount++

			// add it to the queued list
			queued = append(queued, message)
			if blocksize == awssqs.MAX_SQS_BLOCK_COUNT {
				err := processesOutboundBlock(id, aws, queued, outQueue)
				if err != nil {
					log.Fatal(err)
				}

				// reset the counts
				blocksize = 0
				queued = queued[:0]
			}

			if totalCount%1000 == 0 {
				duration := time.Since(start)
				log.Printf("Worker %d: processed %d messages (%0.2f tps)", id, totalCount, float64(totalCount)/duration.Seconds())
			}

		} else {

			// we timed out, probably best to send anything pending
			if blocksize != 0 {
				err := processesOutboundBlock(id, aws, queued, outQueue)
				fatalIfError(err)

				duration := time.Since(start)
				log.Printf("Worker %d: processed %d messages (flushing) (%0.2f tps)", id, totalCount, float64(totalCount)/duration.Seconds())

				// reset the counts
				blocksize = 0
				queued = queued[:0]
			}

			// reset the time
			start = time.Now()
		}
	}
}

func processesOutboundBlock(id int, aws awssqs.AWS_SQS, messages []awssqs.Message, outQueue awssqs.QueueHandle) error {

	// attempt to send the messages
	opStatus, err := aws.BatchMessagePut(outQueue, messages)
	if err != nil {
		if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err
		}
	}

	// if one or more message failed to send, retry...
	if err == awssqs.OneOrMoreOperationsUnsuccessfulError {
		retryMessages := make([]awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT)

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf("WARNING: message %d failed to send to queue, retrying", ix)
				retryMessages = append(retryMessages, messages[ix])
			}
		}

		// attempt another send of the ones that failed last time
		opStatus, err = aws.BatchMessagePut(outQueue, retryMessages)
		if err != nil {
			if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
				return err
			}
		}

		// did we fail again
		if err == awssqs.OneOrMoreOperationsUnsuccessfulError {
			for ix, op := range opStatus {
				if op == false {
					log.Printf("ERROR: message %d failed to send to queue, giving up", ix)
				}
			}
		}
	}

	return nil
}

//
// end of file
//
