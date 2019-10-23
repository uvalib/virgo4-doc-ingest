package main

import (
	"github.com/uvalib/virgo4-sqs-sdk/awssqs"
	"log"
	"time"
)

// time to wait before flushing pending records
var flushTimeout = 5 * time.Second

func worker(id int, config ServiceConfig, aws awssqs.AWS_SQS, queue1 awssqs.QueueHandle, queue2 awssqs.QueueHandle, records <-chan Record) {

	count := uint(1)
	messages := make([]awssqs.Message, 0, awssqs.MAX_SQS_BLOCK_COUNT)
	var record Record
	for {

		timeout := false

		// process a message or wait...
		select {
		case record = <-records:
			break
		case <-time.After(flushTimeout):
			timeout = true
			break
		}

		// did we timeout, if not we have a message to process
		if timeout == false {

			messages = append(messages, constructMessage(record))

			// have we reached a block size limit
			if count%awssqs.MAX_SQS_BLOCK_COUNT == 0 {

				// send the block
				err := sendOutboundMessages(config, aws, queue1, queue2, messages)
				if err != nil {
					if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
						fatalIfError(err)
					}
				}

				// reset the block
				messages = messages[:0]
			}
			count++

			if count%1000 == 0 {
				log.Printf("Worker %d processed %d records", id, count)
			}
		} else {

			// we timed out waiting for new messages, let's flush what we have (if anything)
			if len(messages) != 0 {

				// send the block
				err := sendOutboundMessages(config, aws, queue1, queue2, messages)
				if err != nil {
					if err != awssqs.OneOrMoreOperationsUnsuccessfulError {
						fatalIfError(err)
					}
				}

				// reset the block
				messages = messages[:0]

				log.Printf("Worker %d processed %d records (flushing)", id, count)
			}

			// reset the count
			count = 1
		}
	}

	// should never get here
}

func constructMessage(record Record) awssqs.Message {

	//payload := fmt.Sprintf( xmlDocFormatter, id )
	attributes := make([]awssqs.Attribute, 0, 4)
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordId, Value: record.Id()})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordType, Value: awssqs.AttributeValueRecordTypeXml})
	attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordOperation, Value: awssqs.AttributeValueRecordOperationUpdate})
	//attributes = append(attributes, awssqs.Attribute{Name: awssqs.AttributeKeyRecordSource, Value: datasource})
	return awssqs.Message{Attribs: attributes, Payload: record.Raw()}
}

func sendOutboundMessages(config ServiceConfig, aws awssqs.AWS_SQS, queue1 awssqs.QueueHandle, queue2 awssqs.QueueHandle, batch []awssqs.Message) error {

	opStatus, err1 := aws.BatchMessagePut(queue1, batch)
	if err1 != nil {
		if err1 != awssqs.OneOrMoreOperationsUnsuccessfulError {
			return err1
		}
	}

	// if one or more message failed to send, report the error
	if err1 == awssqs.OneOrMoreOperationsUnsuccessfulError {

		// check the operation results
		for ix, op := range opStatus {
			if op == false {
				log.Printf("WARNING: message %d failed to send to queue 1", ix)
			}
		}
	}

	//	opStatus, err2 = aws.BatchMessagePut(queue2, batch)
	//	if err2 != nil {
	//		if err2 != awssqs.OneOrMoreOperationsUnsuccessfulError {
	//			return err2
	//		}
	//	}
	//
	//	// if one or more message failed to send, report the error
	//	if err2 == awssqs.OneOrMoreOperationsUnsuccessfulError {
	//
	//		// check the operation results
	//		for ix, op := range opStatus {
	//			if op == false {
	//				log.Printf("WARNING: message %d failed to send to queue 2", ix)
	//			}
	//		}
	//	}
	//

	// report that some of the messages were not processed
	if err1 == awssqs.OneOrMoreOperationsUnsuccessfulError {
//	if err1 == awssqs.OneOrMoreOperationsUnsuccessfulError || err2 == awssqs.OneOrMoreOperationsUnsuccessfulError {
		return awssqs.OneOrMoreOperationsUnsuccessfulError
	}

	return nil
}

//
// end of file
//
