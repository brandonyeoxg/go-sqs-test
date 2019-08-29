package main

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

func sendMsg(svc *sqs.SQS, qURL string, msg string) (*sqs.SendMessageOutput, error) {
	sresult, err := svc.SendMessage(&sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"Type": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String("Signal"),
			},
		},
		MessageBody:    aws.String("Send msg"),
		QueueUrl:       &qURL,
		MessageGroupId: aws.String("1"),
	})
	if err != nil {
		fmt.Println("Error", err)
		return nil, err
	}
	return sresult, nil
}

func recvMsg(svc *sqs.SQS, qURL string) (*sqs.ReceiveMessageOutput, error) {
	rresult, err := svc.ReceiveMessage(&sqs.ReceiveMessageInput{
		AttributeNames: []*string{
			aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
		},
		QueueUrl:            &qURL,
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(20),
		WaitTimeSeconds:     aws.Int64(0),
	})

	if err != nil {
		return nil, err
	}
	return rresult, err
}

func main() {
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ap-southeast-1"),
		Credentials: credentials.NewSharedCredentials("", "sqs"),
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}

	svc := sqs.New(sess)

	fmt.Println("Listing all queues in account")
	qresult, err := svc.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String("test-queue.fifo"),
	})

	if err != nil {
		fmt.Println("Error", err)
		return
	}
	fmt.Println("Success", *qresult.QueueUrl)
	qURL := *qresult.QueueUrl

	for i := 0; i < 10; i++ {
		sresult, err := sendMsg(svc, qURL, fmt.Sprintf("Sending %d", i))

		if err != nil {
			fmt.Println("Error", err)
			return
		}

		fmt.Println("Success", sresult.MessageId)
	}

	for i := 0; i < 5; i++ {
		rresult, err := recvMsg(svc, qURL)
		if err != nil {
			fmt.Println("Error", err)
			return
		}
		if len(rresult.Messages) == 0 {
			fmt.Println("Received no messages")
		}

		for j := 0; j < len(rresult.Messages); j++ {
			fmt.Println("Message:", rresult.Messages[j].String())
		}

		fmt.Println("Success", rresult.Messages)
	}

}
