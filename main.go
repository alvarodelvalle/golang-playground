// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX - License - Identifier: Apache - 2.0
// snippet-start:[s3.go-v2.ListBuckets]
package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"log"
)

// s3Bucket defines a bucket and their configurations
type s3Bucket struct {
	name *string
	acl *s3.GetBucketAclOutput
	encryption *s3.GetBucketEncryptionOutput
	creationDate string
}

/*
   Knowledge nugget: any structure that implements all the behaviors(i.e. methods) of an interface becomes an interface.
*/

// S3ListBucketsApi defines the interface for the ListBuckets function.
// We use this interface to test the function using a mocked service.
type S3ListBucketsApi interface {
	ListBuckets(ctx context.Context,
	params *s3.ListBucketsInput,
	optFns ...func(*s3.Options)) (*s3.ListBucketsOutput, error)
}

// S3GetBucketAclApi defines the interface for the GetBucketAcl function.
// We use this interface to test the function using a mocked service.
type S3GetBucketAclApi interface {
	GetBucketAcl(ctx context.Context,
		params *s3.GetBucketAclInput,
		optFns ...func(*s3.Options)) (*s3.GetBucketAclOutput, error)
}

// S3GetBucketEncryptionApi defines the interface for the GetBucketEncryption function.
// We use this interface to test the function using a mocked service.
type S3GetBucketEncryptionApi interface {
	GetBucketEncryption(ctx context.Context,
		params *s3.GetBucketEncryptionInput,
		optFns ...func(options *s3.Options)) (*s3.GetBucketEncryptionOutput, error)
}

// S3GetBucketLocationApi defines the interface for the GetBucketLocation function.
// We use this interface to test the function using a mocked service.
type S3GetBucketLocationApi interface {
	GetBucketLocation(ctx context.Context,
		params *s3.GetBucketLocationInput,
		optFns ...func(options *s3.Options)) (*s3.GetBucketLocationOutput, error)
}

// GetAllBuckets retrieves a list of your Amazon Simple Storage Service (Amazon S3) buckets.
// Inputs:
//     c is the context of the method call.
//     api is the interface that defines the method call.
//     input defines the input arguments to the service call.
// Output:
//     If success, a ListBucketsOutput object containing the result of the service call and nil.
//     Otherwise, nil and an error from the call to ListBuckets.
func GetAllBuckets(c context.Context, api S3ListBucketsApi, input *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	return api.ListBuckets(c, input)
}

// GetBucketAcl returns the access control list (ACL) of a bucket.
// Inputs:
//     c is the context of the method call.
//     api is the interface that defines the method call.
//     input defines the input arguments to the service call.
// Output:
//     If success, a GetBucketAclOutput object containing the result of the service call and nil.
//     Otherwise, nil and an error from the call to GetBucketAcl.
func GetBucketAcl(c context.Context, api S3GetBucketAclApi, input *s3.GetBucketAclInput) (*s3.GetBucketAclOutput, error) {
	return api.GetBucketAcl(c, input)
}

// GetBucketEncryption returns the encryption configuration of a bucket.
// Inputs:
//     c is the context of the method call.
//     api is the interface that defines the method call.
//     input defines the input arguments to the service call.
// Output:
//     If success, a GetBucketEncryptionOutput object containing the result of the service call and nil.
//     Otherwise, nil and an error from the call to GetBucketAcl.
func GetBucketEncryption(c context.Context, api S3GetBucketEncryptionApi, input *s3.GetBucketEncryptionInput) (*s3.GetBucketEncryptionOutput, error) {
	return api.GetBucketEncryption(c, input)
}

func GetBucketLocation(c context.Context, api S3GetBucketLocationApi, input *s3.GetBucketLocationInput) (*s3.GetBucketLocationOutput, error) {
	return api.GetBucketLocation(c, input)
}

func main() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		panic("configuration error, " + err.Error())
	}
	client := s3.NewFromConfig(cfg)

	allBuckets, err := GetAllBuckets(context.TODO(), client, &s3.ListBucketsInput{})
	if err != nil {
		fmt.Println("Got an error retrieving buckets:")
		fmt.Println(err)
		return
	}

	fmt.Println("Buckets:\n")

	for _, bucket := range allBuckets.Buckets {
		// Get the location of the bucket, use it to update the client in order to make a request to the correct S3 endpoint
		location, err := GetBucketLocation(context.TODO(), client, &s3.GetBucketLocationInput{
			Bucket:              bucket.Name,
			ExpectedBucketOwner: nil,
		})
		if err != nil {
			fmt.Println("Got an error retrieving buckets' location:")
			fmt.Println(err)
			return
		}

		// update the client with the buckets' region; if location is "" then it must be us-east-1
		client = s3.NewFromConfig(cfg, func(options *s3.Options) {
			if location.LocationConstraint == "" {
				options.Region = "us-east-1"
			} else {
				options.Region = string(location.LocationConstraint)
			}
		})

		_, err = GetBucketAcl(context.TODO(), client, &s3.GetBucketAclInput{
			Bucket:              bucket.Name,
			ExpectedBucketOwner: nil,
		})
		if err != nil {
			fmt.Printf("Got an error retrieving bucket acl: %v", err)
			return
		}

		encryption, err := GetBucketEncryption(context.TODO(), client, &s3.GetBucketEncryptionInput{
			Bucket:              bucket.Name,
			ExpectedBucketOwner: nil,
		})
		if err != nil {
			var ae smithy.APIError
			if errors.As(err, &ae) {
				log.Printf("Got an API error retrieving bucket encryption bucket: %v, code: %s, message: %s, fault: %s", *bucket.Name, ae.ErrorCode(), ae.ErrorMessage(), ae.ErrorFault().String())
			} else {
				log.Printf("Got an error retrieving bucket encryption: %v", err)
			}
		}

		if encryption != nil {
			b := s3Bucket{
				name: bucket.Name,
				encryption: encryption,
			}
			fmt.Printf("Bucket: %+v\t KeyID: %+v\n", b.name, aws.ToString(b.encryption.ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.KMSMasterKeyID))
		} else {
			b := s3Bucket{
				name: bucket.Name,
			}
			fmt.Printf("Bucket: %+v\t KeyID: <nil>\n", b.name)
		}

	}

}

