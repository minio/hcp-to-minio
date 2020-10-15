/*
 * Minio Cloud Storage, (C) 2020 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/fatih/color"
	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/console"
)

var (
	authToken          string
	hostHeader         string
	namespaceURL       string
	dirPath            string
	bucket             string // HCP bucket name
	minioBucket        string // in case user needs a different bucket name on MinIO
	debugFlag, logFlag bool
	annotation         string
	hcp                *hcpBackend
	minioClient        *miniogo.Client
	dryRun             bool
)

const (
	objListFile = "object_listing.txt"
	failMigFile = "migration_fails.txt"

	// EnvMinIOEndpoint MinIO endpoint
	EnvMinIOEndpoint = "MINIO_ENDPOINT"
	// EnvMinIOAccessKey MinIO access key
	EnvMinIOAccessKey = "MINIO_ACCESS_KEY"
	// EnvMinIOSecretKey MinIO secret key
	EnvMinIOSecretKey = "MINIO_SECRET_KEY"
	// EnvMinIOBucket bucket to MinIO to.
	EnvMinIOBucket = "MINIO_BUCKET"
)

func checkMain(ctx *cli.Context) {
	if !ctx.Args().Present() {
		//		console.Fatalln(fmt.Errorf("not arguments found, please check documentation '%s --help'", ctx.App.Name))
	}
	authToken = ctx.GlobalString("auth-token")
	hostHeader = ctx.GlobalString("host-header")
	namespaceURL = ctx.GlobalString("namespace-url")
	debugFlag = ctx.GlobalBool("debug")
	logFlag = ctx.GlobalBool("log")

	_, err := url.Parse(namespaceURL)
	if err != nil {
		console.Fatalln("--namespace-url malformed", namespaceURL)
	}

	dirPath = ctx.GlobalString("data-dir")
	//	bucket = ctx.GlobalString("bucket")
	annotation = ctx.GlobalString("annotation")

	if authToken == "" || hostHeader == "" || namespaceURL == "" {
		console.Fatalln(fmt.Errorf("--auth-token, --host-header and --namespace-url required, please check documentation '%s --help'", ctx.App.Name))
		return
	}
	if dirPath == "" {
		console.Fatalln(fmt.Errorf("path to working dir required, please set --data-dir flag"))
		return
	}
	// if bucket == "" {
	// 	console.Fatalln(fmt.Errorf("please set --bucket flag to specify starting namespace dir"))
	// 	return
	// }
	// if annotation == "" {
	// 	console.Fatalln(fmt.Errorf("please set --annotation flag to specify annotation"))
	// 	return
	// }

}

func initMinioClient(ctx *cli.Context) error {
	mURL := os.Getenv(EnvMinIOEndpoint)
	if mURL == "" {
		return fmt.Errorf("MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY need to be set")
	}
	target, err := url.Parse(mURL)
	if err != nil {
		return fmt.Errorf("Unable to parse input arg %s: %v", mURL, err)
	}

	accessKey := os.Getenv(EnvMinIOAccessKey)
	secretKey := os.Getenv(EnvMinIOSecretKey)
	minioBucket = os.Getenv(EnvMinIOBucket)
	// // if unspecified use the HCP bucket name
	// if minioBucket == "" {
	// 	minioBucket = bucket
	// }
	if accessKey == "" || secretKey == "" || minioBucket == "" {
		console.Fatalln(fmt.Errorf("One or more of AccessKey:%s SecretKey: %s Bucket:%s missing", accessKey, secretKey, bucket), "Missing MinIO configuration")
	}
	options := miniogo.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: target.Scheme == "https",
		Transport: &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:          256,
			MaxIdleConnsPerHost:   16,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 10 * time.Second,
			TLSClientConfig: &tls.Config{
				RootCAs: mustGetSystemCertPool(),
				// Can't use SSLv3 because of POODLE and BEAST
				// Can't use TLSv1.0 because of POODLE and BEAST using CBC cipher
				// Can't use TLSv1.1 because of RC4 cipher usage
				MinVersion:         tls.VersionTLS12,
				NextProtos:         []string{"http/1.1"},
				InsecureSkipVerify: ctx.GlobalBool("insecure"),
			},
			// Set this value so that the underlying transport round-tripper
			// doesn't try to auto decode the body of objects with
			// content-encoding set to `gzip`.
			//
			// Refer:
			//    https://golang.org/src/net/http/transport.go?h=roundTrip#L1843
			DisableCompression: true,
		},
		Region:       "",
		BucketLookup: 0,
	}

	api, err := miniogo.New(target.Host, &options)
	if err != nil {
		console.Fatalln(err)
	}

	// Store the new api object.
	minioClient = api
	return nil
}
func migrateMain(cliCtx *cli.Context) {
	checkMain(cliCtx)
	console.SetColor("Request", color.New(color.FgCyan))
	console.SetColor("Method", color.New(color.Bold, color.FgWhite))
	console.SetColor("Host", color.New(color.Bold, color.FgGreen))
	console.SetColor("ReqHeaderKey", color.New(color.Bold, color.FgWhite))
	console.SetColor("RespHeaderKey", color.New(color.Bold, color.FgCyan))
	console.SetColor("RespStatus", color.New(color.Bold, color.FgYellow))
	console.SetColor("ErrStatus", color.New(color.Bold, color.FgRed))
	console.SetColor("Response", color.New(color.FgGreen))
	hcp = &hcpBackend{
		URL:        namespaceURL,
		authToken:  authToken,
		hostHeader: hostHeader,
		Insecure:   cliCtx.Bool("insecure"),
	}
	dryRun = os.Getenv(EnvMinIOEndpoint) == ""
	if !dryRun {
		logMsg("Init minio client..")
		if err := initMinioClient(cliCtx); err != nil {
			logDMsg("Unable to  initialize MinIO client, exiting...%w", err)
			return
		}
	}
	ctx := context.Background()
	logMsg("Downloading namespace listing to disk...")
	if err := downloadObjectList(ctx, bucket); err != nil {
		logDMsg("exiting from listing", err)
		return
	}
	migrationState = newMigrationState(ctx)
	initMigration(ctx)
	file, err := os.Open(path.Join(dirPath, objListFile))
	if err != nil {
		logDMsg(fmt.Sprintf("could not open file :%s ", objListFile), err)
		return
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		o := scanner.Text()
		migrationState.queueUploadTask(o)
		logDMsg(fmt.Sprintf("adding %s to migration queue", o), nil)
	}
	if err := scanner.Err(); err != nil {
		logDMsg(fmt.Sprintf("error processing file :%s ", objListFile), err)
		return
	}
	migrationState.finish(ctx)
	logMsg("successfully completed migration.")
}
func main() {
	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Description = `Migration tool from HCP ObjectStore to MinIO`
	app.UsageText = "[FLAGS]"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "auth-token, a",
			Usage: "authorization token for HCP",
			Value: "",
		},
		cli.StringFlag{
			Name:  "namespace-url, n",
			Usage: "namespace URL path, e.g https://namespace-name.tenant-name.hcp-domain-name/rest",
		},
		cli.StringFlag{
			Name:  "host-header",
			Usage: "host header for HCP",
		},
		cli.StringFlag{
			Name:  "data-dir, d",
			Usage: "path to work directory for tool",
		},
		// cli.StringFlag{
		// 	Name:  "bucket",
		// 	Usage: "bucket/name space directory",
		// },
		cli.StringFlag{
			Name:  "annotation",
			Usage: "custom annotation name",
		},
		cli.BoolFlag{
			Name:  "insecure, i",
			Usage: "disable TLS certificate verification",
		},
		cli.BoolFlag{
			Name:  "log, l",
			Usage: "enable logging",
		},
		cli.BoolFlag{
			Name:  "debug",
			Usage: "enable debugging",
		},
	}
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Description}}

USAGE:
  {{.Name}} - {{.UsageText}}

FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}

EXAMPLES:
  1. Run migration tool to migrate from HCP to MinIO
	 $ migratehcp --a "HCP czN0ZXxxxx8177ec668013f38859f" --host-header "HOST:s3testbucket.sandbox.hcp01.slc.paypal.com" --namespace-url "https://hcp-vip.slc.paypal.com/rest"  --annotation "myannotation" --dir "/tmp/data"
`
	app.Action = migrateMain
	app.Run(os.Args)
}
