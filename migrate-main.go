/*
 * MinIO Client (C) 2021 MinIO, Inc.
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
	"time"

	humanize "github.com/dustin/go-humanize"
	"github.com/minio/cli"
	miniogo "github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio/pkg/console"
)

var migrateFlags = []cli.Flag{
	cli.IntFlag{
		Name:  "skip, s",
		Usage: "number of entries to skip from input file",
		Value: 0,
	},
	cli.BoolFlag{
		Name:  "fake",
		Usage: "perform a fake migration",
	},
	cli.StringFlag{
		Name:  "input-file",
		Usage: "file with list of entries to migrate from HCP",
	},
}
var migrateCmd = cli.Command{
	Name:   "migrate",
	Usage:  "Migrate HCP objects to MinIO",
	Action: migrateAction,
	Flags:  append(allFlags, migrateFlags...),
	CustomHelpTemplate: `NAME:
	{{.HelpName}} - {{.Usage}}

USAGE:
	{{.HelpName}} --auth-token --namespace-url --host-header --data-dir [--skip, --fake]

FLAGS:
   {{range .VisibleFlags}}{{.}}
   {{end}}

EXAMPLES:
1. Migrate objects in input file from HCP to MinIO.
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ hcp-to-minio migrate -a "HCP bXl1c2Vy:3f3c6784e97531774380db177774ac8d" --host-header "HOST:s3testbucket.tenant.hcp.example.com \
			--namespace-url "https://hcp-vip.example.com/rest" --data-dir "/tmp/data" \
			--input-file "/tmp/data/to_migrate.txt"

2. Migrate objects in input file from HCP to MinIO after skipping 100000 entries in this file
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ hcp-to-minio migrate -a "HCP bXl1c2Vy:3f3c6784e97531774380db177774ac8d" --host-header "HOST:s3testbucket.tenant.hcp.example.com \
 			--namespace-url "https://hcp-vip.example.com/rest" --data-dir "/tmp/data" \
			--skip 100000 --input-file "/tmp/data/to_migrate.txt"

3. Perform a dry run for migrating objects in input file from HCP to MinIO
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ hcp-to-minio migrate -a "HCP bXl1c2Vy:3f3c6784e97531774380db177774ac8d" --host-header "HOST:s3testbucket.tenant.hcp.example.com \
		--namespace-url "https://hcp-vip.example.com/rest" --data-dir "/tmp/data" \
		--fake --log --input-file "/tmp/data/to_migrate.txt"
`,
}
var minioClient *miniogo.Client

const (

	// EnvMinIOEndpoint MinIO endpoint
	EnvMinIOEndpoint = "MINIO_ENDPOINT"
	// EnvMinIOAccessKey MinIO access key
	EnvMinIOAccessKey = "MINIO_ACCESS_KEY"
	// EnvMinIOSecretKey MinIO secret key
	EnvMinIOSecretKey = "MINIO_SECRET_KEY"
	// EnvMinIOBucket bucket to MinIO to.
	EnvMinIOBucket = "MINIO_BUCKET"
)

func initMinioClient(ctx *cli.Context) error {
	mURL := os.Getenv(EnvMinIOEndpoint)
	if mURL == "" {
		return fmt.Errorf("MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY and MINIO_BUCKET need to be set")
	}
	target, err := url.Parse(mURL)
	if err != nil {
		return fmt.Errorf("unable to parse input arg %s: %v", mURL, err)
	}

	accessKey := os.Getenv(EnvMinIOAccessKey)
	secretKey := os.Getenv(EnvMinIOSecretKey)
	minioBucket = os.Getenv(EnvMinIOBucket)
	// // if unspecified use the HCP bucket name
	// if minioBucket == "" {
	// 	minioBucket = bucket
	// }
	if accessKey == "" || secretKey == "" || minioBucket == "" {
		console.Fatalln(fmt.Errorf("one or more of AccessKey:%s SecretKey: %s Bucket:%s ", accessKey, secretKey, bucket), "are missing in MinIO configuration")
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
			MaxIdleConnsPerHost:   256,
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

func migrateAction(cliCtx *cli.Context) error {
	checkArgsAndInit(cliCtx)
	ctx := context.Background()
	logMsg("Init minio client..")
	if err := initMinioClient(cliCtx); err != nil {
		logDMsg("Unable to  initialize MinIO client, exiting...%w", err)
		cli.ShowCommandHelp(cliCtx, cliCtx.Command.Name) // last argument is exit code
		console.Fatalln(err)
	}
	migrationState = newMigrationState(ctx)
	migrationState.init(ctx)
	skip := cliCtx.Int("skip")
	dryRun = cliCtx.Bool("fake")
	start := time.Now()
	inputFile := cliCtx.String("input-file")
	file, err := os.Open(inputFile)
	if err != nil {
		console.Fatalln("--input-file needs to be specified", err)
	}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		o := scanner.Text()
		if skip > 0 {
			skip--
			continue
		}
		migrationState.queueUploadTask(o)
		logDMsg(fmt.Sprintf("adding %s to migration queue", o), nil)
	}
	if err := scanner.Err(); err != nil {
		logDMsg(fmt.Sprintf("error processing file :%s ", objListFile), err)
		return err
	}
	migrationState.finish(ctx)
	if dryRun {
		logMsg("Migration dry run complete")
	} else {
		end := time.Now()
		latency := end.Sub(start).Seconds()
		count := migrationState.getCount() - migrationState.getFailCount()
		logMsg(fmt.Sprintf("Migrated %s / %s objects with latency %d secs", humanize.Comma(int64(count)), humanize.Comma(int64(migrationState.getCount())), int64(latency)))
		hcp.printLatencyStats()
	}
	return nil
}
