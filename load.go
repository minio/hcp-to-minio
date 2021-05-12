package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	miniogo "github.com/minio/minio-go/v7"
)

var dryRun bool
var download bool

type migrateState struct {
	objectCh chan string
	failedCh chan migrationErr
	logCh    chan string
	count    uint64
	failCnt  uint64
	wg       sync.WaitGroup
}

type migrationErr struct {
	object string
	err    error
}

func (m *migrateState) queueUploadTask(obj string) {
	m.objectCh <- obj
}

var (
	migrationState      *migrateState
	migrationConcurrent = 100
)

func newMigrationState(ctx context.Context) *migrateState {
	if runtime.GOMAXPROCS(0) > migrationConcurrent {
		migrationConcurrent = runtime.GOMAXPROCS(0)
	}
	ms := &migrateState{
		objectCh: make(chan string, migrationConcurrent),
		failedCh: make(chan migrationErr, migrationConcurrent),
		logCh:    make(chan string, migrationConcurrent),
	}

	return ms
}

// Increase count processed
func (m *migrateState) incCount() {
	atomic.AddUint64(&m.count, 1)
}

// Get total count processed
func (m *migrateState) getCount() uint64 {
	return atomic.LoadUint64(&m.count)
}

// Increase count failed
func (m *migrateState) incFailCount() {
	atomic.AddUint64(&m.failCnt, 1)
}

// Get total count failed
func (m *migrateState) getFailCount() uint64 {
	return atomic.LoadUint64(&m.failCnt)
}

// addWorker creates a new worker to process tasks
func (m *migrateState) addWorker(ctx context.Context) {
	m.wg.Add(1)
	// Add a new worker.
	go func() {
		defer m.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-m.objectCh:
				if !ok {
					return
				}
				logDMsg(fmt.Sprintf("Migrating...%s", obj), nil)
				if err := migrateObject(ctx, obj); err != nil {
					m.incFailCount()
					logMsg(fmt.Sprintf("error migrating object %s: %s", obj, err))
					m.failedCh <- migrationErr{object: obj, err: err}
					continue
				}
				m.incCount()
				m.logCh <- obj
			}
		}
	}()
}
func (m *migrateState) finish(ctx context.Context) {
	close(m.objectCh)
	m.wg.Wait() // wait on workers to finish
	close(m.failedCh)
	close(m.logCh)

	if !dryRun {
		logMsg(fmt.Sprintf("Migrated %d objects, %d failures", m.getCount(), m.getFailCount()))
	}
}
func (m *migrateState) init(ctx context.Context) {
	if m == nil {
		return
	}
	for i := 0; i < migrationConcurrent; i++ {
		m.addWorker(ctx)
	}
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, getFileName(failMigFile, "")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create + migration_fails.txt", err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-m.failedCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(obj.object + " : " + obj.err.Error() + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to migration_fails.txt for "+obj.object, err))
					os.Exit(1)
				}

			}
		}
	}()
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, getFileName(logMigFile, "")), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create + migration_log.txt", err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-ctx.Done():
				return
			case obj, ok := <-m.logCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(obj + "\n"); err != nil {
					logMsg(fmt.Sprintf("Error writing to migration_log.txt for "+obj, err))
					os.Exit(1)
				}

			}
		}
	}()

}

func migrateObject(ctx context.Context, object string) error {
	r, oi, err := hcp.GetObject(object)
	if err != nil {
		return err
	}
	defer r.Close()
	if dryRun {
		logMsg(migrateMsg(object, oi.Key))
		return nil
	}
	if download {
		localFile, err := os.Create(path.Join(dirPath, strings.ReplaceAll(oi.Key, "/", "-")))
		if err != nil {
			logDMsg("could not create file "+oi.Key, err)
		}
		defer localFile.Close()

		if _, err := io.CopyN(localFile, r, oi.Size); err != nil {
			logDMsg("Download of "+oi.Key+" to disk failed", err)
		}
		return nil
	}
	if _, err = minioClient.StatObject(ctx, minioBucket, oi.Key, miniogo.StatObjectOptions{}); err == nil {
		logDMsg("object already exists on MinIO "+oi.Key+" not migrated", err)
		return nil
	}
	_, err = minioClient.PutObject(ctx, minioBucket, oi.Key, r, oi.Size, miniogo.PutObjectOptions{
		Internal: miniogo.AdvancedPutOptions{
			SourceMTime: oi.LastModified,
		},
	})
	if err != nil {
		logDMsg("upload to minio client failed for "+oi.Key, err)
		return err
	}
	logDMsg("Uploaded "+oi.Key+" successfully", nil)
	return nil
}
