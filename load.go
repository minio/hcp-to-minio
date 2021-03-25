package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"

	miniogo "github.com/minio/minio-go/v7"
)

var dryRun bool

type migrateState struct {
	objectCh chan string
	failedCh chan string
	count    uint64
	failCnt  uint64
	wg       sync.WaitGroup
}

func (m *migrateState) queueUploadTask(obj string) {
	select {
	case m.objectCh <- obj:
	default:
	}
}

var (
	migrationState      *migrateState
	migrationConcurrent = runtime.GOMAXPROCS(0) / 2
)

func newMigrationState(ctx context.Context) *migrateState {

	// fix minimum concurrent migration to 1 for single CPU setup
	if migrationConcurrent == 0 {
		migrationConcurrent = 1
	}
	ms := &migrateState{
		objectCh: make(chan string, 10000),
		failedCh: make(chan string, 1000),
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
					m.failedCh <- obj
					logDMsg(fmt.Sprintf("error migrating object %s", obj), err)
					continue
				}
				m.incCount()
			}
		}
	}()
}
func (m *migrateState) finish(ctx context.Context) {
	close(m.objectCh)
	m.wg.Wait() // wait on workers to finish

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
		f, err := os.OpenFile(path.Join(dirPath, failMigFile), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			logDMsg("could not create + failMigFile", err)
			return
		}
		fwriter := bufio.NewWriter(f)
		defer fwriter.Flush()
		defer f.Close()

		for {
			select {
			case <-ctx.Done():
				return

			case obj, ok := <-migrationState.failedCh:
				if !ok {
					return
				}
				if _, err := f.WriteString(obj + "\n"); err != nil {
					logDMsg("Error writing to migration_fails.txt for "+obj, err)
				}

			}
		}
	}()
}

func migrateObject(ctx context.Context, object string) error {
	r, oi, _, err := hcp.GetObject(object, annotation)
	if err != nil {
		return err
	}
	if dryRun {
		logMsg(migrateMsg(object, oi.Key))
		return nil
	}
	_, err = minioClient.PutObject(ctx, minioBucket, oi.Key, r, oi.Size, miniogo.PutObjectOptions{
		ContentType:  oi.ContentType,
		UserMetadata: oi.UserMetadata,
		Internal: miniogo.AdvancedPutOptions{
			SourceETag:  oi.ETag,
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
