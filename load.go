package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"runtime"
	"sync"
	"sync/atomic"

	miniogo "github.com/minio/minio-go/v7"
)

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
			case oi, ok := <-m.objectCh:
				if !ok {
					return
				}
				logDMsg(fmt.Sprintf("Migrating...%s", oi), nil)
				if err := migrateObject(ctx, oi); err != nil {
					m.incFailCount()
					logDMsg(fmt.Sprintf("error migrating object %s", oi), err)
					continue
				}
				m.incCount()
			}
		}
	}()
}
func (m *migrateState) finish(ctx context.Context) {
	close(m.objectCh)
	close(m.failedCh)
	m.wg.Wait() // wait on workers to finish
	logMsg(fmt.Sprintf("Migrated %d objects, %d failures", m.getCount(), m.getFailCount()))
}
func initMigration(ctx context.Context) {
	if migrationState == nil {
		return
	}
	for i := 0; i < migrationConcurrent; i++ {
		migrationState.addWorker(ctx)
	}
	go func() {
		f, err := os.OpenFile(path.Join(dirPath, failMigFile), os.O_CREATE, 0600)
		if err != nil {
			log.Println("could not create %s due to %w", failMigFile, err)
			return
		}
		defer f.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case obj := <-migrationState.failedCh:
				f.Write([]byte(obj))
			}
		}
	}()
}

func migrateObject(ctx context.Context, object string) error {
	r, oi, _, err := hcp.GetObject(bucket, object, annotation)
	if err != nil {
		return err
	}
	if dryRun {
		logMsg("DryRun: Will migrate " + object + " =>" + oi.Key)
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
