package main

import (
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// remote_mtime does a HEAD on the file and returns its Last-Modified time.
func remote_mtime(remote string, timeout time.Duration) (time.Time, error) {
	ctx_req, err := http.NewRequest("HEAD", remote, nil)
	if err != nil {
		return time.Time{}, err
	}
	client := &http.Client{Timeout: timeout}
	resp, err := client.Do(ctx_req)
	if err != nil {
		return time.Time{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return time.Time{}, fmt.Errorf("remote_mtime: status %d", resp.StatusCode)
	}
	lm := resp.Header.Get("Last-Modified")
	if lm == "" {
		return time.Time{}, fmt.Errorf("remote_mtime: no Last-Modified header")
	}
	t, err := http.ParseTime(lm)
	if err != nil {
		return time.Time{}, fmt.Errorf("remote_mtime: parse %q: %v", lm, err)
	}
	return t, nil
}

type purgeTask struct {
	key []byte
	rec Record
}

// purge iterates all live keys in leveldb and deletes any whose file on the
// TODO: should have efficient metadata server like s3
func (a *App) purge() {
	cutoff := time.Now().AddDate(0, 0, -a.expiry)
	fmt.Printf("purge: scanning for files older than %d days (before %s)\n", a.expiry, cutoff.Format(time.RFC3339))

	tasks := make(chan purgeTask, 1000)
	var wg sync.WaitGroup
	var deleted int64

	// 128 workers, same as rebuild
	for i := 0; i < 128; i++ {
		go func() {
			for t := range tasks {
				var mtime time.Time
				for _, volume := range t.rec.rvolumes {
					remote := fmt.Sprintf("http://%s%s", volume, key2path(t.key))
					mt, err := remote_mtime(remote, a.voltimeout)
					if err == nil {
						mtime = mt
						break
					}
				}

				if !mtime.IsZero() && mtime.Before(cutoff) {
					status := a.Delete(t.key, false)
					if status == 204 {
						atomic.AddInt64(&deleted, 1)
						fmt.Printf("purge: deleted %s (mtime %s)\n", string(t.key), mtime.Format(time.RFC3339))
					} else {
						fmt.Printf("purge: failed to delete %s: status %d\n", string(t.key), status)
					}
				}
				wg.Done()
			}
		}()
	}

	iter := a.db.NewIterator(nil, nil)
	for iter.Next() {
		rec := toRecord(iter.Value())
		if rec.deleted != NO {
			continue
		}
		key := make([]byte, len(iter.Key()))
		copy(key, iter.Key())
		wg.Add(1)
		tasks <- purgeTask{key, rec}
	}
	iter.Release()

	close(tasks)
	wg.Wait()

	fmt.Printf("purge: done, deleted %d files\n", atomic.LoadInt64(&deleted))
}

func (a *App) PurgeLoop() {
	for range time.Tick(24 * time.Hour) {
		a.purge()
	}
}
