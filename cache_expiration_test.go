// Tests for groupcache expiration

package groupcache

import (
	"fmt"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	expireGroup     *Group
	initOnce_expire sync.Once
)

func expireCacheSetup() {
	setTimeProvider(defaultTimeProvider{}) // reset TimeProvider

	chanSequence := make(chan int, 100)
	for i := 0; i < 100; i++ {
		chanSequence <- i
	}

	expireGroup = NewGroup("expire-group", 1<<20, GetterFunc(func(_ Context, key string, dest Sink) error {
		time.Sleep(200 * time.Millisecond)
		return dest.SetTimestampBytes([]byte(fmt.Sprintf("%s=%d", key, <-chanSequence)), GetTime())
	}))
	expireGroup.SetExpiration(time.Duration(2) * time.Second)
	expireGroup.SetStalePeriod(time.Duration(2) * time.Second)
	expireGroup.SetStaleDeadline(100 * time.Millisecond) // Get() may return old value if not set
}

func callGetCache(gcache *Group, key string) (string, error, int64) {
	start_ts := time.Now()
	var packedContent []byte
	err := gcache.Get(nil, key, AllocatingByteSliceSink(&packedContent))
	if err != nil {
		return "", err, int64(time.Since(start_ts) / time.Millisecond)
	}
	content, _, err := UnpackTimestamp(packedContent)
	return string(content), err, int64(time.Since(start_ts) / time.Millisecond)
}

func TestGroupExpiration(t *testing.T) {
	initOnce_expire.Do(expireCacheSetup)

	Convey("It should return cached value when hit", t, func() {
		content, err, cost_ms := callGetCache(expireGroup, "test")
		So(err, ShouldBeNil)
		So(content, ShouldEqual, "test=0")
		So(cost_ms, ShouldBeBetweenOrEqual, 195, 205) // load() cost

		time.Sleep(1000 * time.Millisecond)

		content, err, cost_ms = callGetCache(expireGroup, "test")
		So(err, ShouldBeNil)
		So(content, ShouldEqual, "test=0")
		So(cost_ms, ShouldBeBetweenOrEqual, 0, 5) // read from cache
	})

	Convey("It should return old value when expire, but return new value in next fetch", t, func() {
		time.Sleep(2000 * time.Millisecond)

		content, err, cost_ms := callGetCache(expireGroup, "test")
		So(err, ShouldBeNil)
		So(content, ShouldEqual, "test=0")
		So(cost_ms, ShouldBeBetweenOrEqual, 95, 105) // only wait StaleDeadline(100ms)

		time.Sleep(220 * time.Millisecond) // wait load() complete, refetch

		content, err, cost_ms = callGetCache(expireGroup, "test")
		So(err, ShouldBeNil)
		So(content, ShouldEqual, "test=1")
		So(cost_ms, ShouldBeBetweenOrEqual, 0, 5) // read from cache
	})

	Convey("It should return new value when reach stalePeriod", t, func() {
		time.Sleep(4200 * time.Millisecond) // wait Expiration + StalePeriod

		content, err, cost_ms := callGetCache(expireGroup, "test")
		So(err, ShouldBeNil)
		So(content, ShouldEqual, "test=2")
		So(cost_ms, ShouldBeBetweenOrEqual, 195, 205) // force load()
	})
}

func TestCache_nbytes(t *testing.T) {
	initOnce_expire.Do(expireCacheSetup)

	// If key exist in Cache, it return wrong nbytes here
	Convey("Cache's nbytes should be zero", t, func() {
		So(expireGroup.mainCache.bytes(), ShouldNotEqual, 0)

		if expireGroup.mainCache.lru != nil {
			expireGroup.mainCache.lru.Clear()
			So(expireGroup.mainCache.bytes(), ShouldEqual, 0)
		}
		if expireGroup.hotCache.lru != nil {
			expireGroup.hotCache.lru.Clear()
			So(expireGroup.hotCache.bytes(), ShouldEqual, 0)
		}
	})
}
