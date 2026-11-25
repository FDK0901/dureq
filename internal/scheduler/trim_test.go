package scheduler

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/FDK0901/dureq/internal/store"
	"github.com/FDK0901/dureq/pkg/types"
)

// dispatchMany puts n messages on the work stream.
func dispatchMany(t *testing.T, st *store.RedisStore, n int) {
	t.Helper()
	for i := range n {
		if _, err := st.DispatchWork(context.Background(), "normal", &types.WorkMessage{RunID: fmt.Sprint("run", i), JobID: "j", TaskType: "t"}); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}
}

// take has the group read n messages and returns their IDs.
func take(t *testing.T, st *store.RedisStore, group string, n int) []string {
	t.Helper()
	rdb, key := st.Client(), store.WorkStreamKey(st.Prefix(), "normal")
	streams, err := rdb.Do(context.Background(), rdb.B().Xreadgroup().Group(group, "n").Count(int64(n)).Streams().Key(key).Id(">").Build()).AsXRead()
	if err != nil {
		t.Fatalf("XREADGROUP: %v", err)
	}
	var ids []string
	for _, e := range streams[key] {
		ids = append(ids, e.ID)
	}
	return ids
}

func ack(t *testing.T, st *store.RedisStore, group string, ids []string) {
	t.Helper()
	rdb := st.Client()
	if err := rdb.Do(context.Background(), rdb.B().Xack().Key(store.WorkStreamKey(st.Prefix(), "normal")).Group(group).Id(ids...).Build()).Error(); err != nil {
		t.Fatalf("XACK: %v", err)
	}
}

func streamIDs(t *testing.T, st *store.RedisStore) []string {
	t.Helper()
	var ids []string
	for _, e := range workEntries(t, st) {
		ids = append(ids, e.ID)
	}
	return ids
}

// wantWaiting checks that the number of messages nobody has taken can still be told.
func wantWaiting(t *testing.T, st *store.RedisStore, want int64) {
	t.Helper()
	depth, err := st.GetQueueDepth(context.Background(), "normal")
	if err != nil || depth.Waiting == nil || *depth.Waiting != want {
		t.Fatalf("waiting: %+v (%v), want %d", depth, err, want)
	}
}

func TestTrimWorkStream_KeepsWhatIsNotDone(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st})

	dispatchMany(t, st, 1000)
	s.trimWorkStreams(ctx)
	if n := len(streamIDs(t, st)); n != 1000 {
		t.Fatalf("%d of 1000 messages left although none was taken", n)
	}

	taken := take(t, st, store.ConsumerGroup, 700)
	s.trimWorkStreams(ctx)
	if n := len(streamIDs(t, st)); n != 1000 {
		t.Fatalf("%d of 1000 messages left although none was acknowledged", n)
	}

	// All but two are done, the older of them being the 400th.
	inFlight := []string{taken[400], taken[600]}
	ack(t, st, store.ConsumerGroup, slices.DeleteFunc(slices.Clone(taken), func(id string) bool { return slices.Contains(inFlight, id) }))
	all := streamIDs(t, st)
	s.trimWorkStreams(ctx)
	left := streamIDs(t, st)
	if !slices.Equal(left[len(left)-600:], all[400:]) {
		t.Fatalf("%d messages left, without some from the oldest in flight on", len(left))
	}
	if len(left) > 750 {
		t.Fatalf("%d messages left, want about 600", len(left))
	}
	wantWaiting(t, st, 300)

	ack(t, st, store.ConsumerGroup, inFlight)
	ack(t, st, store.ConsumerGroup, take(t, st, store.ConsumerGroup, 300))
	s.trimWorkStreams(ctx)
	if n := len(streamIDs(t, st)); n > 150 {
		t.Fatalf("%d messages left although all are done", n)
	}
	wantWaiting(t, st, 0)

	// The stream goes on as before.
	dispatchMany(t, st, 3) // under run IDs that are dispatched already
	for i := range 3 {
		if _, err := st.DispatchWork(ctx, "normal", &types.WorkMessage{RunID: fmt.Sprint("new", i), JobID: "j", TaskType: "t"}); err != nil {
			t.Fatal(err)
		}
	}
	wantWaiting(t, st, 3)
	if n := len(take(t, st, store.ConsumerGroup, 10)); n != 3 {
		t.Fatalf("took %d messages, want the 3 new ones", n)
	}
}

// Another consumer group of the user's that is behind gets to see all messages.
func TestTrimWorkStream_WaitsForEveryGroup(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st})
	rdb := st.Client()
	if err := rdb.Do(ctx, rdb.B().XgroupCreate().Key(store.WorkStreamKey(st.Prefix(), "normal")).Group("other").Id("0").Build()).Error(); err != nil {
		t.Fatal(err)
	}

	dispatchMany(t, st, 500)
	ack(t, st, store.ConsumerGroup, take(t, st, store.ConsumerGroup, 500))
	s.trimWorkStreams(ctx)
	if n := len(streamIDs(t, st)); n != 500 {
		t.Fatalf("%d of 500 messages left although a group has taken none", n)
	}
	ack(t, st, "other", take(t, st, "other", 500))
	s.trimWorkStreams(ctx)
	if n := len(streamIDs(t, st)); n > 150 {
		t.Fatalf("%d messages left although all groups are done", n)
	}
}

// More than Redis removes at once.
func TestTrimWorkStream_LongStream(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	s := New(Config{Store: st})
	rdb, key := st.Client(), store.WorkStreamKey(st.Prefix(), "normal")
	const n = 25_000
	for range n / 1000 {
		script := `for i = 1, tonumber(ARGV[1]) do redis.call('XADD', KEYS[1], '*', 'run_id', 'x') end return 1`
		if err := rdb.Do(ctx, rdb.B().Eval().Script(script).Numkeys(1).Key(key).Arg("1000").Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	for taken := 0; taken < n; taken += 5000 {
		ack(t, st, store.ConsumerGroup, take(t, st, store.ConsumerGroup, 5000))
	}
	s.trimWorkStreams(ctx)
	if left := len(streamIDs(t, st)); left > 150 {
		t.Fatalf("%d of %d messages left although all are done", left, n)
	}
}

func TestTrimWorkStream_NoStream(t *testing.T) {
	st := newTestStore(t)
	ctx := context.Background()
	rdb := st.Client()
	rdb.Do(ctx, rdb.B().Del().Key(store.WorkStreamKey(st.Prefix(), "normal")).Build())
	if n, err := st.TrimWorkStream(ctx, "normal"); n != 0 || err != nil {
		t.Fatalf("trimmed %d (%v), want nothing", n, err)
	}
}
