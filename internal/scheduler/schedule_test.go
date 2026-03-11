package scheduler

import (
	"testing"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
)

func durationPtr(d time.Duration) *types.Duration {
	v := types.Duration(d)
	return &v
}

func uintPtr(v uint) *uint { return &v }
func timePtr(t time.Time) *time.Time { return &t }

func TestNextRunTime_Duration_NoJitter(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute

	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
	}

	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatalf("NextRunTime() error: %v", err)
	}

	expected := after.Add(interval)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Duration_WithJitter(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute
	jitterMax := 30 * time.Second

	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
		Jitter:   durationPtr(jitterMax),
	}

	baseExpected := after.Add(interval)

	// Run multiple iterations to verify the result is always within [base, base+jitter).
	for i := 0; i < 100; i++ {
		next, err := NextRunTime(s, after)
		if err != nil {
			t.Fatalf("NextRunTime() error on iteration %d: %v", i, err)
		}

		if next.Before(baseExpected) {
			t.Fatalf("iteration %d: next %v is before base %v", i, next, baseExpected)
		}
		upperBound := baseExpected.Add(jitterMax)
		if !next.Before(upperBound) {
			t.Fatalf("iteration %d: next %v is at or after upper bound %v", i, next, upperBound)
		}
	}
}

func TestNextRunTime_Duration_JitterProducesVariation(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 5 * time.Minute
	jitterMax := 10 * time.Second

	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
		Jitter:   durationPtr(jitterMax),
	}

	// Collect multiple results and check that not all are identical,
	// confirming jitter introduces randomness.
	results := make(map[int64]struct{})
	for i := 0; i < 50; i++ {
		next, err := NextRunTime(s, after)
		if err != nil {
			t.Fatalf("NextRunTime() error: %v", err)
		}
		results[next.UnixNano()] = struct{}{}
	}

	if len(results) < 2 {
		t.Fatal("expected jitter to produce at least 2 distinct values over 50 iterations, got 1")
	}
}

func TestNextRunTime_Duration_ZeroJitter(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	interval := 10 * time.Minute

	// Jitter of 0 should behave the same as no jitter.
	s := types.Schedule{
		Type:     types.ScheduleDuration,
		Interval: durationPtr(interval),
		Jitter:   durationPtr(0),
	}

	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatalf("NextRunTime() error: %v", err)
	}

	expected := after.Add(interval)
	if !next.Equal(expected) {
		t.Fatalf("expected %v with zero jitter, got %v", expected, next)
	}
}

// ============================================================
// Daily anchor-based tests (Fix #11)
// ============================================================

func TestNextRunTime_Daily_Interval1_SameDayRemaining(t *testing.T) {
	// 09:00에 호출, at_times=[10:00, 14:00] → 같은 날 10:00
	after := time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleDaily,
		RegularInterval: uintPtr(1),
		AtTimes:         []types.AtTime{{Hour: 10}, {Hour: 14}},
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 3, 10, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Daily_Interval1_NoRemainingToday(t *testing.T) {
	// 15:00에 호출, at_times=[10:00, 14:00] → 다음 날 10:00
	after := time.Date(2026, 3, 10, 15, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleDaily,
		RegularInterval: uintPtr(1),
		AtTimes:         []types.AtTime{{Hour: 10}, {Hour: 14}},
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 3, 11, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Daily_Interval3_AnchorBased(t *testing.T) {
	// StartsAt=Jan 1, interval=3, after=Jan 4 15:00
	// anchor=Jan 1, daysSinceAnchor=3, aligned day = Jan 4 (3%3==0)
	// 15:00 > 10:00 → next aligned = Jan 7 10:00
	startsAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 1, 4, 15, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleDaily,
		RegularInterval: uintPtr(3),
		AtTimes:         []types.AtTime{{Hour: 10}},
		StartsAt:        &startsAt,
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 1, 7, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Daily_Interval3_NoDrift(t *testing.T) {
	// Drift 시나리오: StartsAt=Jan 1, interval=3
	// 스케줄러가 지연되어 Jan 5 02:00에 호출됨 (원래 Jan 4에 fire해야 했음)
	// 올바른 anchor 계산: Jan 4는 이미 지남 → 다음 aligned = Jan 7
	startsAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 1, 5, 2, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleDaily,
		RegularInterval: uintPtr(3),
		AtTimes:         []types.AtTime{{Hour: 10}},
		StartsAt:        &startsAt,
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 1, 7, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v (drift detected!)", expected, next)
	}
}

func TestNextRunTime_Daily_Interval2_ConsecutiveCalls(t *testing.T) {
	// 연속 호출이 동일한 anchor 시퀀스를 유지하는지 확인
	// StartsAt=Jan 1, interval=2
	// Sequence: Jan 1 → Jan 3 → Jan 5 → Jan 7 ...
	startsAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleDaily,
		RegularInterval: uintPtr(2),
		AtTimes:         []types.AtTime{{Hour: 8}},
		StartsAt:        &startsAt,
	}

	expectedDays := []int{1, 3, 5, 7, 9}
	cursor := time.Date(2025, 12, 31, 23, 0, 0, 0, time.UTC) // before Jan 1

	for i, day := range expectedDays {
		next, err := NextRunTime(s, cursor)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		expected := time.Date(2026, 1, day, 8, 0, 0, 0, time.UTC)
		if !next.Equal(expected) {
			t.Fatalf("iteration %d: expected %v, got %v", i, expected, next)
		}
		cursor = next // use the returned time as new cursor
	}
}

// ============================================================
// Weekly anchor-based tests (Fix #11)
// ============================================================

func TestNextRunTime_Weekly_Interval1(t *testing.T) {
	// Monday 09:00, included_days=[1(Mon),3(Wed)], at_times=[10:00]
	// → same week, Mon 10:00
	after := time.Date(2026, 3, 9, 9, 0, 0, 0, time.UTC) // Monday
	s := types.Schedule{
		Type:            types.ScheduleWeekly,
		RegularInterval: uintPtr(1),
		AtTimes:         []types.AtTime{{Hour: 10}},
		IncludedDays:    []int{1, 3}, // Mon, Wed
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 3, 9, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Weekly_Interval2_AnchorBased(t *testing.T) {
	// StartsAt = week of Jan 5 (Mon), interval=2
	// Week 1: Jan 5-11, Week 2: Jan 12-18 (skip), Week 3: Jan 19-25 (aligned)
	// after = Jan 12 (Mon) → not aligned, skip → Jan 19 (Mon) 10:00
	startsAt := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC) // Monday
	after := time.Date(2026, 1, 12, 9, 0, 0, 0, time.UTC)   // Monday
	s := types.Schedule{
		Type:            types.ScheduleWeekly,
		RegularInterval: uintPtr(2),
		AtTimes:         []types.AtTime{{Hour: 10}},
		IncludedDays:    []int{1}, // Monday
		StartsAt:        &startsAt,
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 1, 19, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

// ============================================================
// Monthly anchor-based tests (Fix #11)
// ============================================================

func TestNextRunTime_Monthly_Interval1(t *testing.T) {
	// Mar 10 09:00, days=[15], at=[10:00] → Mar 15 10:00
	after := time.Date(2026, 3, 10, 9, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleMonthly,
		RegularInterval: uintPtr(1),
		AtTimes:         []types.AtTime{{Hour: 10}},
		IncludedDays:    []int{15},
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Monthly_Interval3_AnchorBased(t *testing.T) {
	// StartsAt=Jan 2026, interval=3, after=Feb 15
	// anchor=Jan, monthsFromAnchor=1, 1%3!=0 → skip current month
	// next aligned: Jan+3 = Apr → Apr 15 10:00
	startsAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 2, 15, 9, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleMonthly,
		RegularInterval: uintPtr(3),
		AtTimes:         []types.AtTime{{Hour: 10}},
		IncludedDays:    []int{15},
		StartsAt:        &startsAt,
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Monthly_Interval3_AlignedMonth(t *testing.T) {
	// StartsAt=Jan, interval=3, after=Apr 10 → Apr is aligned (3%3==0)
	// days=[15], → Apr 15 10:00
	startsAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	after := time.Date(2026, 4, 10, 9, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleMonthly,
		RegularInterval: uintPtr(3),
		AtTimes:         []types.AtTime{{Hour: 10}},
		IncludedDays:    []int{15},
		StartsAt:        &startsAt,
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 4, 15, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

func TestNextRunTime_Monthly_Feb31_Skips(t *testing.T) {
	// days=[31], interval=1 → Feb has no 31st, should skip to Mar 31
	after := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleMonthly,
		RegularInterval: uintPtr(1),
		AtTimes:         []types.AtTime{{Hour: 10}},
		IncludedDays:    []int{31},
	}
	next, err := NextRunTime(s, after)
	if err != nil {
		t.Fatal(err)
	}
	expected := time.Date(2026, 3, 31, 10, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %v, got %v", expected, next)
	}
}

// ============================================================
// MissedFirings test (uses anchor-based NextRunTime)
// ============================================================

func TestMissedFirings_Daily_Interval2(t *testing.T) {
	// StartsAt=Jan 1, interval=2, catchup=10 days
	// from=Jan 2, until=Jan 10 → firings at Jan 3, Jan 5, Jan 7, Jan 9
	startsAt := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	s := types.Schedule{
		Type:            types.ScheduleDaily,
		RegularInterval: uintPtr(2),
		AtTimes:         []types.AtTime{{Hour: 8}},
		StartsAt:        &startsAt,
		CatchupWindow:   durationPtr(10 * 24 * time.Hour),
	}

	from := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	until := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)

	firings, err := MissedFirings(s, from, until, 10)
	if err != nil {
		t.Fatal(err)
	}

	expectedDays := []int{3, 5, 7, 9}
	if len(firings) != len(expectedDays) {
		t.Fatalf("expected %d firings, got %d: %v", len(expectedDays), len(firings), firings)
	}
	for i, f := range firings {
		if f.Day() != expectedDays[i] {
			t.Errorf("firing %d: expected day %d, got %v", i, expectedDays[i], f)
		}
	}
}
