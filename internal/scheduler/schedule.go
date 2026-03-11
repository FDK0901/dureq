package scheduler

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/FDK0901/dureq/pkg/types"
	"github.com/robfig/cron/v3"
)

// NextRunTime calculates the next run time for a schedule relative to `after`.
// For first-time calculation, pass time.Now() as `after`.
func NextRunTime(s types.Schedule, after time.Time) (time.Time, error) {
	loc := time.UTC
	if s.Timezone != "" {
		var err error
		loc, err = time.LoadLocation(s.Timezone)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid timezone %q: %w", s.Timezone, err)
		}
	}

	after = after.In(loc)

	// If StartsAt is in the future and we haven't reached it yet, the first
	// run should be at or after StartsAt.
	if s.StartsAt != nil && s.StartsAt.After(after) {
		after = s.StartsAt.Add(-time.Second) // slightly before so the first check can match
	}

	var next time.Time
	var err error

	switch s.Type {
	case types.ScheduleImmediate:
		return time.Now().In(loc), nil

	case types.ScheduleOneTime:
		if s.RunAt == nil {
			return time.Time{}, fmt.Errorf("run_at required for ONE_TIME")
		}
		runAt := s.RunAt.In(loc)
		if runAt.After(after) {
			return runAt, nil
		}
		return time.Time{}, fmt.Errorf("ONE_TIME run_at is in the past")

	case types.ScheduleDuration:
		if s.Interval == nil {
			return time.Time{}, fmt.Errorf("interval required for DURATION")
		}
		next = after.Add(s.Interval.Std())
		// For first run with StartsAt, start at StartsAt itself.
		if s.StartsAt != nil && s.StartsAt.After(after) {
			next = s.StartsAt.In(loc)
		}

	case types.ScheduleCron:
		if s.CronExpr == nil {
			return time.Time{}, fmt.Errorf("cron_expr required for CRON")
		}
		sched, parseErr := cron.ParseStandard(*s.CronExpr)
		if parseErr != nil {
			return time.Time{}, fmt.Errorf("invalid cron expression: %w", parseErr)
		}
		next = sched.Next(after)

	case types.ScheduleDaily:
		next, err = nextDaily(s, after, loc)
	case types.ScheduleWeekly:
		next, err = nextWeekly(s, after, loc)
	case types.ScheduleMonthly:
		next, err = nextMonthly(s, after, loc)

	default:
		return time.Time{}, fmt.Errorf("unknown schedule type: %s", s.Type)
	}

	if err != nil {
		return time.Time{}, err
	}

	// Check EndsAt boundary.
	if !next.IsZero() && s.EndsAt != nil && next.After(*s.EndsAt) {
		return time.Time{}, nil // schedule expired
	}

	// Apply jitter: add a random offset in [0, jitter) to prevent thundering herd.
	if !next.IsZero() && s.Jitter != nil && s.Jitter.Std() > 0 {
		jitter := rand.Int64N(int64(s.Jitter.Std()))
		next = next.Add(time.Duration(jitter))
	}

	return next, nil
}

// IsExpired returns true if a schedule's EndsAt has passed.
func IsExpired(s types.Schedule, now time.Time) bool {
	return s.EndsAt != nil && now.After(*s.EndsAt)
}

// MissedFirings returns all firing times between `from` (exclusive) and `until`
// (inclusive) for the given schedule, capped at `maxFirings`. Only returns
// firings within the schedule's CatchupWindow relative to `until`.
// Returns nil if CatchupWindow is not set.
func MissedFirings(s types.Schedule, from time.Time, until time.Time, maxFirings int) ([]time.Time, error) {
	if s.CatchupWindow == nil || s.CatchupWindow.Std() <= 0 {
		return nil, nil
	}

	catchupCutoff := until.Add(-s.CatchupWindow.Std())
	if from.Before(catchupCutoff) {
		from = catchupCutoff
	}

	if maxFirings <= 0 {
		maxFirings = 10
	}

	var firings []time.Time
	cursor := from

	for len(firings) < maxFirings {
		next, err := NextRunTime(s, cursor)
		if err != nil || next.IsZero() {
			break
		}
		if next.After(until) {
			break
		}
		firings = append(firings, next)
		cursor = next
	}

	return firings, nil
}

// --- Daily ---

func nextDaily(s types.Schedule, after time.Time, loc *time.Location) (time.Time, error) {
	if s.RegularInterval == nil || len(s.AtTimes) == 0 {
		return time.Time{}, fmt.Errorf("daily requires regular_interval and at_times")
	}
	interval := int(*s.RegularInterval)
	atTimes := sortAtTimes(s.AtTimes)

	// Anchor-based calculation: use StartsAt (or midnight of `after`) as the
	// epoch, then compute the next aligned day boundary to eliminate drift.
	anchor := time.Date(after.Year(), after.Month(), after.Day(), 0, 0, 0, 0, loc)
	if s.StartsAt != nil {
		anchor = time.Date(s.StartsAt.In(loc).Year(), s.StartsAt.In(loc).Month(), s.StartsAt.In(loc).Day(), 0, 0, 0, 0, loc)
	}

	// dayOffset = number of whole days from anchor to `after`'s midnight.
	afterMidnight := time.Date(after.Year(), after.Month(), after.Day(), 0, 0, 0, 0, loc)
	dayOffset := int(afterMidnight.Sub(anchor).Hours() / 24)

	// For interval=1, check remaining times today first.
	if interval <= 1 {
		for _, at := range atTimes {
			candidate := time.Date(after.Year(), after.Month(), after.Day(),
				int(at.Hour), int(at.Minute), int(at.Second), 0, loc)
			if candidate.After(after) {
				return candidate, nil
			}
		}
		// No remaining time today; next day.
		nextDay := time.Date(after.Year(), after.Month(), after.Day()+1, 0, 0, 0, 0, loc)
		return time.Date(nextDay.Year(), nextDay.Month(), nextDay.Day(),
			int(atTimes[0].Hour), int(atTimes[0].Minute), int(atTimes[0].Second), 0, loc), nil
	}

	// For interval > 1, find the next interval-aligned day relative to anchor.
	// Check if today is an aligned day (dayOffset % interval == 0) and has remaining atTimes.
	if dayOffset >= 0 && dayOffset%interval == 0 {
		for _, at := range atTimes {
			candidate := time.Date(after.Year(), after.Month(), after.Day(),
				int(at.Hour), int(at.Minute), int(at.Second), 0, loc)
			if candidate.After(after) {
				return candidate, nil
			}
		}
	}

	// Jump to next aligned day: smallest N*interval > dayOffset.
	var n int
	if dayOffset < 0 {
		n = 0 // anchor day is in the future
	} else {
		n = ((dayOffset / interval) + 1) * interval
	}
	nextAligned := anchor.AddDate(0, 0, n)
	return time.Date(nextAligned.Year(), nextAligned.Month(), nextAligned.Day(),
		int(atTimes[0].Hour), int(atTimes[0].Minute), int(atTimes[0].Second), 0, loc), nil
}

// --- Weekly ---

func nextWeekly(s types.Schedule, after time.Time, loc *time.Location) (time.Time, error) {
	if s.RegularInterval == nil || len(s.AtTimes) == 0 || len(s.IncludedDays) == 0 {
		return time.Time{}, fmt.Errorf("weekly requires regular_interval, at_times, and included_days")
	}
	interval := int(*s.RegularInterval)
	atTimes := sortAtTimes(s.AtTimes)
	weekdays := sortInts(s.IncludedDays)

	// First pass: remaining times in the current week.
	if next := nextWeekDayAtTime(after, weekdays, atTimes, loc, true); !next.IsZero() {
		// For interval > 1, only allow same-week matches if the current week is aligned.
		if interval <= 1 || isAlignedWeek(s, after, interval, loc) {
			return next, nil
		}
	}

	// Anchor-based: find start of the next aligned week.
	anchor := startOfWeek(after, loc)
	if s.StartsAt != nil {
		anchor = startOfWeek(s.StartsAt.In(loc), loc)
	}
	current := startOfWeek(after, loc)
	weeksSinceAnchor := int(current.Sub(anchor).Hours()/(24*7)) + 1
	if weeksSinceAnchor < 1 {
		weeksSinceAnchor = 1
	}
	n := ((weeksSinceAnchor + interval - 1) / interval) * interval
	nextWeekStart := anchor.AddDate(0, 0, n*7)

	if next := nextWeekDayAtTime(nextWeekStart, weekdays, atTimes, loc, false); !next.IsZero() {
		return next, nil
	}

	return time.Time{}, fmt.Errorf("weekly: no valid next run time found")
}

// isAlignedWeek checks if `t` falls on a week that is interval-aligned to the anchor.
func isAlignedWeek(s types.Schedule, t time.Time, interval int, loc *time.Location) bool {
	anchor := startOfWeek(t, loc)
	if s.StartsAt != nil {
		anchor = startOfWeek(s.StartsAt.In(loc), loc)
	}
	current := startOfWeek(t, loc)
	weeks := int(current.Sub(anchor).Hours() / (24 * 7))
	return weeks%interval == 0
}

// startOfWeek returns midnight of the Sunday that starts the week containing t.
func startOfWeek(t time.Time, loc *time.Location) time.Time {
	wd := int(t.Weekday())
	return time.Date(t.Year(), t.Month(), t.Day()-wd, 0, 0, 0, 0, loc)
}

func nextWeekDayAtTime(from time.Time, weekdays []int, atTimes []types.AtTime, loc *time.Location, firstPass bool) time.Time {
	currentWeekday := int(from.Weekday())
	for _, wd := range weekdays {
		if wd >= currentWeekday {
			diff := wd - currentWeekday
			for _, at := range atTimes {
				candidate := time.Date(from.Year(), from.Month(), from.Day()+diff,
					int(at.Hour), int(at.Minute), int(at.Second), 0, loc)
				if firstPass && candidate.After(from) {
					return candidate
				} else if !firstPass && !candidate.Before(from) {
					return candidate
				}
			}
		}
	}
	return time.Time{}
}

// --- Monthly ---

func nextMonthly(s types.Schedule, after time.Time, loc *time.Location) (time.Time, error) {
	if s.RegularInterval == nil || len(s.AtTimes) == 0 || len(s.IncludedDays) == 0 {
		return time.Time{}, fmt.Errorf("monthly requires regular_interval, at_times, and included_days")
	}
	interval := int(*s.RegularInterval)
	atTimes := sortAtTimes(s.AtTimes)
	days := sortInts(s.IncludedDays)

	// Anchor-based: determine month alignment from StartsAt.
	anchorYear, anchorMonth := after.Year(), after.Month()
	if s.StartsAt != nil {
		a := s.StartsAt.In(loc)
		anchorYear, anchorMonth = a.Year(), a.Month()
	}

	// Check current month if it's aligned (or interval=1).
	monthsFromAnchor := max((after.Year()-anchorYear)*12+int(after.Month()-anchorMonth), 0)
	if interval <= 1 || monthsFromAnchor%interval == 0 {
		if next := nextMonthDayAtTime(after, days, atTimes, loc, true); !next.IsZero() {
			return next, nil
		}
	}

	// Find next aligned month.
	n := ((monthsFromAnchor / interval) + 1) * interval
	from := time.Date(anchorYear, anchorMonth+time.Month(n), 1, 0, 0, 0, 0, loc)

	// Cap at 12 iterations to avoid infinite loops for unreachable days.
	for i := 0; i < 12; i++ {
		if next := nextMonthDayAtTime(from, days, atTimes, loc, false); !next.IsZero() {
			return next, nil
		}
		n += interval
		from = time.Date(anchorYear, anchorMonth+time.Month(n), 1, 0, 0, 0, 0, loc)
	}

	return time.Time{}, fmt.Errorf("monthly: no valid next run time found within 12 iterations")
}

func nextMonthDayAtTime(from time.Time, days []int, atTimes []types.AtTime, loc *time.Location, firstPass bool) time.Time {
	for _, day := range days {
		if day >= from.Day() {
			for _, at := range atTimes {
				candidate := time.Date(from.Year(), from.Month(), day,
					int(at.Hour), int(at.Minute), int(at.Second), 0, loc)

				// Skip if the day doesn't exist in this month (e.g., Feb 31).
				if candidate.Month() != from.Month() {
					continue
				}

				if firstPass && candidate.After(from) {
					return candidate
				} else if !firstPass && !candidate.Before(from) {
					return candidate
				}
			}
		}
	}
	return time.Time{}
}

// --- Helpers ---

func sortAtTimes(at []types.AtTime) []types.AtTime {
	sorted := make([]types.AtTime, len(at))
	copy(sorted, at)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Hour != sorted[j].Hour {
			return sorted[i].Hour < sorted[j].Hour
		}
		if sorted[i].Minute != sorted[j].Minute {
			return sorted[i].Minute < sorted[j].Minute
		}
		return sorted[i].Second < sorted[j].Second
	})
	return sorted
}

func sortInts(s []int) []int {
	sorted := make([]int, len(s))
	copy(sorted, s)
	sort.Ints(sorted)
	return sorted
}
