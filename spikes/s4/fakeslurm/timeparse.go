package fakeslurm

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// parseTimeLimit parses an sbatch --time value into minutes. Slurm accepts
// "minutes", "minutes:seconds", "hours:minutes:seconds", "days-hours",
// "days-hours:minutes" and "days-hours:minutes:seconds"; leftover seconds
// round up to a minute. "UNLIMITED" and "infinite" give 0.
func parseTimeLimit(s string) (int, error) {
	orig := s
	if l := strings.ToLower(s); l == "unlimited" || l == "infinite" {
		return 0, nil
	}
	days := 0
	hasDays := false
	if d, rest, ok := strings.Cut(s, "-"); ok {
		n, err := strconv.Atoi(d)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid time specification %q", orig)
		}
		days, hasDays, s = n, true, rest
	}
	var nums []int
	for _, f := range strings.Split(s, ":") {
		n, err := strconv.Atoi(f)
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid time specification %q", orig)
		}
		nums = append(nums, n)
	}
	var h, m, sec int
	switch {
	case hasDays && len(nums) == 1:
		h = nums[0]
	case hasDays && len(nums) == 2:
		h, m = nums[0], nums[1]
	case hasDays && len(nums) == 3:
		h, m, sec = nums[0], nums[1], nums[2]
	case !hasDays && len(nums) == 1:
		m = nums[0]
	case !hasDays && len(nums) == 2:
		m, sec = nums[0], nums[1]
	case !hasDays && len(nums) == 3:
		h, m, sec = nums[0], nums[1], nums[2]
	default:
		return 0, fmt.Errorf("invalid time specification %q", orig)
	}
	total := days*24*60 + h*60 + m
	if sec > 0 {
		total++
	}
	return total, nil
}

var relTimeRE = regexp.MustCompile(`^now(?:([+-])(\d+)([A-Za-z]*))?$`)

var unitDur = map[string]time.Duration{
	"": time.Second, "second": time.Second, "seconds": time.Second, "sec": time.Second, "secs": time.Second,
	"minute": time.Minute, "minutes": time.Minute, "min": time.Minute, "mins": time.Minute,
	"hour": time.Hour, "hours": time.Hour,
	"day": 24 * time.Hour, "days": 24 * time.Hour,
	"week": 7 * 24 * time.Hour, "weeks": 7 * 24 * time.Hour,
}

// parseSlurmTime parses a sacct -S/-E value: now, now-N, now+N, now-N<unit>,
// YYYY-MM-DDTHH:MM:SS or YYYY-MM-DD, the latter read in loc.
func parseSlurmTime(s string, now time.Time, loc *time.Location) (time.Time, error) {
	if m := relTimeRE.FindStringSubmatch(strings.ToLower(s)); m != nil {
		if m[1] == "" {
			return now, nil
		}
		n, _ := strconv.Atoi(m[2])
		unit, ok := unitDur[m[3]]
		if !ok {
			return time.Time{}, fmt.Errorf("invalid time unit in %q", s)
		}
		d := time.Duration(n) * unit
		if m[1] == "-" {
			d = -d
		}
		return now.Add(d), nil
	}
	for _, layout := range []string{"2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, loc); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid time specification %q", s)
}

// midnight is 00:00:00 of now's date in loc.
func midnight(now time.Time, loc *time.Location) time.Time {
	y, mo, d := now.In(loc).Date()
	return time.Date(y, mo, d, 0, 0, 0, 0, loc)
}
