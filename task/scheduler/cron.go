package scheduler

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type cronField struct {
	values map[int]bool
	wild   bool
}

func field(input string, min, max int, names map[string]int) (cronField, error) {
	f := cronField{values: map[int]bool{}, wild: strings.HasPrefix(input, "*")}
	number := func(v string) (int, error) {
		if n, ok := names[strings.ToUpper(v)]; ok {
			return n, nil
		}
		return strconv.Atoi(v)
	}
	for _, part := range strings.Split(input, ",") {
		bits := strings.Split(part, "/")
		if len(bits) > 2 || bits[0] == "" {
			return f, fmt.Errorf("invalid cron field")
		}
		step := 1
		if len(bits) == 2 {
			var e error
			step, e = strconv.Atoi(bits[1])
			if e != nil || step < 1 || step > max-min+1 {
				return f, fmt.Errorf("invalid cron step")
			}
		}
		lo, hi := min, max
		if bits[0] != "*" {
			interval := strings.Split(bits[0], "-")
			if len(interval) > 2 {
				return f, fmt.Errorf("invalid cron range")
			}
			var e error
			lo, e = number(interval[0])
			if e != nil {
				return f, e
			}
			hi = lo
			if len(interval) == 2 {
				hi, e = number(interval[1])
				if e != nil {
					return f, e
				}
			} else if len(bits) == 2 {
				hi = max
			}
		}
		if lo < min || hi > max || lo > hi {
			return f, fmt.Errorf("cron value outside %d-%d", min, max)
		}
		for n := lo; n <= hi; n += step {
			f.values[n] = true
		}
	}
	return f, nil
}
func Next(expr, zone string, after time.Time) (time.Time, error) {
	if zone == "" {
		zone = "UTC"
	}
	loc, e := time.LoadLocation(zone)
	if e != nil {
		return time.Time{}, fmt.Errorf("invalid timezone")
	}
	parts := strings.Fields(expr)
	if len(parts) != 5 {
		return time.Time{}, fmt.Errorf("cron requires five fields: minute hour day month weekday")
	}
	months := map[string]int{"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6, "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
	days := map[string]int{"SUN": 0, "MON": 1, "TUE": 2, "WED": 3, "THU": 4, "FRI": 5, "SAT": 6}
	bounds := [][2]int{{0, 59}, {0, 23}, {1, 31}, {1, 12}, {0, 7}}
	fields := make([]cronField, 5)
	for i, p := range parts {
		var names map[string]int
		if i == 3 {
			names = months
		}
		if i == 4 {
			names = days
		}
		fields[i], e = field(p, bounds[i][0], bounds[i][1], names)
		if e != nil {
			return time.Time{}, e
		}
	}
	if fields[4].values[7] {
		fields[4].values[0] = true
	}
	candidate := after.UTC().Truncate(time.Minute).Add(time.Minute)
	end := candidate.AddDate(8, 0, 0)
	for candidate.Before(end) {
		v := candidate.In(loc)
		dom := fields[2].values[v.Day()]
		dow := fields[4].values[int(v.Weekday())]
		day := dom && dow
		if !fields[2].wild && !fields[4].wild {
			day = dom || dow
		}
		if fields[0].values[v.Minute()] && fields[1].values[v.Hour()] && day && fields[3].values[int(v.Month())] {
			return candidate, nil
		}
		candidate = candidate.Add(time.Minute)
	}
	return time.Time{}, fmt.Errorf("cron has no occurrence in the next eight years")
}
