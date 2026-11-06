package scheduler

import (
	"testing"
	"time"
)

func TestCronRangesStepsListsAndTimezone(t *testing.T) {
	tests := []struct{ expr, zone, after, want string }{{"*/15 * * * *", "UTC", "2026-11-04T10:01:00Z", "2026-11-04T10:15:00Z"}, {"0 9 * * MON-FRI", "Europe/Vienna", "2026-11-06T10:00:00Z", "2026-11-09T08:00:00Z"}, {"5,35 10 * * *", "UTC", "2026-11-04T10:06:00Z", "2026-11-04T10:35:00Z"}, {"0 0 29 FEB *", "UTC", "2025-03-01T00:00:00Z", "2028-02-29T00:00:00Z"}, {"0 9 13 * FRI", "UTC", "2026-11-05T10:00:00Z", "2026-11-06T09:00:00Z"}, {"30 2 * * *", "Europe/Vienna", "2026-03-28T03:00:00Z", "2026-03-30T00:30:00Z"}}
	for _, tc := range tests {
		t.Run(tc.expr+tc.after, func(t *testing.T) {
			after, _ := time.Parse(time.RFC3339, tc.after)
			got, e := Next(tc.expr, tc.zone, after)
			if e != nil || got.Format(time.RFC3339) != tc.want {
				t.Fatalf("got %v %v, want %s", got, e, tc.want)
			}
		})
	}
}
func TestCronRejectsInvalidFieldsAndTimezone(t *testing.T) {
	for _, expr := range []string{"", "* * *", "60 * * * *", "*/0 * * * *", "0 9 * * 9", "0 0 * 13 *", "5-1 * * * *"} {
		if _, e := Next(expr, "UTC", time.Now()); e == nil {
			t.Errorf("accepted %q", expr)
		}
	}
	if _, e := Next("* * * * *", "Not/AZone", time.Now()); e == nil {
		t.Error("accepted invalid timezone")
	}
}
