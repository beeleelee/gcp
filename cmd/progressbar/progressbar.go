package progressbar

import (
	"context"
	"fmt"
	"time"

	"github.com/dustin/go-humanize"
)

func Progress(ctx context.Context, total int64, pch chan int64, startTime time.Time, interval time.Duration) {
	var sent int64
	ticker := time.Tick(time.Millisecond * 200)
	var timeSpan float64
	for {
		select {
		case <-ctx.Done():
			return
		case cur := <-ticker:
			timeSpan = cur.Sub(startTime).Seconds()
		case size := <-pch:
			sent += size
			fmt.Printf("\rTotal: %s Sent: %s  Completed: %.0f%% elapsed: %.2fs", humanize.IBytes(uint64(total)), humanize.IBytes(uint64(sent)), float64(sent*100)/float64(total), timeSpan)
			if sent == total {
				return
			}
		}
	}
}
