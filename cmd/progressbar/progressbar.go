package progressbar

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
)

const barWidth = 20

func Progress(ctx context.Context, total int64, pch chan int64, startTime time.Time, interval time.Duration) {
	var sent int64
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("\033[2K\r")
			return
		case <-ticker.C:
			printProgress(sent, total, startTime)
		case size := <-pch:
			sent += size
			printProgress(sent, total, startTime)
			if sent == total {
				fmt.Printf("\n")
				return
			}
		}
	}
}

func printProgress(sent, total int64, startTime time.Time) {
	elapsed := time.Since(startTime).Seconds()
	pct := float64(sent*100) / float64(total)

	var bar string
	{
		filled := int(pct / (100.0 / barWidth))
		if filled > barWidth {
			filled = barWidth
		}
		if filled > 0 && filled < barWidth {
			bar = strings.Repeat("=", filled-1) + ">" + strings.Repeat("-", barWidth-filled)
		} else if filled == barWidth {
			bar = strings.Repeat("=", barWidth)
		} else {
			bar = strings.Repeat("-", barWidth)
		}
	}

	var speedStr string
	var etaStr string
	if elapsed > 0 && sent > 0 {
		speed := float64(sent) / elapsed
		speedStr = fmt.Sprintf("%s/s", humanize.IBytes(uint64(speed)))
		remaining := float64(total-sent) / speed
		eta := time.Duration(remaining) * time.Second
		if eta < time.Second {
			etaStr = "0s"
		} else {
			etaStr = eta.Round(time.Second).String()
		}
	} else {
		speedStr = "?/s"
		etaStr = "?"
	}

	fmt.Printf("\033[2K\r%s / %s [%s]  %.0f%%  %s  %s",
		humanize.IBytes(uint64(sent)),
		humanize.IBytes(uint64(total)),
		bar,
		pct,
		speedStr,
		etaStr,
	)
}
