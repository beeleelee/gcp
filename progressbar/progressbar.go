package progressbar

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dustin/go-humanize"
	"golang.org/x/term"
)

func getTermWidth() int {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 80
	}
	return w
}

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

	sentStr := humanize.IBytes(uint64(sent))
	totalStr := humanize.IBytes(uint64(total))

	var speedStr string
	var etaStr string
	var elapsedStr string

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
		elapsedStr = fmt.Sprintf("%.1fs", elapsed)
	} else {
		speedStr = "?/s"
		etaStr = "?"
		elapsedStr = "0.0s"
	}

	prefix := sentStr + " / " + totalStr + " ["
	suffix := fmt.Sprintf("]  %.0f%%  %s  %s  %s", pct, speedStr, etaStr, elapsedStr)

	bw := getTermWidth() - len(prefix) - len(suffix)
	if bw < 10 {
		bw = 10
	}

	var bar string
	{
		filled := int(pct / (100.0 / float64(bw)))
		if filled > bw {
			filled = bw
		}
		if filled > 0 && filled < bw {
			bar = strings.Repeat("=", filled-1) + ">" + strings.Repeat("-", bw-filled)
		} else if filled == bw {
			bar = strings.Repeat("=", bw)
		} else {
			bar = strings.Repeat("-", bw)
		}
	}

	fmt.Printf("\033[2K\r%s%s%s", prefix, bar, suffix)
}
