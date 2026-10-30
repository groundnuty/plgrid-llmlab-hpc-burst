package harness

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// selfTestRole runs the tiny child roles the harness's own tests use to check
// how output and termination are reported.
//
//	spew      writes S8_LINES lines "L<i>" and exits 0
//	longline  writes one line of S8_BYTES bytes and exits 0
//	exit3     exits at once with status 3
//	sleep     prints READY and waits until it is killed
func selfTestRole(role string) {
	switch role {
	case "spew":
		n, _ := strconv.Atoi(os.Getenv("S8_LINES"))
		for i := 0; i < n; i++ {
			fmt.Printf("L%d\n", i)
		}
	case "longline":
		n, _ := strconv.Atoi(os.Getenv("S8_BYTES"))
		fmt.Println(strings.Repeat("x", n))
	case "exit3":
		os.Exit(3)
	case "sleep":
		fmt.Println("READY")
		time.Sleep(time.Hour)
	}
}
