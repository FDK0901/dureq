package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/FDK0901/dureq/cmd/dureqctl/teamodel"
	"github.com/FDK0901/dureq/internal/api"
)

func main() {
	apiURL := flag.String("api", "http://localhost:8080", "dureq monitoring API URL")
	refresh := flag.Duration("refresh", 5*time.Second, "data refresh interval")
	flag.Parse()

	api := api.NewHTTPApiClient(*apiURL)

	p := tea.NewProgram(
		teamodel.InitialModel(api, *refresh),
		tea.WithAltScreen(),
	)

	if _, err := p.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
