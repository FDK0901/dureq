package teamodel

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/FDK0901/dureq/internal/api"
	"github.com/FDK0901/dureq/pkg/types"
	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

// Page identifies the active view.
type page int

const (
	pageDashboard page = iota
	pageJobs
	pageWorkflows
	pageBatches
	pageRuns
	pageNodes
	pageSchedules
	pageQueues
	pageDLQ
	pageJobDetail
	pageWorkflowDetail
	pageBatchDetail
)

var pageNames = []string{"Dashboard", "Jobs", "Workflows", "Batches", "Runs", "Nodes", "Schedules", "Queues", "DLQ"}

// model is the top-level Bubbletea model.
type model struct {
	api     *api.HTTPApiClient
	page    page
	width   int
	height  int
	spinner spinner.Model
	loading bool
	err     error

	// Data.
	stats     *api.StatsResponse
	jobs      []*types.Job
	nodes     []*types.NodeInfo
	schedules []*types.ScheduleEntry
	dlq       []json.RawMessage
	detail    *types.Job
	workflows []*types.WorkflowInstance
	batches   []*types.BatchInstance
	runs      []*types.JobRun
	queues    []api.QueueInfo

	// Detail views.
	workflowDetail *types.WorkflowInstance
	batchDetail    *types.BatchInstance

	// List state.
	cursor         int
	jobFilter      string // status filter for jobs
	workflowFilter string
	batchFilter    string
	runFilter      string
	statusMsg      string
	statusExpiry   time.Time

	// Refresh.
	refreshInterval time.Duration
}

func InitialModel(api *api.HTTPApiClient, refreshInterval time.Duration) model {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))

	return model{
		api:             api,
		page:            pageDashboard,
		spinner:         s,
		loading:         true,
		refreshInterval: refreshInterval,
	}
}

// --- Messages ---

type tickMsg time.Time
type dataMsg struct {
	stats     *api.StatsResponse
	jobs      []*types.Job
	nodes     []*types.NodeInfo
	schedules []*types.ScheduleEntry
	dlq       []json.RawMessage
	workflows []*types.WorkflowInstance
	batches   []*types.BatchInstance
	runs      []*types.JobRun
	queues    []api.QueueInfo
	err       error
}
type detailMsg struct {
	job *types.Job
	err error
}
type workflowDetailMsg struct {
	wf  *types.WorkflowInstance
	err error
}
type batchDetailMsg struct {
	batch *types.BatchInstance
	err   error
}
type actionMsg struct {
	msg string
	err error
}

// --- Commands ---

func (m model) fetchData() tea.Msg {
	stats, err := m.api.GetStats()
	if err != nil {
		return dataMsg{err: err}
	}
	jobs, _ := m.api.ListJobs(m.jobFilter)
	nodes, _ := m.api.ListNodes()
	schedules, _ := m.api.ListSchedules()
	dlq, _ := m.api.ListDLQ(50)
	workflows, _ := m.api.ListWorkflows(m.workflowFilter)
	batches, _ := m.api.ListBatches(m.batchFilter)
	runs, _ := m.api.ListHistoryRuns(m.runFilter, "")
	queues, _ := m.api.ListQueues()

	return dataMsg{
		stats:     stats,
		jobs:      jobs,
		nodes:     nodes,
		schedules: schedules,
		dlq:       dlq,
		workflows: workflows,
		batches:   batches,
		runs:      runs,
		queues:    queues,
	}
}

func (m model) fetchDetail(jobID string) tea.Cmd {
	return func() tea.Msg {
		job, err := m.api.GetJob(jobID)
		return detailMsg{job: job, err: err}
	}
}

func (m model) fetchWorkflowDetail(id string) tea.Cmd {
	return func() tea.Msg {
		wf, err := m.api.GetWorkflow(id)
		return workflowDetailMsg{wf: wf, err: err}
	}
}

func (m model) fetchBatchDetail(id string) tea.Cmd {
	return func() tea.Msg {
		b, err := m.api.GetBatch(id)
		return batchDetailMsg{batch: b, err: err}
	}
}

func (m model) doCancel(jobID string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.CancelJob(jobID); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Cancelled job %s", jobID)}
	}
}

func (m model) doRetry(jobID string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.RetryJob(jobID); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Retried job %s", jobID)}
	}
}

func (m model) doCancelWorkflow(id string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.CancelWorkflow(id); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Cancelled workflow %s", id)}
	}
}

func (m model) doRetryWorkflow(id string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.RetryWorkflow(id); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Retried workflow %s", id)}
	}
}

func (m model) doCancelBatch(id string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.CancelBatch(id); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Cancelled batch %s", id)}
	}
}

func (m model) doRetryBatch(id string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.RetryBatch(id); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Retried batch %s", id)}
	}
}

func (m model) doPauseQueue(tierName string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.PauseQueue(tierName); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Paused queue %s", tierName)}
	}
}

func (m model) doResumeQueue(tierName string) tea.Cmd {
	return func() tea.Msg {
		if err := m.api.ResumeQueue(tierName); err != nil {
			return actionMsg{err: err}
		}
		return actionMsg{msg: fmt.Sprintf("Resumed queue %s", tierName)}
	}
}

func tickCmd(d time.Duration) tea.Cmd {
	return tea.Tick(d, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

// --- Init ---

func (m model) Init() tea.Cmd {
	return tea.Batch(
		m.spinner.Tick,
		m.fetchData,
		tickCmd(m.refreshInterval),
	)
}

// --- Update ---

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case tea.KeyMsg:
		return m.handleKey(msg)

	case spinner.TickMsg:
		var cmd tea.Cmd
		m.spinner, cmd = m.spinner.Update(msg)
		return m, cmd

	case tickMsg:
		return m, tea.Batch(m.fetchData, tickCmd(m.refreshInterval))

	case dataMsg:
		m.loading = false
		if msg.err != nil {
			m.err = msg.err
			return m, nil
		}
		m.err = nil
		m.stats = msg.stats
		m.jobs = msg.jobs
		m.nodes = msg.nodes
		m.schedules = msg.schedules
		m.dlq = msg.dlq
		m.workflows = msg.workflows
		m.batches = msg.batches
		m.runs = msg.runs
		m.queues = msg.queues
		return m, nil

	case detailMsg:
		if msg.err != nil {
			m.setStatus("Error: " + msg.err.Error())
			return m, nil
		}
		m.detail = msg.job
		m.page = pageJobDetail
		return m, nil

	case workflowDetailMsg:
		if msg.err != nil {
			m.setStatus("Error: " + msg.err.Error())
			return m, nil
		}
		m.workflowDetail = msg.wf
		m.page = pageWorkflowDetail
		return m, nil

	case batchDetailMsg:
		if msg.err != nil {
			m.setStatus("Error: " + msg.err.Error())
			return m, nil
		}
		m.batchDetail = msg.batch
		m.page = pageBatchDetail
		return m, nil

	case actionMsg:
		if msg.err != nil {
			m.setStatus("Error: " + msg.err.Error())
		} else {
			m.setStatus(msg.msg)
		}
		return m, m.fetchData
	}

	return m, nil
}

func (m *model) setStatus(msg string) {
	m.statusMsg = msg
	m.statusExpiry = time.Now().Add(5 * time.Second)
}

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch {
	case key.Matches(msg, key.NewBinding(key.WithKeys("q", "ctrl+c"))):
		return m, tea.Quit

	case key.Matches(msg, key.NewBinding(key.WithKeys("1"))):
		m.page = pageDashboard
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("2"))):
		m.page = pageJobs
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("3"))):
		m.page = pageWorkflows
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("4"))):
		m.page = pageBatches
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("5"))):
		m.page = pageRuns
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("6"))):
		m.page = pageNodes
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("7"))):
		m.page = pageSchedules
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("8"))):
		m.page = pageQueues
		m.cursor = 0
	case key.Matches(msg, key.NewBinding(key.WithKeys("9"))):
		m.page = pageDLQ
		m.cursor = 0

	case key.Matches(msg, key.NewBinding(key.WithKeys("tab"))):
		if m.page >= pageJobDetail {
			// From a detail view, go back to the parent tab.
			switch m.page {
			case pageJobDetail:
				m.page = pageJobs
			case pageWorkflowDetail:
				m.page = pageWorkflows
			case pageBatchDetail:
				m.page = pageBatches
			}
		} else {
			nextPage := int(m.page) + 1
			if nextPage > int(pageDLQ) {
				nextPage = int(pageDashboard)
			}
			m.page = page(nextPage)
		}
		m.cursor = 0

	case key.Matches(msg, key.NewBinding(key.WithKeys("esc"))):
		switch m.page {
		case pageJobDetail:
			m.page = pageJobs
		case pageWorkflowDetail:
			m.page = pageWorkflows
		case pageBatchDetail:
			m.page = pageBatches
		}

	case key.Matches(msg, key.NewBinding(key.WithKeys("j", "down"))):
		m.cursor++
		m.clampCursor()

	case key.Matches(msg, key.NewBinding(key.WithKeys("k", "up"))):
		if m.cursor > 0 {
			m.cursor--
		}

	case key.Matches(msg, key.NewBinding(key.WithKeys("enter"))):
		switch m.page {
		case pageJobs:
			if len(m.jobs) > 0 && m.cursor < len(m.jobs) {
				return m, m.fetchDetail(m.jobs[m.cursor].ID)
			}
		case pageWorkflows:
			if len(m.workflows) > 0 && m.cursor < len(m.workflows) {
				return m, m.fetchWorkflowDetail(m.workflows[m.cursor].ID)
			}
		case pageBatches:
			if len(m.batches) > 0 && m.cursor < len(m.batches) {
				return m, m.fetchBatchDetail(m.batches[m.cursor].ID)
			}
		}

	case key.Matches(msg, key.NewBinding(key.WithKeys("c"))):
		switch m.page {
		case pageJobs:
			if len(m.jobs) > 0 && m.cursor < len(m.jobs) {
				job := m.jobs[m.cursor]
				if !job.Status.IsTerminal() {
					return m, m.doCancel(job.ID)
				}
			}
		case pageWorkflows:
			if len(m.workflows) > 0 && m.cursor < len(m.workflows) {
				wf := m.workflows[m.cursor]
				if !wf.Status.IsTerminal() {
					return m, m.doCancelWorkflow(wf.ID)
				}
			}
		case pageBatches:
			if len(m.batches) > 0 && m.cursor < len(m.batches) {
				b := m.batches[m.cursor]
				if !b.Status.IsTerminal() {
					return m, m.doCancelBatch(b.ID)
				}
			}
		}

	case key.Matches(msg, key.NewBinding(key.WithKeys("r"))):
		switch m.page {
		case pageJobs:
			if len(m.jobs) > 0 && m.cursor < len(m.jobs) {
				job := m.jobs[m.cursor]
				if job.Status == types.JobStatusFailed || job.Status == types.JobStatusDead {
					return m, m.doRetry(job.ID)
				}
			}
		case pageWorkflows:
			if len(m.workflows) > 0 && m.cursor < len(m.workflows) {
				wf := m.workflows[m.cursor]
				if wf.Status == types.WorkflowStatusFailed {
					return m, m.doRetryWorkflow(wf.ID)
				}
			}
		case pageBatches:
			if len(m.batches) > 0 && m.cursor < len(m.batches) {
				b := m.batches[m.cursor]
				if b.Status == types.WorkflowStatusFailed {
					return m, m.doRetryBatch(b.ID)
				}
			}
		}
		// Manual refresh on any page.
		return m, m.fetchData

	case key.Matches(msg, key.NewBinding(key.WithKeys("f"))):
		switch m.page {
		case pageJobs:
			m.cycleJobFilter()
			return m, m.fetchData
		case pageWorkflows:
			m.cycleWorkflowFilter()
			return m, m.fetchData
		case pageBatches:
			m.cycleBatchFilter()
			return m, m.fetchData
		case pageRuns:
			m.cycleRunFilter()
			return m, m.fetchData
		}

	case key.Matches(msg, key.NewBinding(key.WithKeys("p"))):
		if m.page == pageQueues && len(m.queues) > 0 && m.cursor < len(m.queues) {
			q := m.queues[m.cursor]
			if !q.Paused {
				return m, m.doPauseQueue(q.Name)
			}
		}

	case key.Matches(msg, key.NewBinding(key.WithKeys("u"))):
		if m.page == pageQueues && len(m.queues) > 0 && m.cursor < len(m.queues) {
			q := m.queues[m.cursor]
			if q.Paused {
				return m, m.doResumeQueue(q.Name)
			}
		}
	}

	return m, nil
}

func (m *model) clampCursor() {
	maxVal := 0
	switch m.page {
	case pageJobs:
		maxVal = len(m.jobs)
	case pageWorkflows:
		maxVal = len(m.workflows)
	case pageBatches:
		maxVal = len(m.batches)
	case pageRuns:
		maxVal = len(m.runs)
	case pageNodes:
		maxVal = len(m.nodes)
	case pageSchedules:
		maxVal = len(m.schedules)
	case pageQueues:
		maxVal = len(m.queues)
	case pageDLQ:
		maxVal = len(m.dlq)
	}
	if maxVal > 0 && m.cursor >= maxVal {
		m.cursor = maxVal - 1
	}
}

var (
	jobFilters      = []string{"", "pending", "scheduled", "running", "completed", "failed", "dead", "cancelled"}
	workflowFilters = []string{"", "pending", "running", "completed", "failed", "cancelled"}
	batchFilters    = []string{"", "pending", "running", "completed", "failed", "cancelled"}
	runFilters      = []string{"", "claimed", "running", "succeeded", "failed", "timed_out", "cancelled"}
)

func (m *model) cycleJobFilter() {
	m.jobFilter = cycleFilter(m.jobFilter, jobFilters)
	m.cursor = 0
}

func (m *model) cycleWorkflowFilter() {
	m.workflowFilter = cycleFilter(m.workflowFilter, workflowFilters)
	m.cursor = 0
}

func (m *model) cycleBatchFilter() {
	m.batchFilter = cycleFilter(m.batchFilter, batchFilters)
	m.cursor = 0
}

func (m *model) cycleRunFilter() {
	m.runFilter = cycleFilter(m.runFilter, runFilters)
	m.cursor = 0
}

func cycleFilter(current string, filters []string) string {
	for i, f := range filters {
		if f == current {
			return filters[(i+1)%len(filters)]
		}
	}
	return ""
}

// --- View ---

// Styles.
var (
	titleStyle     = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("39"))
	subtitleStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	selectedStyle  = lipgloss.NewStyle().Background(lipgloss.Color("236")).Bold(true)
	tabStyle       = lipgloss.NewStyle().Padding(0, 2)
	activeTabStyle = tabStyle.Foreground(lipgloss.Color("39")).Bold(true).Underline(true)
	statusOK       = lipgloss.NewStyle().Foreground(lipgloss.Color("42"))
	statusErr      = lipgloss.NewStyle().Foreground(lipgloss.Color("196"))
	statusWarn     = lipgloss.NewStyle().Foreground(lipgloss.Color("214"))
	dimStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	keyStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("205"))
)

func (m model) View() string {
	if m.width == 0 {
		return "Loading..."
	}

	var b strings.Builder

	// Header.
	b.WriteString(titleStyle.Render("dureqctl"))
	b.WriteString(dimStyle.Render(" — dureq monitoring CLI"))
	b.WriteString("\n")

	// Tabs.
	b.WriteString(m.renderTabs())
	b.WriteString("\n\n")

	// Content.
	contentHeight := m.height - 6 // header + tabs + footer
	content := m.renderPage(contentHeight)
	b.WriteString(content)

	// Fill remaining space.
	lines := strings.Count(b.String(), "\n")
	for i := lines; i < m.height-2; i++ {
		b.WriteString("\n")
	}

	// Status bar.
	if m.statusMsg != "" && time.Now().Before(m.statusExpiry) {
		b.WriteString("\n" + statusWarn.Render(m.statusMsg))
	} else {
		b.WriteString("\n")
	}

	// Footer.
	b.WriteString(m.renderFooter())

	return b.String()
}

func (m model) renderTabs() string {
	var tabs []string
	// Determine which top-level tab is highlighted.
	highlightTab := m.page
	switch m.page {
	case pageJobDetail:
		highlightTab = pageJobs
	case pageWorkflowDetail:
		highlightTab = pageWorkflows
	case pageBatchDetail:
		highlightTab = pageBatches
	}

	for i, name := range pageNames {
		label := fmt.Sprintf("%d:%s", i+1, name)
		if page(i) == highlightTab {
			tabs = append(tabs, activeTabStyle.Render(label))
		} else {
			tabs = append(tabs, tabStyle.Render(label))
		}
	}
	return strings.Join(tabs, " ")
}

func (m model) renderPage(maxLines int) string {
	if m.loading {
		return m.spinner.View() + " Loading data..."
	}
	if m.err != nil {
		return statusErr.Render("Error: "+m.err.Error()) + "\n" +
			dimStyle.Render("Check that dureqd is running and the API is accessible.")
	}

	switch m.page {
	case pageDashboard:
		return m.viewDashboard()
	case pageJobs:
		return m.viewJobs(maxLines)
	case pageWorkflows:
		return m.viewWorkflows(maxLines)
	case pageBatches:
		return m.viewBatches(maxLines)
	case pageRuns:
		return m.viewRuns(maxLines)
	case pageNodes:
		return m.viewNodes(maxLines)
	case pageSchedules:
		return m.viewSchedules(maxLines)
	case pageQueues:
		return m.viewQueues(maxLines)
	case pageDLQ:
		return m.viewDLQ(maxLines)
	case pageJobDetail:
		return m.viewJobDetail()
	case pageWorkflowDetail:
		return m.viewWorkflowDetail()
	case pageBatchDetail:
		return m.viewBatchDetail()
	}
	return ""
}

func (m model) viewDashboard() string {
	if m.stats == nil {
		return dimStyle.Render("No data")
	}

	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Cluster Overview") + "\n\n")

	b.WriteString(fmt.Sprintf("  Active Nodes:     %s\n", statusOK.Render(fmt.Sprintf("%d", m.stats.ActiveNodes))))
	b.WriteString(fmt.Sprintf("  Active Schedules: %s\n", fmt.Sprintf("%d", m.stats.ActiveSchedules)))
	b.WriteString(fmt.Sprintf("  Active Runs:      %s\n", fmt.Sprintf("%d", m.stats.ActiveRuns)))
	b.WriteString("\n")

	b.WriteString(subtitleStyle.Render("Jobs by Status") + "\n\n")
	for _, status := range []string{"pending", "scheduled", "running", "completed", "failed", "retrying", "dead", "cancelled"} {
		count := m.stats.JobCounts[status]
		style := dimStyle
		if count > 0 {
			switch status {
			case "running":
				style = statusOK
			case "failed", "dead":
				style = statusErr
			case "pending", "retrying":
				style = statusWarn
			default:
				style = lipgloss.NewStyle()
			}
		}
		b.WriteString(fmt.Sprintf("  %-12s %s\n", status, style.Render(fmt.Sprintf("%d", count))))
	}

	return b.String()
}

func (m model) viewJobs(maxLines int) string {
	var b strings.Builder

	filterLabel := "all"
	if m.jobFilter != "" {
		filterLabel = m.jobFilter
	}
	b.WriteString(subtitleStyle.Render(fmt.Sprintf("Jobs [filter: %s] (f to cycle)", filterLabel)) + "\n\n")

	if len(m.jobs) == 0 {
		b.WriteString(dimStyle.Render("  No jobs found"))
		return b.String()
	}

	// Header row.
	header := fmt.Sprintf("  %-20s %-24s %-12s %-10s %-8s %s", "ID", "TaskType", "Status", "Schedule", "Attempt", "Updated")
	b.WriteString(dimStyle.Render(header) + "\n")

	visible := maxLines - 3
	if visible < 1 {
		visible = 10
	}

	start := 0
	if m.cursor >= visible {
		start = m.cursor - visible + 1
	}

	for i := start; i < len(m.jobs) && i < start+visible; i++ {
		j := m.jobs[i]
		line := fmt.Sprintf("  %-20s %-24s %-12s %-10s %-8d %s",
			truncate(j.ID, 20),
			truncate(string(j.TaskType), 24),
			colorStatus(string(j.Status)),
			string(j.Schedule.Type),
			j.Attempt,
			timeAgo(j.UpdatedAt),
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + dimStyle.Render(fmt.Sprintf("  %d jobs total", len(m.jobs))))
	return b.String()
}

func (m model) viewWorkflows(maxLines int) string {
	var b strings.Builder

	filterLabel := "all"
	if m.workflowFilter != "" {
		filterLabel = m.workflowFilter
	}
	b.WriteString(subtitleStyle.Render(fmt.Sprintf("Workflows [filter: %s] (f to cycle)", filterLabel)) + "\n\n")

	if len(m.workflows) == 0 {
		b.WriteString(dimStyle.Render("  No workflows found"))
		return b.String()
	}

	header := fmt.Sprintf("  %-20s %-20s %-12s %-14s %-8s %s", "ID", "Name", "Status", "Tasks", "Attempt", "Updated")
	b.WriteString(dimStyle.Render(header) + "\n")

	visible := maxLines - 3
	if visible < 1 {
		visible = 10
	}

	start := 0
	if m.cursor >= visible {
		start = m.cursor - visible + 1
	}

	for i := start; i < len(m.workflows) && i < start+visible; i++ {
		wf := m.workflows[i]
		completed := 0
		total := len(wf.Tasks)
		for _, t := range wf.Tasks {
			if t.Status == types.JobStatusCompleted {
				completed++
			}
		}
		tasks := fmt.Sprintf("%d/%d", completed, total)

		line := fmt.Sprintf("  %-20s %-20s %-12s %-14s %-8d %s",
			truncate(wf.ID, 20),
			truncate(wf.WorkflowName, 20),
			colorStatus(string(wf.Status)),
			tasks,
			wf.Attempt,
			timeAgo(wf.UpdatedAt),
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + dimStyle.Render(fmt.Sprintf("  %d workflows total", len(m.workflows))))
	return b.String()
}

func (m model) viewBatches(maxLines int) string {
	var b strings.Builder

	filterLabel := "all"
	if m.batchFilter != "" {
		filterLabel = m.batchFilter
	}
	b.WriteString(subtitleStyle.Render(fmt.Sprintf("Batches [filter: %s] (f to cycle)", filterLabel)) + "\n\n")

	if len(m.batches) == 0 {
		b.WriteString(dimStyle.Render("  No batches found"))
		return b.String()
	}

	header := fmt.Sprintf("  %-20s %-20s %-12s %-18s %s", "ID", "Name", "Status", "Progress", "Created")
	b.WriteString(dimStyle.Render(header) + "\n")

	visible := maxLines - 3
	if visible < 1 {
		visible = 10
	}

	start := 0
	if m.cursor >= visible {
		start = m.cursor - visible + 1
	}

	for i := start; i < len(m.batches) && i < start+visible; i++ {
		batch := m.batches[i]
		progress := fmt.Sprintf("%d/%d", batch.CompletedItems, batch.TotalItems)
		if batch.FailedItems > 0 {
			progress += fmt.Sprintf(" (%d failed)", batch.FailedItems)
		}

		line := fmt.Sprintf("  %-20s %-20s %-12s %-18s %s",
			truncate(batch.ID, 20),
			truncate(batch.Name, 20),
			colorStatus(string(batch.Status)),
			truncate(progress, 18),
			timeAgo(batch.CreatedAt),
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + dimStyle.Render(fmt.Sprintf("  %d batches total", len(m.batches))))
	return b.String()
}

func (m model) viewRuns(maxLines int) string {
	var b strings.Builder

	filterLabel := "all"
	if m.runFilter != "" {
		filterLabel = m.runFilter
	}
	b.WriteString(subtitleStyle.Render(fmt.Sprintf("History Runs [filter: %s] (f to cycle)", filterLabel)) + "\n\n")

	if len(m.runs) == 0 {
		b.WriteString(dimStyle.Render("  No runs found"))
		return b.String()
	}

	header := fmt.Sprintf("  %-20s %-20s %-16s %-12s %-8s %s", "RunID", "JobID", "Node", "Status", "Attempt", "Duration")
	b.WriteString(dimStyle.Render(header) + "\n")

	visible := maxLines - 3
	if visible < 1 {
		visible = 10
	}

	start := 0
	if m.cursor >= visible {
		start = m.cursor - visible + 1
	}

	for i := start; i < len(m.runs) && i < start+visible; i++ {
		r := m.runs[i]
		dur := "-"
		if r.Duration > 0 {
			dur = r.Duration.String()
		}

		line := fmt.Sprintf("  %-20s %-20s %-16s %-12s %-8d %s",
			truncate(r.ID, 20),
			truncate(r.JobID, 20),
			truncate(r.NodeID, 16),
			colorStatus(string(r.Status)),
			r.Attempt,
			dur,
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + dimStyle.Render(fmt.Sprintf("  %d runs total", len(m.runs))))
	return b.String()
}

func (m model) viewNodes(maxLines int) string {
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Nodes") + "\n\n")

	if len(m.nodes) == 0 {
		b.WriteString(dimStyle.Render("  No nodes"))
		return b.String()
	}

	header := fmt.Sprintf("  %-20s %-30s %-8s %-8s %s", "NodeID", "TaskTypes", "Running", "Idle", "Last Heartbeat")
	b.WriteString(dimStyle.Render(header) + "\n")

	for i, n := range m.nodes {
		running, idle, maxC := 0, 0, 0
		if n.PoolStats != nil {
			running = n.PoolStats.RunningWorkers
			idle = n.PoolStats.IdleWorkers
			maxC = n.PoolStats.MaxConcurrency
		}

		line := fmt.Sprintf("  %-20s %-30s %d/%-5d %-8d %s",
			truncate(n.NodeID, 20),
			truncate(strings.Join(n.TaskTypes, ","), 30),
			running, maxC,
			idle,
			timeAgo(n.LastHeartbeat),
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	return b.String()
}

func (m model) viewSchedules(maxLines int) string {
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Active Schedules") + "\n\n")

	if len(m.schedules) == 0 {
		b.WriteString(dimStyle.Render("  No active schedules"))
		return b.String()
	}

	header := fmt.Sprintf("  %-20s %-12s %-12s %-12s %s", "JobID", "Type", "Overlap", "Catchup", "Next Run")
	b.WriteString(dimStyle.Render(header) + "\n")

	for i, s := range m.schedules {
		overlap := string(s.Schedule.OverlapPolicy)
		if overlap == "" {
			overlap = "-"
		}
		catchup := "-"
		if s.Schedule.CatchupWindow != nil {
			catchup = s.Schedule.CatchupWindow.Std().String()
		}
		line := fmt.Sprintf("  %-20s %-12s %-12s %-12s %s",
			truncate(s.JobID, 20),
			string(s.Schedule.Type),
			truncate(overlap, 12),
			truncate(catchup, 12),
			s.NextRunAt.Format(time.RFC3339),
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	return b.String()
}

func (m model) viewQueues(maxLines int) string {
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Queues") + "\n\n")

	if len(m.queues) == 0 {
		b.WriteString(dimStyle.Render("  No queues"))
		return b.String()
	}

	header := fmt.Sprintf("  %-20s %-8s %-10s %-10s %s", "Name", "Weight", "Batch", "Size", "Status")
	b.WriteString(dimStyle.Render(header) + "\n")

	for i, q := range m.queues {
		qStatus := statusOK.Render("active")
		if q.Paused {
			qStatus = statusWarn.Render("paused")
		}
		line := fmt.Sprintf("  %-20s %-8d %-10d %-10d %s",
			truncate(q.Name, 20),
			q.Weight,
			q.FetchBatch,
			q.Size,
			qStatus,
		)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	return b.String()
}

func (m model) viewDLQ(maxLines int) string {
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Dead Letter Queue") + "\n\n")

	if len(m.dlq) == 0 {
		b.WriteString(dimStyle.Render("  DLQ is empty"))
		return b.String()
	}

	for i, raw := range m.dlq {
		preview := truncate(string(raw), 100)
		line := fmt.Sprintf("  %d. %s", i+1, preview)
		if i == m.cursor {
			b.WriteString(selectedStyle.Render(line))
		} else {
			b.WriteString(line)
		}
		b.WriteString("\n")
	}

	b.WriteString("\n" + dimStyle.Render(fmt.Sprintf("  %d messages", len(m.dlq))))
	return b.String()
}

func (m model) viewJobDetail() string {
	if m.detail == nil {
		return dimStyle.Render("No job selected")
	}

	j := m.detail
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Job Detail") + "  " + dimStyle.Render("(esc to go back)") + "\n\n")

	b.WriteString(fmt.Sprintf("  ID:         %s\n", j.ID))
	b.WriteString(fmt.Sprintf("  TaskType:   %s\n", j.TaskType))
	b.WriteString(fmt.Sprintf("  Status:     %s\n", colorStatus(string(j.Status))))
	b.WriteString(fmt.Sprintf("  Schedule:   %s\n", string(j.Schedule.Type)))
	b.WriteString(fmt.Sprintf("  Attempt:    %d\n", j.Attempt))

	if j.LastError != nil {
		b.WriteString(fmt.Sprintf("  LastError:  %s\n", statusErr.Render(*j.LastError)))
	}
	if j.LastRunAt != nil {
		b.WriteString(fmt.Sprintf("  LastRunAt:  %s\n", j.LastRunAt.Format(time.RFC3339)))
	}
	if j.NextRunAt != nil {
		b.WriteString(fmt.Sprintf("  NextRunAt:  %s\n", j.NextRunAt.Format(time.RFC3339)))
	}
	if j.CompletedAt != nil {
		b.WriteString(fmt.Sprintf("  CompletedAt: %s\n", j.CompletedAt.Format(time.RFC3339)))
	}
	if len(j.Tags) > 0 {
		b.WriteString(fmt.Sprintf("  Tags:       %s\n", strings.Join(j.Tags, ", ")))
	}
	if j.UniqueKey != nil {
		b.WriteString(fmt.Sprintf("  UniqueKey:  %s\n", *j.UniqueKey))
	}
	if j.HeartbeatTimeout != nil {
		b.WriteString(fmt.Sprintf("  HeartbeatTimeout: %s\n", j.HeartbeatTimeout.Std()))
	}
	if j.Schedule.OverlapPolicy != "" {
		b.WriteString(fmt.Sprintf("  OverlapPolicy:    %s\n", j.Schedule.OverlapPolicy))
	}
	if j.Schedule.CatchupWindow != nil {
		b.WriteString(fmt.Sprintf("  CatchupWindow:    %s\n", j.Schedule.CatchupWindow.Std()))
	}

	b.WriteString(fmt.Sprintf("  CreatedAt:  %s\n", j.CreatedAt.Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("  UpdatedAt:  %s\n", j.UpdatedAt.Format(time.RFC3339)))

	if j.RetryPolicy != nil {
		b.WriteString("\n  RetryPolicy:\n")
		b.WriteString(fmt.Sprintf("    MaxAttempts:  %d\n", j.RetryPolicy.MaxAttempts))
		b.WriteString(fmt.Sprintf("    InitialDelay: %s\n", j.RetryPolicy.InitialDelay))
		b.WriteString(fmt.Sprintf("    MaxDelay:     %s\n", j.RetryPolicy.MaxDelay))
		b.WriteString(fmt.Sprintf("    Multiplier:   %.1f\n", j.RetryPolicy.Multiplier))
	}

	if len(j.Payload) > 0 {
		b.WriteString("\n  Payload:\n")
		// Pretty-print JSON.
		var pretty json.RawMessage
		if err := json.Unmarshal(j.Payload, &pretty); err == nil {
			formatted, err := json.MarshalIndent(pretty, "    ", "  ")
			if err == nil {
				b.WriteString("    " + string(formatted) + "\n")
			} else {
				b.WriteString("    " + string(j.Payload) + "\n")
			}
		}
	}

	return b.String()
}

func (m model) viewWorkflowDetail() string {
	if m.workflowDetail == nil {
		return dimStyle.Render("No workflow selected")
	}

	wf := m.workflowDetail
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Workflow Detail") + "  " + dimStyle.Render("(esc to go back)") + "\n\n")

	b.WriteString(fmt.Sprintf("  ID:         %s\n", wf.ID))
	b.WriteString(fmt.Sprintf("  Name:       %s\n", wf.WorkflowName))
	b.WriteString(fmt.Sprintf("  Status:     %s\n", colorStatus(string(wf.Status))))
	b.WriteString(fmt.Sprintf("  Attempt:    %d / %d\n", wf.Attempt, wf.MaxAttempts))
	b.WriteString(fmt.Sprintf("  CreatedAt:  %s\n", wf.CreatedAt.Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("  UpdatedAt:  %s\n", wf.UpdatedAt.Format(time.RFC3339)))
	if wf.CompletedAt != nil {
		b.WriteString(fmt.Sprintf("  CompletedAt: %s\n", wf.CompletedAt.Format(time.RFC3339)))
	}
	if wf.Deadline != nil {
		b.WriteString(fmt.Sprintf("  Deadline:   %s\n", wf.Deadline.Format(time.RFC3339)))
	}

	if len(wf.Tasks) > 0 {
		b.WriteString("\n  " + subtitleStyle.Render("Tasks") + "\n\n")

		// Sort task names for stable display.
		names := make([]string, 0, len(wf.Tasks))
		for name := range wf.Tasks {
			names = append(names, name)
		}
		sort.Strings(names)

		header := fmt.Sprintf("    %-20s %-20s %-12s %s", "Task", "JobID", "Status", "Error")
		b.WriteString(dimStyle.Render(header) + "\n")

		for _, name := range names {
			t := wf.Tasks[name]
			errStr := "-"
			if t.Error != nil && *t.Error != "" {
				errStr = truncate(*t.Error, 40)
			}
			b.WriteString(fmt.Sprintf("    %-20s %-20s %-12s %s\n",
				truncate(name, 20),
				truncate(t.JobID, 20),
				colorStatus(string(t.Status)),
				errStr,
			))
		}
	}

	return b.String()
}

func (m model) viewBatchDetail() string {
	if m.batchDetail == nil {
		return dimStyle.Render("No batch selected")
	}

	batch := m.batchDetail
	var b strings.Builder
	b.WriteString(subtitleStyle.Render("Batch Detail") + "  " + dimStyle.Render("(esc to go back)") + "\n\n")

	b.WriteString(fmt.Sprintf("  ID:         %s\n", batch.ID))
	b.WriteString(fmt.Sprintf("  Name:       %s\n", batch.Name))
	b.WriteString(fmt.Sprintf("  Status:     %s\n", colorStatus(string(batch.Status))))
	b.WriteString(fmt.Sprintf("  Attempt:    %d\n", batch.Attempt))
	b.WriteString(fmt.Sprintf("  CreatedAt:  %s\n", batch.CreatedAt.Format(time.RFC3339)))
	b.WriteString(fmt.Sprintf("  UpdatedAt:  %s\n", batch.UpdatedAt.Format(time.RFC3339)))
	if batch.CompletedAt != nil {
		b.WriteString(fmt.Sprintf("  CompletedAt: %s\n", batch.CompletedAt.Format(time.RFC3339)))
	}
	if batch.Deadline != nil {
		b.WriteString(fmt.Sprintf("  Deadline:   %s\n", batch.Deadline.Format(time.RFC3339)))
	}

	b.WriteString("\n  " + subtitleStyle.Render("Progress") + "\n\n")
	b.WriteString(fmt.Sprintf("  Total:     %d\n", batch.TotalItems))
	b.WriteString(fmt.Sprintf("  Completed: %s\n", statusOK.Render(fmt.Sprintf("%d", batch.CompletedItems))))
	b.WriteString(fmt.Sprintf("  Running:   %s\n", statusWarn.Render(fmt.Sprintf("%d", batch.RunningItems))))
	b.WriteString(fmt.Sprintf("  Pending:   %d\n", batch.PendingItems))
	if batch.FailedItems > 0 {
		b.WriteString(fmt.Sprintf("  Failed:    %s\n", statusErr.Render(fmt.Sprintf("%d", batch.FailedItems))))
	} else {
		b.WriteString(fmt.Sprintf("  Failed:    %d\n", batch.FailedItems))
	}

	// ASCII progress bar.
	if batch.TotalItems > 0 {
		pct := float64(batch.CompletedItems) / float64(batch.TotalItems)
		barWidth := 30
		filled := int(pct * float64(barWidth))
		if filled > barWidth {
			filled = barWidth
		}
		bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
		b.WriteString(fmt.Sprintf("\n  [%s] %.0f%%\n", bar, pct*100))
	}

	return b.String()
}

func (m model) renderFooter() string {
	switch m.page {
	case pageJobs:
		return dimStyle.Render("  j/k:navigate  enter:detail  c:cancel  r:retry  f:filter  tab:next  q:quit")
	case pageJobDetail, pageWorkflowDetail, pageBatchDetail:
		return dimStyle.Render("  esc:back  tab:next  q:quit")
	case pageWorkflows:
		return dimStyle.Render("  j/k:navigate  enter:detail  c:cancel  r:retry  f:filter  tab:next  q:quit")
	case pageBatches:
		return dimStyle.Render("  j/k:navigate  enter:detail  c:cancel  r:retry  f:filter  tab:next  q:quit")
	case pageRuns:
		return dimStyle.Render("  j/k:navigate  f:filter  tab:next  q:quit")
	case pageQueues:
		return dimStyle.Render("  j/k:navigate  p:pause  u:resume  tab:next  q:quit")
	default:
		return dimStyle.Render("  j/k:navigate  r:refresh  tab:next  1-9:page  q:quit")
	}
}

// --- Helpers ---

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

func timeAgo(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

func colorStatus(s string) string {
	switch s {
	case "running":
		return statusOK.Render(s)
	case "completed", "succeeded":
		return statusOK.Render(s)
	case "pending", "retrying", "scheduled", "claimed":
		return statusWarn.Render(s)
	case "failed", "dead", "timed_out":
		return statusErr.Render(s)
	case "cancelled":
		return dimStyle.Render(s)
	default:
		return s
	}
}
