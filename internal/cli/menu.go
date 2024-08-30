package cli

import (
	"fmt"

	"github.com/arrowarc/arrowarc/internal/ui"
	"github.com/charmbracelet/bubbles/list"
	tea "github.com/charmbracelet/bubbletea"
)

type item struct {
	title, desc string
}

func (i item) Title() string       { return i.title }
func (i item) Description() string { return i.desc }
func (i item) FilterValue() string { return i.title }

type model struct {
	list     list.Model
	choice   string
	quitting bool
}

func initialModel() model {
	items := []list.Item{
		item{title: "Generate Parquet", desc: "Generate a new Parquet file"},
		item{title: "Parquet to CSV", desc: "Convert Parquet to CSV"},
		item{title: "CSV to Parquet", desc: "Convert CSV to Parquet"},
		item{title: "Parquet to JSON", desc: "Convert Parquet to JSON"},
		item{title: "Rewrite Parquet", desc: "Rewrite a Parquet file"},
		item{title: "Run Flight Tests", desc: "Execute Arrow Flight tests"},
		item{title: "Avro to Parquet", desc: "Convert Avro to Parquet"},
		item{title: "Quit", desc: "Exit the application"},
	}

	l := list.New(items, list.NewDefaultDelegate(), 0, 0)
	l.Title = "ArrowArc Menu"
	l.SetShowStatusBar(false)
	l.SetFilteringEnabled(false)
	l.Styles.Title = ui.TitleStyle
	l.Styles.PaginationStyle = ui.PaginationStyle
	l.Styles.HelpStyle = ui.HelpStyle

	return model{list: l}
}

func (m model) Init() tea.Cmd {
	return nil
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c", "q":
			m.quitting = true
			return m, tea.Quit
		case "enter":
			i, ok := m.list.SelectedItem().(item)
			if ok {
				m.choice = i.title
				return m, tea.Quit
			}
		}
	case tea.WindowSizeMsg:
		h, v := ui.DocStyle.GetFrameSize()
		m.list.SetSize(msg.Width-h, msg.Height-v)
	}

	var cmd tea.Cmd
	m.list, cmd = m.list.Update(msg)
	return m, cmd
}

func (m model) View() string {
	if m.quitting {
		return "Goodbye!\n"
	}
	return ui.DocStyle.Render(m.list.View() + "\n(press q to quit)")
}

func RunMenu() error {
	for {
		p := tea.NewProgram(initialModel())
		m, err := p.Run()
		if err != nil {
			return fmt.Errorf("error running menu: %w", err)
		}

		if m, ok := m.(model); ok {
			if m.quitting {
				return nil
			}
			if m.choice == "Quit" {
				fmt.Println("Goodbye!")
				return nil
			}
			if m.choice != "" {
				err := ExecuteCommand(m.choice)
				if err != nil {
					fmt.Printf("Error executing command: %v\n", err)
				}
				fmt.Println("Press Enter to return to the menu...")
				fmt.Scanln() // Wait for user to press Enter
			}
		}
	}
}
