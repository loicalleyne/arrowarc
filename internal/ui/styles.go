package ui

import "github.com/charmbracelet/lipgloss"

var (
	TitleStyle = lipgloss.NewStyle().
			MarginLeft(2).
			Foreground(lipgloss.Color("#FFFDF5")).
			Background(lipgloss.Color("#25A065")).
			Padding(0, 1)

	PaginationStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#25A065"))

	HelpStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("#25A065"))

	DocStyle = lipgloss.NewStyle().
			Margin(1, 2)
)
