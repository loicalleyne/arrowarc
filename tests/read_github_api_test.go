// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	github "github.com/arrowarc/arrowarc/integrations/api/github"
	helper "github.com/arrowarc/arrowarc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestGitHubRepoAPIStream(t *testing.T) {
	if os.Getenv("GITHUB_TOKEN") == "" {
		t.Skip("Skipping test as GITHUB_TOKEN is not set")
	}

	// List of repositories to fetch data for
	repos := []string{
		"torvalds/linux",
		"apple/swift",
		"golang/go",
		"tfmv/arrowarc",
	}

	// Retrieve the GitHub OAuth token from the environment variable
	githubToken := os.Getenv("GITHUB_TOKEN")
	if githubToken == "" {
		t.Fatal("GITHUB_TOKEN environment variable is not set")
	}

	tests := []struct {
		repos       []string
		description string
	}{
		{
			repos:       repos,
			description: "Fetch GitHub repos information for multiple repositories",
		},
	}

	for _, test := range tests {
		test := test // capture range variable
		t.Run(test.description, func(t *testing.T) {

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			client := github.NewGitHubClient(ctx, githubToken)
			assert.NotNil(t, client, "GitHub client should not be nil")

			reader, err := github.NewGitHubReader(ctx, &github.GitHubReadOptions{
				Repos: test.repos,
				Token: githubToken,
			})
			assert.NoError(t, err, "Error should be nil when creating GitHub API reader")

			// Read records from the GitHub API

			var recordsRead int
			for {
				record, err := reader.Read()
				if err == io.EOF {
					break
				}
				assert.NoError(t, err, "Error should be nil when reading from GitHub API stream")
				if record == nil {
					break
				}
				helper.PrintRecordBatch(record)
				recordsRead += int(record.NumRows())
				record.Release()
			}

			assert.Greater(t, recordsRead, 0, "Should have read at least one record")
		})
	}
}
