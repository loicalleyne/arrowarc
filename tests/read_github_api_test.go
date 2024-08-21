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
	"testing"
	"time"

	github "github.com/ArrowArc/ArrowArc/integrations/api/github"
	helper "github.com/ArrowArc/ArrowArc/pkg/common/utils"
	"github.com/stretchr/testify/assert"
)

func TestGitHubRepoAPIStream(t *testing.T) {
	t.Parallel() // Parallelize the top-level test

	repos := []string{
		"torvalds/linux",
		"apple/swift",
		"golang/go",
		"tfmv/dbX",
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
			t.Parallel() // Parallelize each subtest

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			recordChan, errChan := github.ReadGitHubRepoAPIStream(ctx, test.repos)

			errs := make(chan error)
			go func() {
				for err := range errChan {
					if err != nil {
						errs <- err
					}
				}
				close(errs)
			}()

			for record := range recordChan {
				assert.NotNil(t, record, "Record should not be nil")
				helper.PrintRecordBatch(record)
			}

			for err := range errs {
				assert.NoError(t, err, "Error should be nil when reading GitHub API stream")
			}
		})
	}
}
