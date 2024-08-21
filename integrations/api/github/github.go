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

package github

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/go-github/v64/github"
	"golang.org/x/oauth2"
)

func ReadGitHubRepoAPIStream(ctx context.Context, repos []string, client *github.Client) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

		allocator := memory.NewGoAllocator()

		schema := arrow.NewSchema([]arrow.Field{
			{Name: "name", Type: arrow.BinaryTypes.String},
			{Name: "owner", Type: arrow.BinaryTypes.String},
			{Name: "description", Type: arrow.BinaryTypes.String},
			{Name: "stars", Type: arrow.PrimitiveTypes.Int32},
			{Name: "forks", Type: arrow.PrimitiveTypes.Int32},
			{Name: "language", Type: arrow.BinaryTypes.String},
		}, nil)

		for _, repo := range repos {
			select {
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			default:
			}

			repoInfo, err := fetchGitHubRepoData(ctx, repo, client)
			if err != nil {
				errChan <- err
				return
			}
			b := array.NewRecordBuilder(allocator, schema)
			defer b.Release()

			b.Field(0).(*array.StringBuilder).Append(repoInfo.GetName())
			b.Field(1).(*array.StringBuilder).Append(repoInfo.GetOwner().GetLogin())
			b.Field(2).(*array.StringBuilder).Append(repoInfo.GetDescription())
			b.Field(3).(*array.Int32Builder).Append(int32(repoInfo.GetStargazersCount()))
			b.Field(4).(*array.Int32Builder).Append(int32(repoInfo.GetForksCount()))
			b.Field(5).(*array.StringBuilder).Append(repoInfo.GetLanguage())

			record := b.NewRecord()
			recordChan <- record
		}
	}()

	return recordChan, errChan
}

func fetchGitHubRepoData(ctx context.Context, repo string, client *github.Client) (*github.Repository, error) {
	ownerRepo := parseRepo(repo)
	if len(ownerRepo) != 2 {
		return nil, fmt.Errorf("invalid repo format: %s, expected 'owner/repo'", repo)
	}

	repoInfo, _, err := client.Repositories.Get(ctx, ownerRepo[0], ownerRepo[1])
	if err != nil {
		return nil, fmt.Errorf("failed to fetch repository information: %w", err)
	}

	return repoInfo, nil
}

func parseRepo(repo string) []string {
	return strings.Split(repo, "/")
}

func NewGitHubClient(ctx context.Context, token string) *github.Client {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	tc := oauth2.NewClient(ctx, ts)
	return github.NewClient(tc)
}
