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
	"io"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	memoryPool "github.com/arrowarc/arrowarc/internal/memory"
	"github.com/google/go-github/v64/github"
	"golang.org/x/oauth2"
)

// GitHubReader reads GitHub repository data and implements the Reader interface.
type GitHubReader struct {
	repos        []string
	client       *github.Client
	schema       *arrow.Schema
	currentIndex int
	alloc        memory.Allocator
}

// ReadOptions defines options for reading GitHub repository data.
type GitHubReadOptions struct {
	Repos []string
	Token string
}

// NewGitHubReader creates a new GitHub reader for fetching repository data.
func NewGitHubReader(ctx context.Context, opts *GitHubReadOptions) (*GitHubReader, error) {
	alloc := memoryPool.GetAllocator()

	client := NewGitHubClient(ctx, opts.Token)
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "name", Type: arrow.BinaryTypes.String},
		{Name: "owner", Type: arrow.BinaryTypes.String},
		{Name: "description", Type: arrow.BinaryTypes.String},
		{Name: "stars", Type: arrow.PrimitiveTypes.Int32},
		{Name: "forks", Type: arrow.PrimitiveTypes.Int32},
		{Name: "language", Type: arrow.BinaryTypes.String},
	}, nil)

	return &GitHubReader{
		repos:        opts.Repos,
		client:       client,
		schema:       schema,
		currentIndex: 0,
		alloc:        alloc,
	}, nil
}

// Read reads the next record of GitHub repository data.
func (r *GitHubReader) Read() (arrow.Record, error) {
	if r.currentIndex >= len(r.repos) {
		return nil, io.EOF
	}

	repo := r.repos[r.currentIndex]
	r.currentIndex++

	repoInfo, err := fetchGitHubRepoData(context.Background(), repo, r.client)
	if err != nil {
		return nil, fmt.Errorf("error fetching GitHub repo data: %w", err)
	}

	b := array.NewRecordBuilder(r.alloc, r.schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).Append(repoInfo.GetName())
	b.Field(1).(*array.StringBuilder).Append(repoInfo.GetOwner().GetLogin())
	b.Field(2).(*array.StringBuilder).Append(repoInfo.GetDescription())
	b.Field(3).(*array.Int32Builder).Append(int32(repoInfo.GetStargazersCount()))
	b.Field(4).(*array.Int32Builder).Append(int32(repoInfo.GetForksCount()))
	b.Field(5).(*array.StringBuilder).Append(repoInfo.GetLanguage())

	record := b.NewRecord()
	return record, nil
}

// Close releases resources associated with the GitHubReader.
func (r *GitHubReader) Close() error {
	defer memoryPool.PutAllocator(r.alloc)
	// Additional cleanup if needed
	return nil
}

// Schema returns the schema of the records being read from GitHub API.
func (r *GitHubReader) Schema() *arrow.Schema {
	return r.schema
}

// fetchGitHubRepoData retrieves data for a GitHub repository.
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

// parseRepo parses a repository string in the format "owner/repo" into its components.
func parseRepo(repo string) []string {
	return strings.Split(repo, "/")
}

// NewGitHubClient creates a new GitHub client using the provided OAuth token.
func NewGitHubClient(ctx context.Context, token string) *github.Client {
	ts := oauth2.StaticTokenSource(
		&oauth2.Token{AccessToken: token},
	)
	tc := oauth2.NewClient(ctx, ts)
	return github.NewClient(tc)
}
