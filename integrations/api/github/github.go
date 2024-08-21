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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type OwnerInfo struct {
	Login string `json:"login"`
}

type RepoInfo struct {
	Name        string    `json:"name"`
	Owner       OwnerInfo `json:"owner"`
	Description string    `json:"description"`
	Stars       int       `json:"stargazers_count"`
	Forks       int       `json:"forks_count"`
	Language    string    `json:"language"`
}

func ReadGitHubRepoAPIStream(ctx context.Context, repos []string) (<-chan arrow.Record, <-chan error) {
	recordChan := make(chan arrow.Record)
	errChan := make(chan error, 1)

	go func() {
		defer close(recordChan)
		defer close(errChan)

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

			repoInfo, err := fetchGitHubRepoData(ctx, repo)
			if err != nil {
				errChan <- err
				return
			}

			// Flatten the nested owner structure to match the Arrow schema
			repoData := map[string]interface{}{
				"name":        repoInfo.Name,
				"owner":       repoInfo.Owner.Login,
				"description": repoInfo.Description,
				"stars":       repoInfo.Stars,
				"forks":       repoInfo.Forks,
				"language":    repoInfo.Language,
			}

			jsonDataBytes, err := json.Marshal(repoData)
			if err != nil {
				errChan <- err
				return
			}

			jsonReader := array.NewJSONReader(bytes.NewReader(jsonDataBytes), schema)
			if jsonReader == nil {
				errChan <- fmt.Errorf("failed to create JSON reader")
				return
			}
			defer jsonReader.Release()

			for jsonReader.Next() {
				record := jsonReader.Record()
				if record == nil {
					continue
				}

				record.Retain()
				recordChan <- record
			}

			if err := jsonReader.Err(); err != nil {
				errChan <- err
				return
			}
		}
	}()

	return recordChan, errChan
}

func fetchGitHubRepoData(ctx context.Context, repo string) (*RepoInfo, error) {
	url := fmt.Sprintf("https://api.github.com/repos/%s", repo)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to call GitHub API: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API error: %s", resp.Status)
	}

	var repoInfo RepoInfo
	if err := json.NewDecoder(resp.Body).Decode(&repoInfo); err != nil {
		return nil, fmt.Errorf("failed to decode API response: %w", err)
	}

	return &repoInfo, nil
}
