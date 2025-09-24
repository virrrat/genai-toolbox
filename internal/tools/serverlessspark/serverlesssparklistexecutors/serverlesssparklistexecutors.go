// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package serverlesssparklistexecutors

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/serverlessspark"
	"github.com/googleapis/genai-toolbox/internal/tools"
)

const kind = "serverless-spark-list-executors"

func init() {
	if !tools.Register(kind, newConfig) {
		panic(fmt.Sprintf("tool kind %q already registered", kind))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (tools.ToolConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type Config struct {
	Name         string   `yaml:"name" validate:"required"`
	Kind         string   `yaml:"kind" validate:"required"`
	Source       string   `yaml:"source" validate:"required"`
	Description  string   `yaml:"description"`
	AuthRequired []string `yaml:"authRequired"`
}

// validate interface
var _ tools.ToolConfig = Config{}

// ToolConfigKind returns the unique name for this tool.
func (cfg Config) ToolConfigKind() string {
	return kind
}

// Initialize creates a new Tool instance.
func (cfg Config) Initialize(srcs map[string]sources.Source) (tools.Tool, error) {
	rawS, ok := srcs[cfg.Source]
	if !ok {
		return nil, fmt.Errorf("source %q not found", cfg.Source)
	}

	ds, ok := rawS.(*serverlessspark.Source)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be `%s`", kind, serverlessspark.SourceKind)
	}

	desc := cfg.Description
	if desc == "" {
		desc = "Searches Spark Applications within a batch, then fetches executors for the first application found."
	}

	allParameters := tools.Parameters{
		tools.NewStringParameterWithRequired("batchId", "The ID of the batch to search within.", true),
	}

	mcpManifest := tools.McpManifest{
		Name:        cfg.Name,
		Description: desc,
		InputSchema: allParameters.McpManifest(),
	}

	return Tool{
		Name:        cfg.Name,
		Kind:        kind,
		Source:      ds,
		AllParams:   allParameters,
		manifest:    tools.Manifest{Description: desc, Parameters: allParameters.Manifest()},
		mcpManifest: mcpManifest,
	}, nil
}

// Tool is the implementation of the tool.
type Tool struct {
	Name        string `yaml:"name"`
	Kind        string `yaml:"kind"`
	Description string `yaml:"description"`
	Source      *serverlessspark.Source
	AllParams   tools.Parameters

	manifest    tools.Manifest
	mcpManifest tools.McpManifest
}

// --- Structs for SearchSparkApplications ---
type appInfo struct {
	ApplicationId string `json:"applicationId"`
}

type apiSparkApplication struct {
	Application   appInfo `json:"application"`
	Name          string `json:"name"`
}

type apiSearchSparkApplicationsResponse struct {
	SparkApplications []apiSparkApplication `json:"sparkApplications"`
	NextPageToken     string                `json:"nextPageToken"`
}

// --- Structs for SearchSparkApplicationExecutors ---

type apiExecutorSummary struct {
	ExecutorId       string `json:"executorId"`
	TotalCores       int32  `json:"totalCores"`
	TotalTasks       int32  `json:"totalTasks"`
	ActiveTasks      int32  `json:"activeTasks"`
	CompletedTasks   int32  `json:"completedTasks"`
	FailedTasks      int32  `json:"failedTasks"`
	DurationInMillis string `json:"totalDurationMillis"`
	GcTimeInMillis   string `json:"totalGcTimeMillis"`
	IsActive         bool   `json:"isActive"`
}

type apiSearchSparkApplicationExecutorsResponse struct {
	SparkApplicationExecutors []apiExecutorSummary `json:"sparkApplicationExecutors"`
	NextPageToken             string               `json:"nextPageToken"`
}

// --- Combined Result ---

type ToolResult struct {
	Executors    *apiSearchSparkApplicationExecutorsResponse `json:"executors,omitempty"`
}

// Invoke executes the tool's operation.
func (t Tool) Invoke(ctx context.Context, params tools.ParamValues, accessToken tools.AccessToken) (any, error) {
	paramMap := params.AsMap()
	batchId, ok := paramMap["batchId"].(string)
	if !ok || batchId == "" {
		return nil, fmt.Errorf("batchId parameter is required")
	}

	client, err := t.Source.GetClient(ctx, string(accessToken))
	if err != nil {
		return nil, fmt.Errorf("error getting client: %w", err)
	}

	// 1. Call SearchSparkApplications
	appsResponse, err := t.searchSparkApplications(ctx, client, paramMap, batchId)
	if err != nil {
		return nil, err
	}

	result := ToolResult{}

	if len(appsResponse.SparkApplications) == 0 {
		return result, nil // No applications found, return early
	}

	// 2. Call SearchSparkApplicationExecutors for the first application
	firstAppId := appsResponse.SparkApplications[0].Application.ApplicationId
	executorsResponse, err := t.searchSparkApplicationExecutors(ctx, client, paramMap, batchId, firstAppId)
	if err != nil {
		return result, err // Return partial result with error
	}

	result.Executors = executorsResponse
	return result, nil
}

func (t Tool) searchSparkApplications(ctx context.Context, client *http.Client, paramMap map[string]any, batchId string) (*apiSearchSparkApplicationsResponse, error) {
	urlString := fmt.Sprintf("%s/v1/projects/%s/locations/%s/batches/%s/sparkApplications:search", t.Source.BaseURL, t.Source.Project, t.Source.Location, batchId)
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("error parsing SearchSparkApplications URL %s: %w", urlString, err)
	}

	q := u.Query()
	q.Add("pageSize", "100")
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating SearchSparkApplications request: %w", err)
	}
	req.Header.Set("User-Agent", t.Source.UserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making SearchSparkApplications request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("SearchSparkApplications API request failed with status %d: error reading response body: %w", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("SearchSparkApplications API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var searchResponse apiSearchSparkApplicationsResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("error decoding SearchSparkApplications response: %w", err)
	}
	return &searchResponse, nil
}

func (t Tool) searchSparkApplicationExecutors(ctx context.Context, client *http.Client, paramMap map[string]any, batchId, appId string) (*apiSearchSparkApplicationExecutorsResponse, error) {
	parent := fmt.Sprintf("projects/%s/locations/%s/batches/%s", t.Source.Project, t.Source.Location, batchId)
	urlString := fmt.Sprintf("%s/v1/%s/sparkApplications/%s:searchExecutors", t.Source.BaseURL, parent, appId)
	u, err := url.Parse(urlString)
	if err != nil {
		return nil, fmt.Errorf("error parsing SearchSparkApplicationExecutors URL %s: %w", urlString, err)
	}

	q := u.Query()
	q.Add("pageSize", "100")
	q.Add("parent", parent)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating SearchSparkApplicationExecutors request: %w", err)
	}
	req.Header.Set("User-Agent", t.Source.UserAgent)

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error making SearchSparkApplicationExecutors request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, readErr := io.ReadAll(resp.Body)
		if readErr != nil {
			return nil, fmt.Errorf("SearchSparkApplicationExecutors API request failed with status %d: error reading response body: %w", resp.StatusCode, readErr)
		}
		return nil, fmt.Errorf("SearchSparkApplicationExecutors API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var searchResponse apiSearchSparkApplicationExecutorsResponse
	if err := json.NewDecoder(resp.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("error decoding SearchSparkApplicationExecutors response: %w", err)
	}
	return &searchResponse, nil
}

// ParseParams parses and validates the input parameters.
func (t Tool) ParseParams(data map[string]any, claims map[string]map[string]any) (tools.ParamValues, error) {
	return tools.ParseParams(t.AllParams, data, claims)
}

// Manifest returns the tool's manifest.
func (t Tool) Manifest() tools.Manifest {
	return t.manifest
}

// McpManifest returns the tool's MCP manifest.
func (t Tool) McpManifest() tools.McpManifest {
	return t.mcpManifest
}

// Authorized checks if the tool is authorized to run.
func (t Tool) Authorized(services []string) bool {
	return true
}

func (t Tool) RequiresClientAuthorization() bool {
	return false
}
