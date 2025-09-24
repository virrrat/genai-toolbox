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

package dataproclistjobs

import (
	dataprocpb "cloud.google.com/go/dataproc/apiv1/dataprocpb"
	"context"
	"encoding/json"
	"fmt"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/sources/dataproc"
	"github.com/googleapis/genai-toolbox/internal/tools"
	"google.golang.org/api/iterator"
	"google.golang.org/protobuf/types/known/timestamppb"
	"strings"
)

const kind = "dataproc-list-jobs"

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

	ds, ok := rawS.(*dataproc.Source)
	if !ok {
		return nil, fmt.Errorf("invalid source for %q tool: source kind must be `%s`", kind, dataproc.SourceKind)
	}

	desc := cfg.Description
	if desc == "" {
		desc = "Lists Different types of Dataproc jobs"
	}

	// An empty parameters object will generate the correct empty schema.
	allParameters := tools.Parameters{
		tools.NewStringParameterWithRequired("labels", "Optional: Resource labels as JSON string", false),
		tools.NewStringParameterWithRequired("status.state", "Optional: One of the following: `NON_ACTIVE`, `ACTIVE`", false),
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

// Tool is the implementation of the dataproc tool.
type Tool struct {
	Name        string `yaml:"name"`
	Kind        string `yaml:"kind"`
	Description string `yaml:"description"`

	Source    *dataproc.Source
	AllParams tools.Parameters

	manifest    tools.Manifest
	mcpManifest tools.McpManifest
}

type SimpleJob struct {
	ProjectId      string                 `json:"project_id,omitempty"`
	JobId          string                 `json:"job_id,omitempty"`
	JobUuid        string                 `json:"job_uuid,omitempty"`
	State          string                 `json:"state,omitempty"`
	StateStartTime *timestamppb.Timestamp `json:"state_start_time,omitempty"`
	JobType        string                 `json:"job_type,omitempty"`
}

func createJobFilter(params tools.ParamValues) (string, error) {
	paramsMap := params.AsMap()
	var filterParts []string

	labelsJSONString, ok := paramsMap["labels"].(string)
	if ok && labelsJSONString != "" && labelsJSONString != "{}" {
		var labels map[string]string
		if err := json.Unmarshal([]byte(labelsJSONString), &labels); err != nil {
			return "", fmt.Errorf("error parsing labels JSON: %w", err)
		}
		for key, value := range labels {
			part := fmt.Sprintf("labels.%s = %q", key, value)
			filterParts = append(filterParts, part)
		}
	}
	return strings.Join(filterParts, " AND "), nil
}

func newSimpleJob(job *dataprocpb.Job) SimpleJob {
	sj := SimpleJob{
		JobUuid: job.JobUuid,
	}

	if job.Reference != nil {
		sj.ProjectId = job.Reference.ProjectId
		sj.JobId = job.Reference.JobId
	}

	if job.Status != nil {
		sj.State = job.Status.State.String()
		sj.StateStartTime = job.Status.StateStartTime
	}

	var jobType string
	switch job.TypeJob.(type) {
	case *dataprocpb.Job_HadoopJob:
		jobType = "hadoop"
	case *dataprocpb.Job_SparkJob:
		jobType = "spark"
	case *dataprocpb.Job_PysparkJob:
		jobType = "pyspark"
	case *dataprocpb.Job_HiveJob:
		jobType = "hive"
	case *dataprocpb.Job_PigJob:
		jobType = "pig"
	case *dataprocpb.Job_SparkRJob:
		jobType = "spark_r"
	case *dataprocpb.Job_SparkSqlJob:
		jobType = "spark_sql"
	case *dataprocpb.Job_PrestoJob:
		jobType = "presto"
	default:
		jobType = "unknown"
	}
	sj.JobType = jobType

	return sj
}

// Invoke executes the tool's operation.
func (t Tool) Invoke(ctx context.Context, params tools.ParamValues, accessToken tools.AccessToken) (any, error) {
	filter, err := createJobFilter(params)
	if err != nil {
		return nil, err
	}

	req := &dataprocpb.ListJobsRequest{
		ProjectId: t.Source.Project,
		Region:    t.Source.Region,
		Filter:    filter,
	}

	matcher := dataprocpb.ListJobsRequest_ACTIVE
	if status, ok := params.AsMap()["status"].(string); ok && status != "" {
		switch strings.ToUpper(status) {
		case "ALL":
			matcher = dataprocpb.ListJobsRequest_ALL
		case "ACTIVE":
			matcher = dataprocpb.ListJobsRequest_ACTIVE
		case "NON_ACTIVE":
			matcher = dataprocpb.ListJobsRequest_NON_ACTIVE
		default:
			return nil, fmt.Errorf("invalid status: %q. Must be one of `ALL`, `ACTIVE`, `NON_ACTIVE`", status)
		}
	}
	req.JobStateMatcher = matcher

	var jobs []*dataprocpb.Job
	it := t.Source.JobClient.ListJobs(ctx, req)
	for {
		resp, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error listing Dataproc jobs: %w", err)
		}
		jobs = append(jobs, resp)
	}

	simpleJobs := make([]SimpleJob, 0, len(jobs))
	for _, job := range jobs {
		simpleJobs = append(simpleJobs, newSimpleJob(job))
	}

	return simpleJobs, nil
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
