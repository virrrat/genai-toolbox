// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dataproc

import (
	"context"
	"fmt"

	dataprocapi "cloud.google.com/go/dataproc/apiv1"
	"github.com/goccy/go-yaml"
	"github.com/googleapis/genai-toolbox/internal/sources"
	"github.com/googleapis/genai-toolbox/internal/util"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
)

const SourceKind string = "dataproc"

// validate interface
var _ sources.SourceConfig = Config{}

func init() {
	if !sources.Register(SourceKind, newConfig) {
		panic(fmt.Sprintf("source kind %q already registered", SourceKind))
	}
}

func newConfig(ctx context.Context, name string, decoder *yaml.Decoder) (sources.SourceConfig, error) {
	actual := Config{Name: name}
	if err := decoder.DecodeContext(ctx, &actual); err != nil {
		return nil, err
	}
	return actual, nil
}

type Config struct {
	Name    string `yaml:"name" validate:"required"`
	Kind    string `yaml:"kind" validate:"required"`
	Project string `yaml:"project" validate:"required"`
	Region  string `yaml:"region" validate:"required"`
}

func (r Config) SourceConfigKind() string {
	return SourceKind
}

func (r Config) Initialize(ctx context.Context, tracer trace.Tracer) (sources.Source, error) {
	clusterClient, jobClient, err := initDataprocConnection(ctx, tracer, r.Name, r.Region)
	if err != nil {
		return nil, fmt.Errorf("error creating dataproc client: %w", err)
	}

	s := &Source{
		Name:    r.Name,
		Kind:    SourceKind,
		Project: r.Project,
		Region:  r.Region,
		ClusterClient: clusterClient,
		JobClient:     jobClient,
	}
	return s, nil
}

var _ sources.Source = &Source{}

type Source struct {
	Name    string `yaml:"name"`
	Kind    string `yaml:"kind"`
	Project string
	Region  string
	ClusterClient  *dataprocapi.ClusterControllerClient
	JobClient     *dataprocapi.JobControllerClient
}

func (s *Source) SourceKind() string {
	return SourceKind
}

func initDataprocConnection(
	ctx context.Context,
	tracer trace.Tracer,
	name string,
	region string,
) (*dataprocapi.ClusterControllerClient, *dataprocapi.JobControllerClient, error) {
	ctx, span := sources.InitConnectionSpan(ctx, tracer, SourceKind, name)
	defer span.End()

	cred, err := google.FindDefaultCredentials(ctx, dataprocapi.DefaultAuthScopes()...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to find default Google Cloud credentials: %w", err)
	}

	userAgent, err := util.UserAgentFromContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	endpoint := fmt.Sprintf("%s-dataproc.googleapis.com:443", region)
	clientOptions := []option.ClientOption{
		option.WithEndpoint(endpoint),
		option.WithUserAgent(userAgent),
		option.WithCredentials(cred),
	}
	
	clusterClient, err := dataprocapi.NewClusterControllerClient(ctx, clientOptions...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create Dataproc cluster client: %w", err)
	}

	jobClient, err := dataprocapi.NewJobControllerClient(ctx, clientOptions...)
	if err != nil {
		clusterClient.Close() // Clean up the already created client
		return nil, nil, fmt.Errorf("failed to create Dataproc JobControllerClient: %w", err)
	}

	return clusterClient, jobClient, nil
}
