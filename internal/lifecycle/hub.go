package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/groundnuty/plgrid-llmlab-hpc-burst/api/v1alpha1"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/deploymentid"
	"github.com/groundnuty/plgrid-llmlab-hpc-burst/pkg/hubfiles"
)

// HubSource reads checkpoints from the Hugging Face Hub through pkg/hubfiles,
// one client per token file (a gated checkpoint names its own).
type HubSource struct {
	// BaseURL is the Hub's origin; empty means hubfiles.DefaultBaseURL.
	BaseURL string

	mu      sync.Mutex
	clients map[string]*hubfiles.Client
}

func (h *HubSource) client(tokenFile string) (*hubfiles.Client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.clients[tokenFile]; ok {
		return c, nil
	}
	c, err := hubfiles.New(hubfiles.Config{BaseURL: h.BaseURL, TokenFile: tokenFile})
	if err != nil {
		return nil, err
	}
	if h.clients == nil {
		h.clients = map[string]*hubfiles.Client{}
	}
	h.clients[tokenFile] = c
	return c, nil
}

func tokenFileOf(cp v1alpha1.CheckpointSpec) string {
	if cp.HubTokenRef != nil {
		return cp.HubTokenRef.File
	}
	return ""
}

// Files implements CheckpointSource.
func (h *HubSource) Files(ctx context.Context, cp v1alpha1.CheckpointSpec) ([]deploymentid.File, error) {
	c, err := h.client(tokenFileOf(cp))
	if err != nil {
		return nil, err
	}
	entries, err := c.Files(ctx, cp.Repository, cp.Revision)
	if err != nil {
		return nil, err
	}
	return hubfiles.Inputs(entries), nil
}

// TemplateSources implements CheckpointSource. pkg/hubfiles cannot read a
// file's content yet (requested of the contracts owner, 31 Oct 2026), so it
// returns ErrTemplateSourcesUnavailable, and a serving spec without a chat
// template is not given an ID.
func (h *HubSource) TemplateSources(context.Context, v1alpha1.CheckpointSpec, []deploymentid.File) (deploymentid.TemplateSources, error) {
	return deploymentid.TemplateSources{}, ErrTemplateSourcesUnavailable
}

// retryableHub reports whether a checkpoint read is worth an early retry.
func retryableHub(err error) bool {
	if errors.Is(err, ErrTemplateSourcesUnavailable) {
		return false
	}
	return hubfiles.Retryable(err)
}

var _ = fmt.Sprintf
