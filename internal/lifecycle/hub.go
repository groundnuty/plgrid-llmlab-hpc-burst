package lifecycle

import (
	"context"
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
	// entries are the listings Files read, by repository and revision, which
	// TemplateSources checks the template files against.
	entries map[string][]hubfiles.Entry
}

// endpoint is the Hub's origin the source lists from.
func (h *HubSource) endpoint() string {
	if h.BaseURL == "" {
		return hubfiles.DefaultBaseURL
	}
	return h.BaseURL
}

func (h *HubSource) client(tokenFile string) (*hubfiles.Client, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.clients[tokenFile]; ok {
		return c, nil
	}
	c, err := hubfiles.New(hubfiles.Config{BaseURL: h.endpoint(), TokenFile: tokenFile})
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
	h.mu.Lock()
	if h.entries == nil {
		h.entries = map[string][]hubfiles.Entry{}
	}
	h.entries[cacheKey(cp)] = entries
	h.mu.Unlock()
	return hubfiles.Inputs(entries), nil
}

// TemplateSources implements CheckpointSource: the checkpoint's chat-template
// files (HBEP-3's lookup chain), read by hubfiles.Client.TemplateSources and
// checked against the listing Files read.
func (h *HubSource) TemplateSources(ctx context.Context, cp v1alpha1.CheckpointSpec, _ []deploymentid.File) (deploymentid.TemplateSources, error) {
	h.mu.Lock()
	entries, ok := h.entries[cacheKey(cp)]
	h.mu.Unlock()
	if !ok {
		if _, err := h.Files(ctx, cp); err != nil {
			return deploymentid.TemplateSources{}, err
		}
		h.mu.Lock()
		entries = h.entries[cacheKey(cp)]
		h.mu.Unlock()
	}
	c, err := h.client(tokenFileOf(cp))
	if err != nil {
		return deploymentid.TemplateSources{}, err
	}
	return c.TemplateSources(ctx, cp.Repository, cp.Revision, entries)
}

// retryableHub reports whether a checkpoint read is worth an early retry.
func retryableHub(err error) bool {
	return hubfiles.Retryable(err)
}
