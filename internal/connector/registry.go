// Copyright (C) 2026 YIQISOFT
// SPDX-License-Identifier: Apache-2.0

package connector

import "sync"

// Registry maintains a mapping from connector type to handler.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry creates a new registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]Handler),
	}
}

// Register registers a handler for the given type.
func (r *Registry) Register(h Handler) {
	if r == nil || h == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[h.Type()] = h
}

// Get returns the handler for the given type.
func (r *Registry) Get(t string) (Handler, bool) {
	if r == nil {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[t]
	return h, ok
}

// GetAll returns a copy of all registered handlers.
func (r *Registry) GetAll() map[string]Handler {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make(map[string]Handler, len(r.handlers))
	for k, v := range r.handlers {
		out[k] = v
	}
	return out
}
