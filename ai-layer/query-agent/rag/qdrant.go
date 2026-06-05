package rag

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// QdrantClient wraps the Qdrant REST API. We use the HTTP API directly rather
// than the gRPC client to keep the dependency tree minimal (no protobuf, no
// CGO). All operations are synchronous and return errors rather than panicking.
type QdrantClient struct {
	baseURL    string
	collection string
	httpClient *http.Client
}

// NewQdrantClient creates a client for the given Qdrant instance.
// baseURL should be e.g. "http://localhost:6333".
// collection is the Qdrant collection name (created if absent).
func NewQdrantClient(baseURL, collection string) *QdrantClient {
	return &QdrantClient{
		baseURL:    baseURL,
		collection: collection,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// ─── Collection management ────────────────────────────────────────────────────

// EnsureCollection creates the collection if it doesn't exist.
// vectorSize must match the dimensionality of the embeddings being indexed.
func (q *QdrantClient) EnsureCollection(vectorSize int) error {
	// Check if collection already exists.
	resp, err := q.httpClient.Get(q.baseURL + "/collections/" + q.collection)
	if err != nil {
		return fmt.Errorf("qdrant: cannot reach %s: %w", q.baseURL, err)
	}
	resp.Body.Close()
	if resp.StatusCode == 200 {
		return nil // already exists
	}

	// Create it.
	body, _ := json.Marshal(map[string]interface{}{
		"vectors": map[string]interface{}{
			"size":     vectorSize,
			"distance": "Cosine",
		},
	})
	return q.put("/collections/"+q.collection, body)
}

// ─── Upsert ───────────────────────────────────────────────────────────────────

// UpsertPoint inserts or overwrites a single vector point.
// id must be a string UUID or integer; we use the TableDoc.ID (a short slug)
// converted to a deterministic integer via hash so Qdrant accepts it.
func (q *QdrantClient) UpsertPoint(id string, vector []float32, payload map[string]interface{}) error {
	body, _ := json.Marshal(map[string]interface{}{
		"points": []map[string]interface{}{
			{
				"id":      slugToID(id),
				"vector":  vector,
				"payload": payload,
			},
		},
	})
	return q.put("/collections/"+q.collection+"/points", body)
}

// ─── Search ───────────────────────────────────────────────────────────────────

// SearchResult is one hit returned by Qdrant.
type SearchResult struct {
	ID      uint64                 `json:"id"`
	Score   float32                `json:"score"`
	Payload map[string]interface{} `json:"payload"`
}

// Search returns the top-k most similar points to the query vector.
func (q *QdrantClient) Search(vector []float32, topK int) ([]SearchResult, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"vector":       vector,
		"limit":        topK,
		"with_payload": true,
	})

	raw, err := q.post("/collections/"+q.collection+"/points/search", body)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Result []SearchResult `json:"result"`
	}
	if err := json.Unmarshal(raw, &resp); err != nil {
		return nil, fmt.Errorf("qdrant: failed to parse search response: %w", err)
	}
	return resp.Result, nil
}

// ─── HTTP helpers ─────────────────────────────────────────────────────────────

func (q *QdrantClient) put(path string, body []byte) error {
	req, _ := http.NewRequest("PUT", q.baseURL+path, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("qdrant PUT %s: %w", path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		raw, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("qdrant PUT %s returned %d: %s", path, resp.StatusCode, raw)
	}
	return nil
}

func (q *QdrantClient) post(path string, body []byte) ([]byte, error) {
	req, _ := http.NewRequest("POST", q.baseURL+path, bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("qdrant POST %s: %w", path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("qdrant POST %s returned %d: %s", path, resp.StatusCode, raw)
	}
	return raw, nil
}

// slugToID maps a short slug like "fct-orders" to a stable uint64 Qdrant point
// ID using a polynomial hash. Qdrant requires integer or UUID point IDs.
func slugToID(slug string) uint64 {
	h := uint64(14695981039346656037) // FNV-1a offset basis
	for _, b := range []byte(slug) {
		h ^= uint64(b)
		h *= 1099511628211 // FNV prime
	}
	return h
}
