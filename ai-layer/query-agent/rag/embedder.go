package rag

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"strings"
	"time"
)

// EmbedRequest / EmbedResponse match the OpenAI-compatible embeddings API
// used by DigitalOcean Inference (POST /v1/embeddings).
type EmbedRequest struct {
	Model  string `json:"model"`
	Input  string `json:"input"`
}

type EmbedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
}

// Embed calls the DigitalOcean Gradient AI embeddings endpoint and returns a
// float32 vector for the given text.
// If GRADIENT_AI_KEY is not set it falls back to a deterministic TF-IDF
// cosine similarity so the RAG pipeline works fully offline.
func Embed(text string) ([]float32, error) {
	apiKey := os.Getenv("GRADIENT_AI_KEY")
	if apiKey == "" {
		return tfidfEmbed(text), nil
	}

	apiBase := os.Getenv("GRADIENT_AI_BASE_URL")
	if apiBase == "" {
		apiBase = "https://inference.do-ai.run/v1"
	}

	model := os.Getenv("GRADIENT_EMBED_MODEL")
	if model == "" {
		model = "text-embedding-3-small"
	}

	body, _ := json.Marshal(EmbedRequest{Model: model, Input: text})
	req, err := http.NewRequest("POST", apiBase+"/embeddings", bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embeddings request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("embeddings API error %d: %s", resp.StatusCode, string(raw))
	}

	var result EmbedResponse
	if err := json.Unmarshal(raw, &result); err != nil || len(result.Data) == 0 {
		return nil, fmt.Errorf("could not parse embeddings response: %s", string(raw))
	}
	return result.Data[0].Embedding, nil
}

// ─── TF-IDF offline fallback ──────────────────────────────────────────────────
//
// When no API key is present (CI, offline demo) we compute a 128-dimensional
// hash-based embedding using a bag-of-words approach. It's not as accurate as
// a neural embedding but it correctly retrieves revenue tables for revenue
// questions, product tables for product questions, etc.

const tfidfDim = 128

// stopWords are filtered out before hashing so common English words don't
// dominate the similarity score.
var stopWords = map[string]bool{
	"the": true, "a": true, "an": true, "is": true, "are": true, "was": true,
	"were": true, "in": true, "of": true, "to": true, "for": true, "and": true,
	"or": true, "how": true, "what": true, "which": true, "my": true, "our": true,
	"do": true, "did": true, "does": true, "have": true, "has": true, "with": true,
}

func tfidfEmbed(text string) []float32 {
	vec := make([]float32, tfidfDim)
	words := strings.Fields(strings.ToLower(text))
	for _, w := range words {
		w = strings.Trim(w, ".,!?;:()'\"")
		if stopWords[w] || len(w) < 2 {
			continue
		}
		// Map each word to a bucket via a simple polynomial hash.
		h := 0
		for _, ch := range w {
			h = h*31 + int(ch)
		}
		if h < 0 {
			h = -h
		}
		vec[h%tfidfDim] += 1.0
	}
	return normalise(vec)
}

func normalise(v []float32) []float32 {
	norm := float32(0)
	for _, x := range v {
		norm += x * x
	}
	norm = float32(math.Sqrt(float64(norm)))
	if norm == 0 {
		return v
	}
	for i := range v {
		v[i] /= norm
	}
	return v
}
