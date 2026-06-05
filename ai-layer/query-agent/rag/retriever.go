package rag

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	defaultQdrantURL   = "http://localhost:6333"
	collectionName     = "streamsense-schema"
	defaultTopK        = 3
)

// Retriever manages the schema index in Qdrant and handles retrieval.
// It is initialised once at service startup; the zero value is safe to use
// (falls back to the full hardcoded schema if Qdrant is not available).
type Retriever struct {
	qdrant    *QdrantClient
	available bool // false when Qdrant is unreachable; falls back gracefully
}

// NewRetriever creates a Retriever and indexes all warehouse tables into Qdrant.
// If Qdrant is not running (QDRANT_URL not set or unreachable) it logs a warning
// and continues — queries will fall back to the full schema prompt.
func NewRetriever() *Retriever {
	url := os.Getenv("QDRANT_URL")
	if url == "" {
		url = defaultQdrantURL
	}

	r := &Retriever{
		qdrant: NewQdrantClient(url, collectionName),
	}

	if err := r.indexAll(); err != nil {
		log.Printf("⚠️  RAG: Qdrant unavailable (%v) — falling back to full schema prompt", err)
		r.available = false
	} else {
		log.Printf("✅ RAG: indexed %d tables into Qdrant at %s", len(WarehouseTables), url)
		r.available = true
	}
	return r
}

// indexAll embeds every TableDoc and upserts it into Qdrant.
func (r *Retriever) indexAll() error {
	// Embed the first table to discover the vector size.
	first, err := Embed(WarehouseTables[0].EmbedText())
	if err != nil {
		return fmt.Errorf("embedding failed: %w", err)
	}

	if err := r.qdrant.EnsureCollection(len(first)); err != nil {
		return err
	}

	for _, table := range WarehouseTables {
		vec, err := Embed(table.EmbedText())
		if err != nil {
			return fmt.Errorf("embedding %s: %w", table.QualifiedName, err)
		}

		payload := map[string]interface{}{
			"id":             table.ID,
			"qualified_name": table.QualifiedName,
			"description":    table.Description,
		}
		if err := r.qdrant.UpsertPoint(table.ID, vec, payload); err != nil {
			return fmt.Errorf("upsert %s: %w", table.QualifiedName, err)
		}
	}
	return nil
}

// RetrieveSchema embeds the user's question, retrieves the top-k most relevant
// tables from Qdrant, and returns a focused schema block ready for injection
// into the system prompt.
//
// If Qdrant is unavailable it returns the full schema (all 5 tables) so the
// agent always has something to work with.
func (r *Retriever) RetrieveSchema(question string, topK int) (string, []string, error) {
	if topK <= 0 {
		topK = defaultTopK
	}

	if !r.available {
		return fullSchemaBlock(), allTableNames(), nil
	}

	vec, err := Embed(question)
	if err != nil {
		log.Printf("RAG: embed error for question, using full schema: %v", err)
		return fullSchemaBlock(), allTableNames(), nil
	}

	results, err := r.qdrant.Search(vec, topK)
	if err != nil {
		log.Printf("RAG: search error, using full schema: %v", err)
		return fullSchemaBlock(), allTableNames(), nil
	}

	// Map result IDs back to TableDocs.
	tableByID := make(map[string]TableDoc)
	for _, t := range WarehouseTables {
		tableByID[t.ID] = t
	}

	var schemaBlocks []string
	var retrievedNames []string

	for _, hit := range results {
		idRaw, _ := hit.Payload["id"].(string)
		t, ok := tableByID[idRaw]
		if !ok {
			continue
		}
		schemaBlocks = append(schemaBlocks, t.SchemaText())
		retrievedNames = append(retrievedNames, t.QualifiedName)
		log.Printf("RAG: retrieved %s (score=%.3f)", t.QualifiedName, hit.Score)
	}

	if len(schemaBlocks) == 0 {
		// No hits — return full schema as safe fallback.
		return fullSchemaBlock(), allTableNames(), nil
	}

	return strings.Join(schemaBlocks, "\n"), retrievedNames, nil
}

// IsAvailable reports whether Qdrant is reachable and the index is populated.
func (r *Retriever) IsAvailable() bool {
	return r.available
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func fullSchemaBlock() string {
	var parts []string
	for _, t := range WarehouseTables {
		parts = append(parts, t.SchemaText())
	}
	return strings.Join(parts, "\n")
}

func allTableNames() []string {
	names := make([]string, len(WarehouseTables))
	for i, t := range WarehouseTables {
		names[i] = t.QualifiedName
	}
	return names
}
