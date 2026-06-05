package rag

import (
	"strings"
	"testing"
)

// ─── Schema tests ─────────────────────────────────────────────────────────────

func TestWarehouseTables_AllHaveEmbedText(t *testing.T) {
	for _, tbl := range WarehouseTables {
		text := tbl.EmbedText()
		if text == "" {
			t.Errorf("table %s has empty embed text", tbl.ID)
		}
		if !strings.Contains(text, tbl.QualifiedName) {
			t.Errorf("table %s embed text missing qualified name", tbl.ID)
		}
	}
}

func TestWarehouseTables_SchemaTextContainsColumns(t *testing.T) {
	for _, tbl := range WarehouseTables {
		schema := tbl.SchemaText()
		for _, col := range tbl.Columns {
			if !strings.Contains(schema, col.Name) {
				t.Errorf("table %s schema text missing column %s", tbl.QualifiedName, col.Name)
			}
		}
	}
}

// ─── Embedder tests ───────────────────────────────────────────────────────────

func TestTFIDFEmbed_DimensionAndNorm(t *testing.T) {
	vec := tfidfEmbed("revenue orders product top customers")
	if len(vec) != tfidfDim {
		t.Errorf("expected dim %d, got %d", tfidfDim, len(vec))
	}
	// Normalised vector should have unit length (within float32 precision).
	norm := float32(0)
	for _, v := range vec {
		norm += v * v
	}
	if norm > 0 && (norm < 0.99 || norm > 1.01) {
		t.Errorf("expected unit norm, got %.4f", norm)
	}
}

func TestTFIDFEmbed_SimilarQueriesSimilarVectors(t *testing.T) {
	v1 := tfidfEmbed("how much revenue today")
	v2 := tfidfEmbed("total revenue today amount")
	v3 := tfidfEmbed("which products have highest conversion rate")

	sim12 := cosineSim(v1, v2)
	sim13 := cosineSim(v1, v3)

	// Revenue queries should be more similar to each other than to product queries.
	if sim12 <= sim13 {
		t.Errorf("expected revenue queries more similar (%.3f) than revenue vs product (%.3f)", sim12, sim13)
	}
}

func TestTFIDFEmbed_EmptyInput(t *testing.T) {
	vec := tfidfEmbed("")
	if len(vec) != tfidfDim {
		t.Errorf("empty input should still return %d-dim vector", tfidfDim)
	}
}

func TestEmbed_OfflineFallback(t *testing.T) {
	// When no GRADIENT_AI_KEY is set, Embed should return the TF-IDF vector.
	t.Setenv("GRADIENT_AI_KEY", "")
	vec, err := Embed("top products by revenue")
	if err != nil {
		t.Fatalf("offline embed should not error: %v", err)
	}
	if len(vec) != tfidfDim {
		t.Errorf("expected offline vector dim %d, got %d", tfidfDim, len(vec))
	}
}

// ─── Qdrant ID hash test ──────────────────────────────────────────────────────

func TestSlugToID_Stable(t *testing.T) {
	if slugToID("fct-orders") != slugToID("fct-orders") {
		t.Error("slugToID must be deterministic")
	}
	if slugToID("fct-orders") == slugToID("dim-users") {
		t.Error("different slugs should produce different IDs")
	}
}

// ─── Retriever offline test ───────────────────────────────────────────────────

func TestRetriever_FallbackWhenQdrantUnavailable(t *testing.T) {
	t.Setenv("GRADIENT_AI_KEY", "")
	t.Setenv("QDRANT_URL", "http://localhost:19999") // nothing listening here

	r := NewRetriever()
	if r.IsAvailable() {
		t.Error("retriever should not be available when Qdrant is unreachable")
	}

	// Should still return a usable schema (full fallback).
	schema, tables, err := r.RetrieveSchema("how much revenue today", 3)
	if err != nil {
		t.Fatalf("fallback should not error: %v", err)
	}
	if schema == "" {
		t.Error("fallback schema should not be empty")
	}
	if len(tables) == 0 {
		t.Error("fallback should return all table names")
	}
	// Full fallback includes all tables.
	if len(tables) != len(WarehouseTables) {
		t.Errorf("expected %d tables in fallback, got %d", len(WarehouseTables), len(tables))
	}
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func cosineSim(a, b []float32) float32 {
	if len(a) != len(b) {
		return 0
	}
	dot := float32(0)
	for i := range a {
		dot += a[i] * b[i]
	}
	return dot // already normalised
}
