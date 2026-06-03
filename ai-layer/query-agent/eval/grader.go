package eval

import (
	"strings"
)

// CaseResult holds the graded outcome for one evaluation case.
type CaseResult struct {
	ID            string  `json:"id"`
	Question      string  `json:"question"`
	GeneratedSQL  string  `json:"generated_sql"`
	Passed        bool    `json:"passed"`
	TableScore    float64 `json:"table_score"`   // fraction of required tables present
	KeywordScore  float64 `json:"keyword_score"` // fraction of required keywords present
	ValidReadOnly bool    `json:"valid_read_only"`
	Notes         string  `json:"notes"`
}

// Report aggregates results across the whole dataset.
type Report struct {
	Total            int          `json:"total"`
	Passed           int          `json:"passed"`
	PassRate         float64      `json:"pass_rate"`
	AvgTableScore    float64      `json:"avg_table_score"`
	AvgKeywordScore  float64      `json:"avg_keyword_score"`
	ValidReadOnlyPct float64      `json:"valid_read_only_pct"`
	PromptVariant    string       `json:"prompt_variant"`
	Cases            []CaseResult `json:"cases"`
}

// SafetyValidator is the signature of the SQL guardrail, injected so eval can
// reuse the production validator without importing the main package.
type SafetyValidator func(sql string) (string, error)

// GradeCase scores a single generated SQL string against a gold case.
//
// Grading is intentionally structural rather than exact-string: two correct
// SQL statements can differ in whitespace, alias names, or clause order. We
// check that (a) the required tables are referenced, (b) the required
// keywords/columns appear, and (c) the query passes the read-only guardrail.
func GradeCase(c EvalCase, generatedSQL string, validate SafetyValidator) CaseResult {
	res := CaseResult{
		ID:           c.ID,
		Question:     c.Question,
		GeneratedSQL: generatedSQL,
	}

	upper := strings.ToUpper(generatedSQL)

	// Table coverage.
	tableHits := 0
	for _, tbl := range c.MustTables {
		if strings.Contains(strings.ToLower(generatedSQL), strings.ToLower(tbl)) {
			tableHits++
		}
	}
	if len(c.MustTables) > 0 {
		res.TableScore = float64(tableHits) / float64(len(c.MustTables))
	} else {
		res.TableScore = 1
	}

	// Keyword coverage.
	kwHits := 0
	for _, kw := range c.MustKeyword {
		if strings.Contains(upper, strings.ToUpper(kw)) {
			kwHits++
		}
	}
	if len(c.MustKeyword) > 0 {
		res.KeywordScore = float64(kwHits) / float64(len(c.MustKeyword))
	} else {
		res.KeywordScore = 1
	}

	// Safety: the generated SQL must survive the production guardrail.
	if validate != nil {
		if _, err := validate(generatedSQL); err == nil {
			res.ValidReadOnly = true
		} else {
			res.Notes = "blocked by guardrail: " + err.Error()
		}
	} else {
		res.ValidReadOnly = true
	}

	// A case passes when it is safe, references all required tables, and hits
	// at least 75% of the expected keywords.
	res.Passed = res.ValidReadOnly && res.TableScore == 1.0 && res.KeywordScore >= 0.75
	return res
}

// GradeAll runs the full dataset through a generator function and produces an
// aggregate report. The generator turns a question into SQL — in production
// this calls the LLM; in tests it can be a deterministic stub.
func GradeAll(variant string, generate func(EvalCase) string, validate SafetyValidator) Report {
	rep := Report{PromptVariant: variant}
	var sumTable, sumKeyword, validCount float64

	for _, c := range Dataset {
		sql := generate(c)
		r := GradeCase(c, sql, validate)
		rep.Cases = append(rep.Cases, r)
		if r.Passed {
			rep.Passed++
		}
		sumTable += r.TableScore
		sumKeyword += r.KeywordScore
		if r.ValidReadOnly {
			validCount++
		}
	}

	rep.Total = len(Dataset)
	if rep.Total > 0 {
		rep.PassRate = float64(rep.Passed) / float64(rep.Total)
		rep.AvgTableScore = sumTable / float64(rep.Total)
		rep.AvgKeywordScore = sumKeyword / float64(rep.Total)
		rep.ValidReadOnlyPct = validCount / float64(rep.Total)
	}
	return rep
}
