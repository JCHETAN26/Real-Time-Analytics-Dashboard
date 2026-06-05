// Package guard implements a read-only SQL safety check for LLM-generated
// queries. It enforces an allow-list: a single SELECT statement that touches
// only approved schemas. It is imported by both the query service and the
// evaluation harness so they share identical safety semantics.
package guard

import (
	"fmt"
	"regexp"
	"strings"
)

// forbiddenKeywords are statement types that must never reach the warehouse.
var forbiddenKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "CREATE", "TRUNCATE",
	"MERGE", "GRANT", "REVOKE", "REPLACE", "CALL", "EXECUTE", "EXEC",
	"COPY", "PUT", "REMOVE", "UNDROP", "USE", "SET",
}

// allowedSchemas are the only schema prefixes the agent may read from.
var allowedSchemas = map[string]bool{
	"marts":       true,
	"marts_marts": true,
	"staging":     true,
}

var (
	lineCommentRe  = regexp.MustCompile(`--[^\n]*`)
	blockCommentRe = regexp.MustCompile(`(?s)/\*.*?\*/`)
	// matches a table reference immediately following FROM or JOIN, e.g.
	// "FROM marts.fct_orders" or "JOIN staging.stg_user_events u"
	fromJoinRefRe = regexp.MustCompile(`(?i)\b(?:FROM|JOIN)\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)`)
	cteNameRe     = regexp.MustCompile(`(?i)(?:\bWITH\b|,)\s*([a-z_][a-z0-9_]*)\s+AS\s*\(`)
)

func stripComments(sql string) string {
	sql = blockCommentRe.ReplaceAllString(sql, " ")
	sql = lineCommentRe.ReplaceAllString(sql, " ")
	return sql
}

// ValidateSQL returns the cleaned, single-statement SQL if it is safe to run,
// or an error describing why it was rejected.
func ValidateSQL(raw string) (string, error) {
	cleaned := strings.TrimSpace(stripComments(raw))
	if cleaned == "" {
		return "", fmt.Errorf("empty query")
	}

	// Drop a single trailing semicolon, then ensure there are no more — a
	// second statement (stacked query) is a classic injection vector.
	cleaned = strings.TrimRight(cleaned, "; \n\t\r")
	if strings.Contains(cleaned, ";") {
		return "", fmt.Errorf("multiple statements are not allowed")
	}

	upper := strings.ToUpper(cleaned)

	if !strings.HasPrefix(upper, "SELECT") && !strings.HasPrefix(upper, "WITH") {
		return "", fmt.Errorf("only read-only SELECT queries are permitted")
	}

	for _, kw := range forbiddenKeywords {
		if containsWord(upper, kw) {
			return "", fmt.Errorf("forbidden keyword %q detected", kw)
		}
	}

	if err := validateSchemaRefs(cleaned); err != nil {
		return "", err
	}

	return cleaned, nil
}

// containsWord reports whether word appears in s bounded by non-identifier
// characters, so "UPDATE" matches but "updated_at" does not.
func containsWord(s, word string) bool {
	idx := 0
	for {
		found := strings.Index(s[idx:], word)
		if found == -1 {
			return false
		}
		start := idx + found
		end := start + len(word)
		beforeOK := start == 0 || !isIdentChar(s[start-1])
		afterOK := end == len(s) || !isIdentChar(s[end])
		if beforeOK && afterOK {
			return true
		}
		idx = start + 1
	}
}

func isIdentChar(b byte) bool {
	return b == '_' ||
		(b >= 'a' && b <= 'z') ||
		(b >= 'A' && b <= 'Z') ||
		(b >= '0' && b <= '9')
}

// validateSchemaRefs ensures that every table referenced after FROM or JOIN
// uses an approved schema or is a CTE defined within the same query.
func validateSchemaRefs(sql string) error {
	matches := fromJoinRefRe.FindAllStringSubmatch(sql, -1)
	knownCTEs := collectCTENames(sql)
	for _, m := range matches {
		ref := strings.ToLower(m[1])
		if !strings.Contains(ref, ".") {
			if knownCTEs[ref] {
				continue
			}
			return fmt.Errorf("unqualified table %q is not a known CTE; use marts.* or staging.*", ref)
		}
		schema := strings.SplitN(ref, ".", 2)[0]
		if !allowedSchemas[schema] {
			return fmt.Errorf("schema %q is not permitted (allowed: marts, staging)", schema)
		}
	}
	return nil
}

// collectCTENames extracts names defined in a WITH clause.
func collectCTENames(sql string) map[string]bool {
	names := map[string]bool{}
	for _, m := range cteNameRe.FindAllStringSubmatch(sql, -1) {
		names[strings.ToLower(m[1])] = true
	}
	return names
}
