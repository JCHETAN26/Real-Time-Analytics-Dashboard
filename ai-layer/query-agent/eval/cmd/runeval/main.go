// Command runeval executes the StreamSense Text-to-SQL evaluation benchmark
// and prints accuracy metrics. It supports two modes:
//
//	live    — calls the configured Gradient AI model (requires GRADIENT_AI_KEY)
//	offline — uses a deterministic stub generator so the harness can be run and
//	          graded with zero external dependencies (default when no key)
//
// It also compares prompt variants so you can measure which system prompt
// produces more accurate SQL.
//
// Usage:
//
//	go run ./eval/cmd/runeval                 # auto: live if key present, else offline
//	go run ./eval/cmd/runeval -mode offline   # force offline demo
//	go run ./eval/cmd/runeval -json report.json
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/streamsense-ai/ai-layer/query-agent/eval"
	"github.com/streamsense-ai/ai-layer/query-agent/guard"
)

func init() {
	// Wire the production SQL guardrail into the eval harness so generated
	// queries are graded with the same safety semantics used in production.
	eval.Validate = guard.ValidateSQL
}

func main() {
	mode := flag.String("mode", "auto", "eval mode: auto | live | offline")
	jsonOut := flag.String("json", "", "optional path to write the full JSON report")
	flag.Parse()

	resolved := *mode
	if resolved == "auto" {
		if os.Getenv("GRADIENT_AI_KEY") != "" {
			resolved = "live"
		} else {
			resolved = "offline"
		}
	}

	fmt.Printf("StreamSense Text-to-SQL Eval — mode=%s, cases=%d\n", resolved, len(eval.Dataset))
	fmt.Println("════════════════════════════════════════════════════════════")

	variants := eval.PromptVariants()
	var reports []eval.Report

	for _, v := range variants {
		var gen func(eval.EvalCase) string
		switch resolved {
		case "live":
			gen = eval.LiveGenerator(v)
		default:
			gen = eval.OfflineGenerator(v)
		}
		rep := eval.GradeAll(v.Name, gen, eval.Validate)
		reports = append(reports, rep)
		printReport(rep)
	}

	if len(reports) > 1 {
		printComparison(reports)
	}

	if *jsonOut != "" {
		f, err := os.Create(*jsonOut)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to write report: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		_ = enc.Encode(reports)
		fmt.Printf("\nFull JSON report written to %s\n", *jsonOut)
	}

	// Exit non-zero if the best variant is below an acceptable bar, so the
	// harness can gate CI.
	best := bestPassRate(reports)
	if best < 0.7 {
		fmt.Printf("\n❌ Best pass rate %.0f%% is below the 70%% bar\n", best*100)
		os.Exit(1)
	}
	fmt.Printf("\n✅ Best pass rate %.0f%% meets the 70%% bar\n", best*100)
}

func printReport(r eval.Report) {
	fmt.Printf("\nPrompt variant: %s\n", r.PromptVariant)
	fmt.Printf("  Pass rate        : %.0f%% (%d/%d)\n", r.PassRate*100, r.Passed, r.Total)
	fmt.Printf("  Avg table score  : %.0f%%\n", r.AvgTableScore*100)
	fmt.Printf("  Avg keyword score: %.0f%%\n", r.AvgKeywordScore*100)
	fmt.Printf("  Read-only safe   : %.0f%%\n", r.ValidReadOnlyPct*100)
	for _, c := range r.Cases {
		status := "✓"
		if !c.Passed {
			status = "✗"
		}
		fmt.Printf("    %s %-28s table=%.0f%% kw=%.0f%%", status, c.ID, c.TableScore*100, c.KeywordScore*100)
		if c.Notes != "" {
			fmt.Printf("  [%s]", c.Notes)
		}
		fmt.Println()
	}
}

func printComparison(reports []eval.Report) {
	fmt.Println("\n════════════════════════════════════════════════════════════")
	fmt.Println("Prompt A/B comparison")
	for _, r := range reports {
		fmt.Printf("  %-12s pass=%.0f%%  kw=%.0f%%\n", r.PromptVariant, r.PassRate*100, r.AvgKeywordScore*100)
	}
}

func bestPassRate(reports []eval.Report) float64 {
	best := 0.0
	for _, r := range reports {
		if r.PassRate > best {
			best = r.PassRate
		}
	}
	return best
}
