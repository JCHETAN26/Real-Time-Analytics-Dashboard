package eval

import "testing"

// noopValidator accepts any SQL — used to isolate structural grading from
// guardrail behavior in unit tests.
func noopValidator(sql string) (string, error) { return sql, nil }

func TestGradeCase_PerfectMatchPasses(t *testing.T) {
	c := Dataset[0] // top-products-last-hour
	r := GradeCase(c, c.GoldSQL, noopValidator)
	if !r.Passed {
		t.Fatalf("gold SQL should pass its own case, got: %+v", r)
	}
	if r.TableScore != 1.0 {
		t.Errorf("expected full table score, got %v", r.TableScore)
	}
}

func TestGradeCase_WrongTableFails(t *testing.T) {
	c := Dataset[0]
	bad := "SELECT product_name FROM marts.dim_users LIMIT 5"
	r := GradeCase(c, bad, noopValidator)
	if r.Passed {
		t.Errorf("query against wrong table should fail")
	}
	if r.TableScore != 0.0 {
		t.Errorf("expected zero table score, got %v", r.TableScore)
	}
}

func TestGradeAll_GoldGeneratorScoresPerfect(t *testing.T) {
	// A generator that always returns the gold SQL must score 100% pass rate.
	rep := GradeAll("gold", func(c EvalCase) string { return c.GoldSQL }, noopValidator)
	if rep.PassRate != 1.0 {
		t.Errorf("gold generator should yield pass rate 1.0, got %v", rep.PassRate)
	}
	if rep.Total != len(Dataset) {
		t.Errorf("expected %d cases, got %d", len(Dataset), rep.Total)
	}
}

func TestGradeAll_EmptyGeneratorScoresZero(t *testing.T) {
	rep := GradeAll("empty", func(c EvalCase) string { return "" }, noopValidator)
	if rep.Passed != 0 {
		t.Errorf("empty generator should pass 0 cases, got %d", rep.Passed)
	}
}
