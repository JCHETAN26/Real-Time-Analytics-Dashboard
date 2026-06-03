package guard

import "testing"

func TestValidateSQL_AllowsReadOnlySelect(t *testing.T) {
	valid := []string{
		`SELECT product_name, SUM(revenue) AS total_revenue
		 FROM marts.fct_orders
		 WHERE order_status = 'paid'
		 GROUP BY product_name
		 ORDER BY total_revenue DESC
		 LIMIT 5;`,

		`SELECT minute_bucket, total_revenue FROM marts.agg_revenue_per_minute ORDER BY minute_bucket DESC LIMIT 20`,

		`WITH recent AS (
		     SELECT order_id, revenue FROM marts.fct_orders
		     WHERE order_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
		 )
		 SELECT COUNT(*) FROM recent`,

		`SELECT u.user_segment, AVG(o.revenue)
		 FROM marts.fct_orders o
		 JOIN marts.dim_users u ON o.user_id = u.user_id
		 GROUP BY u.user_segment`,
	}
	for _, q := range valid {
		if _, err := ValidateSQL(q); err != nil {
			t.Errorf("expected query to pass, got error: %v\nquery: %s", err, q)
		}
	}
}

func TestValidateSQL_RejectsDestructive(t *testing.T) {
	bad := map[string]string{
		"delete":            `DELETE FROM marts.fct_orders WHERE 1=1`,
		"drop":              `DROP TABLE marts.fct_orders`,
		"update":            `UPDATE marts.fct_orders SET revenue = 0`,
		"insert":            `INSERT INTO marts.fct_orders VALUES (1)`,
		"truncate":          `TRUNCATE TABLE marts.dim_users`,
		"stacked statement": `SELECT 1 FROM marts.fct_orders; DROP TABLE marts.fct_orders`,
		"non-select start":  `EXPLAIN SELECT * FROM marts.fct_orders`,
	}
	for name, q := range bad {
		if _, err := ValidateSQL(q); err == nil {
			t.Errorf("%s: expected rejection, query passed: %s", name, q)
		}
	}
}

func TestValidateSQL_NeutralizesCommentInjection(t *testing.T) {
	q := `SELECT 1 FROM marts.fct_orders -- ; DROP TABLE x`
	got, err := ValidateSQL(q)
	if err != nil {
		t.Fatalf("expected comment to be neutralized, got error: %v", err)
	}
	if got != "SELECT 1 FROM marts.fct_orders" {
		t.Errorf("comment not stripped cleanly: %q", got)
	}
}

func TestValidateSQL_RejectsUnapprovedSchema(t *testing.T) {
	bad := []string{
		`SELECT * FROM information_schema.tables`,
		`SELECT * FROM raw.order_events`,
		`SELECT * FROM snowflake.account_usage.query_history`,
		`SELECT * FROM secret_table`,
	}
	for _, q := range bad {
		if _, err := ValidateSQL(q); err == nil {
			t.Errorf("expected schema rejection, query passed: %s", q)
		}
	}
}

func TestValidateSQL_StripsTrailingSemicolonAndComments(t *testing.T) {
	q := "SELECT revenue FROM marts.fct_orders LIMIT 1; -- trailing comment"
	got, err := ValidateSQL(q)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "SELECT revenue FROM marts.fct_orders LIMIT 1" {
		t.Errorf("unexpected cleaned SQL: %q", got)
	}
}

func TestValidateSQL_DoesNotFlagColumnNamedUpdatedAt(t *testing.T) {
	q := `SELECT updated_at, revenue FROM marts.fct_orders ORDER BY updated_at DESC LIMIT 10`
	if _, err := ValidateSQL(q); err != nil {
		t.Errorf("column updated_at should not trigger UPDATE keyword: %v", err)
	}
}
