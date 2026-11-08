package common

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// RequireSchema checks that every table and column a service uses exists.
// Services call it at startup so a schema mismatch stops the pod with a clear
// message instead of failing every request at runtime.
func RequireSchema(ctx context.Context, db *sql.DB, required map[string][]string) error {
	if len(required) == 0 {
		return nil
	}
	rows, err := db.QueryContext(ctx, `
		SELECT table_name, column_name
		FROM information_schema.columns
		WHERE table_schema = 'public'`)
	if err != nil {
		return err
	}
	defer rows.Close()

	have := map[string]map[string]bool{}
	for rows.Next() {
		var t, c string
		if err := rows.Scan(&t, &c); err != nil {
			return err
		}
		if have[t] == nil {
			have[t] = map[string]bool{}
		}
		have[t][c] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	var missing []string
	for table, cols := range required {
		if have[table] == nil {
			missing = append(missing, "table "+table)
			continue
		}
		for _, c := range cols {
			if !have[table][c] {
				missing = append(missing, table+"."+c)
			}
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing %s", strings.Join(missing, ", "))
	}
	return nil
}
