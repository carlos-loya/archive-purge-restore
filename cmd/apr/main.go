package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	"github.com/carlos-loya/archive-purge-restore/internal/engine"
	"github.com/carlos-loya/archive-purge-restore/internal/history"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/database"
	dbmysql "github.com/carlos-loya/archive-purge-restore/internal/provider/database/mysql"
	dbpg "github.com/carlos-loya/archive-purge-restore/internal/provider/database/postgres"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage"
	"github.com/carlos-loya/archive-purge-restore/internal/provider/storage/filesystem"
	s3store "github.com/carlos-loya/archive-purge-restore/internal/provider/storage/s3"
	"github.com/carlos-loya/archive-purge-restore/internal/scheduler"
	"github.com/spf13/cobra"
)

var (
	version   = "dev"
	cfgFile   string
	logFormat string
	logLevel  string
	rootCmd   *cobra.Command
)

func init() {
	rootCmd = &cobra.Command{
		Use:   "apr",
		Short: "Archive-Purge-Restore: move old data to cheap storage",
		Long: `APR (Archive-Purge-Restore) is a CLI tool that archives old database rows
to object storage (S3, local filesystem) as Parquet files, deletes them
from the source database, and can restore them on demand.`,
	}

	rootCmd.PersistentFlags().StringVarP(&cfgFile, "config", "c", "", "config file (default: ./apr.yaml, ~/.apr/config.yaml, /etc/apr/config.yaml)")
	rootCmd.PersistentFlags().StringVar(&logFormat, "log-format", "text", "log output format (text|json)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "log level (debug|info|warn|error)")

	rootCmd.AddCommand(daemonCmd())
	rootCmd.AddCommand(archiveCmd())
	rootCmd.AddCommand(restoreCmd())
	rootCmd.AddCommand(verifyCmd())
	rootCmd.AddCommand(historyCmd())
	rootCmd.AddCommand(validateCmd())
	rootCmd.AddCommand(versionCmd())
}

func buildLogger() *slog.Logger {
	var level slog.Level
	switch strings.ToLower(logLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch strings.ToLower(logFormat) {
	case "json":
		handler = slog.NewJSONHandler(os.Stderr, opts)
	default:
		handler = slog.NewTextHandler(os.Stderr, opts)
	}

	return slog.New(handler)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func loadConfig() (*config.Config, error) {
	return config.Load(cfgFile)
}

func makeStorage(ctx context.Context, cfg config.StorageConfig) (storage.Provider, error) {
	switch cfg.Type {
	case "filesystem":
		return filesystem.New(cfg.Filesystem.BasePath)
	case "s3":
		return s3store.New(ctx, cfg.S3.Bucket, cfg.S3.Region, cfg.S3.Prefix, cfg.S3.Endpoint)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
	}
}

func makeDBProvider(src config.SourceConfig) (database.Provider, error) {
	user, pass := resolveCredentials(src.Credentials)
	switch src.Engine {
	case "postgres":
		return dbpg.New(src.Host, src.Port, src.Database, user, pass, src.SSLMode, src.Pool), nil
	case "mysql":
		return dbmysql.New(src.Host, src.Port, src.Database, user, pass, src.Pool), nil
	default:
		return nil, fmt.Errorf("unsupported engine: %s", src.Engine)
	}
}

func resolveCredentials(cred config.CredentialConfig) (user, pass string) {
	switch cred.Type {
	case "env":
		user = os.Getenv(cred.UsernameEnv)
		pass = os.Getenv(cred.PasswordEnv)
	case "static":
		user = cred.Username
		pass = cred.Password
	}
	return
}

func daemonCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Run as a daemon with built-in scheduler",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			store, err := makeStorage(ctx, cfg.Storage)
			if err != nil {
				return fmt.Errorf("creating storage: %w", err)
			}

			hist, err := history.NewStore(cfg.History.Path)
			if err != nil {
				return fmt.Errorf("opening history: %w", err)
			}
			defer hist.Close()

			logger := buildLogger()
			eng := engine.New(cfg, store, logger)
			sched := scheduler.NewStandard(logger.With("component", "scheduler"))

			for _, rule := range cfg.Rules {
				ruleCopy := rule
				err := sched.AddRule(ruleCopy, func(ctx context.Context, r config.Rule) error {
					db, err := makeDBProvider(r.Source)
					if err != nil {
						return err
					}
					if err := db.Connect(ctx); err != nil {
						return err
					}
					defer db.Close()

					result, archErr := eng.RunArchive(ctx, r.Name, db)
					recordHistory(hist, result, archErr)
					return archErr
				})
				if err != nil {
					return fmt.Errorf("scheduling rule %s: %w", rule.Name, err)
				}
			}

			fmt.Fprintf(os.Stderr, "apr daemon started with %d rules\n", len(cfg.Rules))
			return sched.Start(ctx)
		},
	}
}

func archiveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "archive [rule]",
		Short: "Manually trigger an archive run",
		Long:  "Run the archive process for all rules, or a specific rule by name.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}

			dryRun, _ := cmd.Flags().GetBool("dry-run")

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			store, err := makeStorage(ctx, cfg.Storage)
			if err != nil {
				return fmt.Errorf("creating storage: %w", err)
			}

			eng := engine.New(cfg, store, buildLogger())

			if dryRun {
				if len(args) == 1 {
					rule := cfg.FindRule(args[0])
					if rule == nil {
						return fmt.Errorf("rule %q not found", args[0])
					}
					db, err := makeDBProvider(rule.Source)
					if err != nil {
						return err
					}
					if err := db.Connect(ctx); err != nil {
						return err
					}
					defer db.Close()

					result, err := eng.RunArchiveDryRun(ctx, rule.Name, db)
					if err != nil {
						return err
					}
					printArchiveDryRunResult(result)
					return nil
				}

				results, err := eng.RunArchiveAllDryRun(ctx, func(src config.SourceConfig) (database.Provider, error) {
					return makeDBProvider(src)
				})
				for _, r := range results {
					printArchiveDryRunResult(r)
				}
				return err
			}

			hist, err := history.NewStore(cfg.History.Path)
			if err != nil {
				return fmt.Errorf("opening history: %w", err)
			}
			defer hist.Close()

			if len(args) == 1 {
				rule := cfg.FindRule(args[0])
				if rule == nil {
					return fmt.Errorf("rule %q not found", args[0])
				}
				db, err := makeDBProvider(rule.Source)
				if err != nil {
					return err
				}
				if err := db.Connect(ctx); err != nil {
					return err
				}
				defer db.Close()

				result, archErr := eng.RunArchive(ctx, rule.Name, db)
				recordHistory(hist, result, archErr)
				printRunResult(result)
				return archErr
			}

			results, err := eng.RunArchiveAll(ctx, func(src config.SourceConfig) (database.Provider, error) {
				return makeDBProvider(src)
			})
			for _, r := range results {
				recordHistory(hist, r, r.Error)
				printRunResult(r)
			}
			return err
		},
	}
	cmd.Flags().Bool("dry-run", false, "Preview what would be archived without making changes")
	return cmd
}

func restoreCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore archived data back to the database",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}

			ruleName, _ := cmd.Flags().GetString("rule")
			table, _ := cmd.Flags().GetString("table")
			date, _ := cmd.Flags().GetString("date")
			runID, _ := cmd.Flags().GetString("run-id")
			dryRun, _ := cmd.Flags().GetBool("dry-run")

			if ruleName == "" {
				return fmt.Errorf("--rule is required")
			}

			rule := cfg.FindRule(ruleName)
			if rule == nil {
				return fmt.Errorf("rule %q not found", ruleName)
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			store, err := makeStorage(ctx, cfg.Storage)
			if err != nil {
				return fmt.Errorf("creating storage: %w", err)
			}

			eng := engine.New(cfg, store, buildLogger())

			if dryRun {
				// For dry-run restore, we don't need a real DB connection since
				// we only list and read files from storage.
				result, restoreErr := eng.RunRestore(ctx, ruleName, table, date, runID, true, nil)
				if restoreErr != nil {
					return restoreErr
				}
				printRestoreDryRunResult(result)
				return nil
			}

			hist, err := history.NewStore(cfg.History.Path)
			if err != nil {
				return fmt.Errorf("opening history: %w", err)
			}
			defer hist.Close()

			db, err := makeDBProvider(rule.Source)
			if err != nil {
				return err
			}
			if err := db.Connect(ctx); err != nil {
				return err
			}
			defer db.Close()

			result, restoreErr := eng.RunRestore(ctx, ruleName, table, date, runID, false, db)
			recordRestoreHistory(hist, result, restoreErr)
			printRestoreResult(result)
			return restoreErr
		},
	}
	cmd.Flags().String("rule", "", "Rule name to restore from (required)")
	cmd.Flags().String("table", "", "Table name to restore (optional, all tables if empty)")
	cmd.Flags().String("date", "", "Date of archived data (YYYY-MM-DD)")
	cmd.Flags().String("run-id", "", "Specific run ID to restore")
	cmd.Flags().Bool("dry-run", false, "Preview what would be restored without making changes")
	return cmd
}

func verifyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "verify",
		Short: "Verify integrity of archived Parquet files",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}

			ruleName, _ := cmd.Flags().GetString("rule")
			table, _ := cmd.Flags().GetString("table")
			date, _ := cmd.Flags().GetString("date")
			runID, _ := cmd.Flags().GetString("run-id")

			if ruleName == "" {
				return fmt.Errorf("--rule is required")
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			store, err := makeStorage(ctx, cfg.Storage)
			if err != nil {
				return fmt.Errorf("creating storage: %w", err)
			}

			eng := engine.New(cfg, store, buildLogger())

			fmt.Printf("Verifying archives for rule %q...\n", ruleName)

			result, verifyErr := eng.RunVerify(ctx, ruleName, table, date, runID)
			if verifyErr != nil {
				return verifyErr
			}

			printVerifyResult(result)

			// Best-effort history comparison.
			hist, err := history.NewStore(cfg.History.Path)
			if err == nil {
				defer hist.Close()
				printHistoryComparison(hist, result)
			}

			return nil
		},
	}
	cmd.Flags().String("rule", "", "Rule name to verify (required)")
	cmd.Flags().String("table", "", "Table name to verify (optional, all tables if empty)")
	cmd.Flags().String("date", "", "Date of archived data (YYYY-MM-DD)")
	cmd.Flags().String("run-id", "", "Specific run ID to verify")
	return cmd
}

func historyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "history",
		Short: "View archive/restore execution history",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			rule, _ := cmd.Flags().GetString("rule")
			limit, _ := cmd.Flags().GetInt("limit")

			hist, err := history.NewStore(cfg.History.Path)
			if err != nil {
				return fmt.Errorf("opening history: %w", err)
			}
			defer hist.Close()

			events, err := hist.List(rule, limit)
			if err != nil {
				return fmt.Errorf("listing history: %w", err)
			}

			if len(events) == 0 {
				fmt.Println("No history events found.")
				return nil
			}

			w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
			fmt.Fprintln(w, "RUN ID\tRULE\tTYPE\tTABLE\tROWS\tSTATUS\tTIME")
			for _, e := range events {
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%s\t%s\n",
					e.RunID, e.Rule, e.EventType, e.Table, e.RowCount, e.Status,
					e.StartTime.Format("2006-01-02 15:04:05"))
			}
			return w.Flush()
		},
	}
	cmd.Flags().String("rule", "", "Filter by rule name")
	cmd.Flags().Int("limit", 20, "Maximum number of entries to show")
	return cmd
}

func validateCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "validate",
		Short: "Validate the configuration file",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := loadConfig()
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}
			if err := cfg.Validate(); err != nil {
				return fmt.Errorf("invalid config: %w", err)
			}
			fmt.Println("Configuration is valid.")
			fmt.Printf("  Storage: %s\n", cfg.Storage.Type)
			fmt.Printf("  Rules: %d\n", len(cfg.Rules))
			for _, r := range cfg.Rules {
				fmt.Printf("    - %s (%s, %d tables)\n", r.Name, r.Source.Engine, len(r.Tables))
			}
			return nil
		},
	}
}

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("apr version %s\n", version)
		},
	}
}

func recordHistory(hist *history.Store, result *engine.RunResult, err error) {
	if result == nil {
		return
	}
	for _, t := range result.Tables {
		status := "success"
		errMsg := ""
		if t.Error != nil {
			status = "error"
			errMsg = t.Error.Error()
		} else if err != nil {
			status = "error"
			errMsg = err.Error()
		}
		filesJSON, _ := json.Marshal(t.Files)
		hist.Record(history.Event{
			RunID:        result.RunID,
			Rule:         result.Rule,
			EventType:    history.EventArchive,
			Table:        t.Table,
			RowCount:     t.RowsArchived,
			Files:        string(filesJSON),
			Status:       status,
			ErrorMessage: errMsg,
			StartTime:    result.StartTime,
			EndTime:      result.EndTime,
		})
	}
}

func recordRestoreHistory(hist *history.Store, result *engine.RestoreResult, err error) {
	if result == nil {
		return
	}
	for _, t := range result.Tables {
		status := "success"
		errMsg := ""
		if t.Error != nil {
			status = "error"
			errMsg = t.Error.Error()
		} else if err != nil {
			status = "error"
			errMsg = err.Error()
		}
		filesJSON, _ := json.Marshal(t.Files)
		hist.Record(history.Event{
			RunID:        "restore",
			Rule:         result.Rule,
			EventType:    history.EventRestore,
			Table:        t.Table,
			RowCount:     t.RowsRestored,
			Files:        string(filesJSON),
			Status:       status,
			ErrorMessage: errMsg,
			StartTime:    result.StartTime,
			EndTime:      result.EndTime,
		})
	}
}

func printRunResult(r *engine.RunResult) {
	if r == nil {
		return
	}
	fmt.Printf("Archive run %s for rule %q:\n", r.RunID, r.Rule)
	fmt.Printf("  Duration: %v\n", r.EndTime.Sub(r.StartTime))
	for _, t := range r.Tables {
		fmt.Printf("  Table %s: %d rows archived, %d rows deleted\n",
			t.Table, t.RowsArchived, t.RowsDeleted)
		if len(t.Files) > 0 {
			fmt.Printf("    Files: %s\n", strings.Join(t.Files, ", "))
		}
	}
	if r.Error != nil {
		fmt.Printf("  Error: %v\n", r.Error)
	}
}

func printRestoreResult(r *engine.RestoreResult) {
	if r == nil {
		return
	}
	fmt.Printf("Restore for rule %q:\n", r.Rule)
	fmt.Printf("  Duration: %v\n", r.EndTime.Sub(r.StartTime))
	for _, t := range r.Tables {
		fmt.Printf("  Table %s: %d rows restored\n", t.Table, t.RowsRestored)
		if len(t.Files) > 0 {
			fmt.Printf("    Files: %s\n", strings.Join(t.Files, ", "))
		}
	}
	if r.Error != nil {
		fmt.Printf("  Error: %v\n", r.Error)
	}
}

func printArchiveDryRunResult(r *engine.DryRunResult) {
	if r == nil {
		return
	}
	fmt.Println("DRY RUN — no changes will be made")
	fmt.Println()
	fmt.Printf("Rule: %s\n", r.Rule)
	for _, t := range r.Tables {
		fmt.Printf("  %s: %d rows older than %s would be archived\n",
			t.Table, t.Count, t.Cutoff.Format("2006-01-02"))
	}
}

func printRestoreDryRunResult(r *engine.RestoreResult) {
	if r == nil {
		return
	}
	fmt.Println("DRY RUN — no changes will be made")
	fmt.Println()
	fmt.Printf("Rule: %s\n", r.Rule)
	for _, t := range r.Tables {
		fmt.Printf("  %s: %d file(s), %d rows would be restored\n",
			t.Table, len(t.Files), t.RowsRestored)
	}
}

func printVerifyResult(r *engine.VerifyResult) {
	if r == nil {
		return
	}

	var okCount, corruptCount int
	for _, t := range r.Tables {
		for _, f := range t.Files {
			switch f.Status {
			case "OK":
				okCount++
				fmt.Printf("  %s: OK (%s rows)\n", f.Key, formatCount(f.RowCount))
			case "CORRUPT":
				corruptCount++
				errMsg := "unknown error"
				if f.Error != nil {
					errMsg = f.Error.Error()
				}
				fmt.Printf("  %s: CORRUPT (%s)\n", f.Key, errMsg)
			}
		}
	}

	fmt.Println()
	fmt.Printf("Verified: %d file(s) OK, %d file(s) CORRUPT\n", okCount, corruptCount)
}

func printHistoryComparison(hist *history.Store, result *engine.VerifyResult) {
	events, err := hist.List(result.Rule, 1000)
	if err != nil || len(events) == 0 {
		return
	}

	// Build a map of run_id+table → expected row count from archive events.
	type runTable struct {
		RunID string
		Table string
	}
	expected := make(map[runTable]int64)
	for _, e := range events {
		if e.EventType != history.EventArchive || e.Status != "success" {
			continue
		}
		key := runTable{RunID: e.RunID, Table: e.Table}
		expected[key] += e.RowCount
	}

	if len(expected) == 0 {
		return
	}

	// Build a map of run_id+table → actual row count from verify results.
	// Extract run IDs from file keys (the segment before _NNN.parquet).
	actual := make(map[runTable]int)
	for _, t := range result.Tables {
		for _, f := range t.Files {
			rid := extractRunID(f.Key)
			if rid == "" {
				continue
			}
			key := runTable{RunID: rid, Table: t.Table}
			if f.Status == "OK" {
				actual[key] += f.RowCount
			}
		}
	}

	// Only print if there are matches to compare.
	var printed bool
	for key, exp := range expected {
		act, found := actual[key]
		if !found {
			continue
		}
		if !printed {
			fmt.Println()
			fmt.Println("History comparison:")
			printed = true
		}
		if int64(act) == exp {
			fmt.Printf("  Run %s, table %s: expected %s rows, found %s \u2713\n",
				key.RunID, key.Table, formatCount(int(exp)), formatCount(act))
		} else {
			fmt.Printf("  Run %s, table %s: expected %s rows, found %s\n",
				key.RunID, key.Table, formatCount(int(exp)), formatCount(act))
		}
	}
}

// extractRunID pulls the run ID from a parquet file key like "db/table/date/runid_000.parquet".
func extractRunID(key string) string {
	// Find the last slash to get the filename.
	lastSlash := -1
	for i := len(key) - 1; i >= 0; i-- {
		if key[i] == '/' {
			lastSlash = i
			break
		}
	}
	filename := key[lastSlash+1:]

	// Run ID is everything before the first underscore.
	for i := 0; i < len(filename); i++ {
		if filename[i] == '_' {
			return filename[:i]
		}
	}
	return ""
}

func formatCount(n int) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return formatCountWithCommas(n)
}

func formatCountWithCommas(n int) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result []byte
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result = append(result, ',')
		}
		result = append(result, byte(c))
	}
	return string(result)
}
