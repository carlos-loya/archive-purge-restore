package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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
	version = "dev"
	cfgFile string
	rootCmd *cobra.Command
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

	rootCmd.AddCommand(daemonCmd())
	rootCmd.AddCommand(archiveCmd())
	rootCmd.AddCommand(restoreCmd())
	rootCmd.AddCommand(historyCmd())
	rootCmd.AddCommand(validateCmd())
	rootCmd.AddCommand(versionCmd())
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

			eng := engine.New(cfg, store)
			logger := log.New(os.Stderr, "[apr] ", log.LstdFlags)
			sched := scheduler.NewStandard(logger)

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

			eng := engine.New(cfg, store)

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

			eng := engine.New(cfg, store)

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
