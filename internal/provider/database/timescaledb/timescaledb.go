package timescaledb

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/carlos-loya/archive-purge-restore/internal/config"
	dbpg "github.com/carlos-loya/archive-purge-restore/internal/provider/database/postgres"
)

// Provider implements database.Provider for TimescaleDB.
// It embeds the PostgreSQL provider and adds chunk-aware deletion
// via drop_chunks() for hypertables.
type Provider struct {
	*dbpg.Provider
	log *slog.Logger
}

// New creates a new TimescaleDB provider.
func New(host string, port int, dbname, user, password, sslMode string, pool config.PoolConfig, logger *slog.Logger) *Provider {
	if logger == nil {
		logger = slog.Default()
	}
	return &Provider{
		Provider: dbpg.New(host, port, dbname, user, password, sslMode, pool),
		log:      logger,
	}
}

// DeleteByTimeRange implements database.ChunkAwareDeleter. For hypertables,
// it uses drop_chunks() to efficiently remove all chunks whose time range
// falls entirely before the cutoff. For regular tables, it returns (0, nil).
func (p *Provider) DeleteByTimeRange(ctx context.Context, table, dateColumn string, before time.Time) (int, error) {
	isHT, err := p.isHypertable(ctx, table)
	if err != nil {
		p.log.Warn("failed to check hypertable status, skipping chunk-aware delete",
			"table", table, "error", err)
		return 0, nil
	}

	if !isHT {
		return 0, nil
	}

	// Count fully expired chunks before dropping for logging.
	chunks, err := p.getExpiredChunks(ctx, table, before)
	if err != nil {
		return 0, fmt.Errorf("querying expired chunks for %s: %w", table, err)
	}

	if len(chunks) == 0 {
		p.log.Debug("no fully expired chunks to drop", "table", table, "cutoff", before.Format("2006-01-02"))
		return 0, nil
	}

	// drop_chunks() drops all chunks whose range_end <= older_than.
	query := fmt.Sprintf(
		`SELECT drop_chunks('%s', $1)`,
		strings.ReplaceAll(table, "'", "''"),
	)

	rows, err := p.DB().QueryContext(ctx, query, before)
	if err != nil {
		return 0, fmt.Errorf("dropping chunks for %s: %w", table, err)
	}
	defer rows.Close()

	dropped := 0
	for rows.Next() {
		dropped++
	}
	if err := rows.Err(); err != nil {
		return dropped, fmt.Errorf("iterating drop_chunks result: %w", err)
	}

	p.log.Info("dropped chunks", "table", table, "chunks_dropped", dropped, "cutoff", before.Format("2006-01-02"))
	return dropped, nil
}

// isHypertable checks whether the given table is a TimescaleDB hypertable.
func (p *Provider) isHypertable(ctx context.Context, table string) (bool, error) {
	query := `SELECT EXISTS (
		SELECT 1 FROM timescaledb_information.hypertables
		WHERE hypertable_name = $1
	)`
	var exists bool
	err := p.DB().QueryRowContext(ctx, query, table).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("checking if %s is a hypertable: %w", table, err)
	}
	return exists, nil
}

// chunkInfo describes a TimescaleDB chunk's metadata.
type chunkInfo struct {
	chunkSchema string
	chunkName   string
	rangeStart  time.Time
	rangeEnd    time.Time
}

// getExpiredChunks returns chunks whose entire time range falls before the cutoff.
func (p *Provider) getExpiredChunks(ctx context.Context, table string, before time.Time) ([]chunkInfo, error) {
	query := `
		SELECT chunk_schema, chunk_name, range_start, range_end
		FROM timescaledb_information.chunks
		WHERE hypertable_name = $1
		  AND range_end <= $2
		ORDER BY range_start`

	rows, err := p.DB().QueryContext(ctx, query, table, before)
	if err != nil {
		return nil, fmt.Errorf("querying chunks for %s: %w", table, err)
	}
	defer rows.Close()

	var chunks []chunkInfo
	for rows.Next() {
		var c chunkInfo
		if err := rows.Scan(&c.chunkSchema, &c.chunkName, &c.rangeStart, &c.rangeEnd); err != nil {
			return nil, fmt.Errorf("scanning chunk info: %w", err)
		}
		chunks = append(chunks, c)
	}
	return chunks, rows.Err()
}
