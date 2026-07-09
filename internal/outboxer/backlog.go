package outboxer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/fvdsn/outboxer/internal/outboxer/provider"
)

// backlogProbeFallbackInterval throttles the backlog probe when the stats
// interval is disabled.
const backlogProbeFallbackInterval = 10 * time.Second

// updateBacklog maintains the backlog gauge after a committed batch. A drained
// batch (no route hit its share) proves the exact backlog with no query: it is
// what the batch left behind. Only a truncated batch leaves the depth unknown,
// and a relay with a truncated batch never goes idle, so the probe runs between
// batches on the main connection and can never displace the idle LISTEN.
func (a *app) updateBacklog(ctx context.Context, result batchResult) {
	if result.drained {
		a.stats.setBacklog(int64(result.stats.keptForRetry), false)
		return
	}
	if a.cfg.BacklogCountLimit <= 0 {
		// Probing disabled: the events kept for retry are still a true floor.
		a.stats.setBacklog(int64(result.stats.keptForRetry), true)
		return
	}
	if time.Since(a.lastBacklogProbe) < a.backlogProbeInterval() {
		return
	}
	a.lastBacklogProbe = time.Now()

	count, capped, err := a.countBacklog(ctx)
	if err != nil {
		a.logFailure(ctx, "Failed to count backlog", "backlog-probe", "error", err.Error())
		return
	}
	a.stats.setBacklog(count, capped)
}

func (a *app) backlogProbeInterval() time.Duration {
	if a.cfg.StatsInterval > 0 {
		return a.cfg.StatsInterval
	}
	return backlogProbeFallbackInterval
}

// countBacklog counts this relay's routable events among the oldest
// backlogCountLimit rows of the outbox table. The scan is bounded by the
// limit regardless of how many rows belong to other relays; capped reports
// whether the scan hit the limit, in which case the count is a floor.
func (a *app) countBacklog(ctx context.Context) (int64, bool, error) {
	ctx, cancel := provider.WithTimeout(ctx, a.cfg.PGQueryTimeout)
	defer cancel()

	var routable, scanned int64
	if err := a.db.QueryRowContext(ctx, a.backlogCountSQL(), a.cfg.BacklogCountLimit).Scan(&routable, &scanned); err != nil {
		return 0, false, err
	}
	return routable, scanned >= int64(a.cfg.BacklogCountLimit), nil
}

// backlogCountSQL scans the oldest $1 rows and counts the ones this relay
// would collect, using the same routing predicate builders as the collection
// query so the two cannot disagree. Sharded relays therefore report their own
// backlog, not the whole table's.
func (a *app) backlogCountSQL() string {
	alias := "backlog"
	targetExpr, targetPredicate := a.resolvedTargetSQL(alias)
	destinationExpr := a.resolvedDestinationSQL(alias, targetExpr)
	routablePredicate := a.routableSQL(targetExpr, destinationExpr, targetPredicate)

	// The inner scan carries only the columns the predicate can reference.
	columns := []string{columnSQL(alias, a.cfg.EventID)}
	if a.cfg.EventTarget != "" {
		columns = append(columns, columnSQL(alias, a.cfg.EventTarget))
	}
	if a.cfg.EventDestination != "" {
		columns = append(columns, columnSQL(alias, a.cfg.EventDestination))
	}

	return fmt.Sprintf(
		`SELECT count(*) FILTER (WHERE %s), count(*) `+
			`FROM (SELECT %s FROM %s AS %s ORDER BY %s LIMIT $1) AS %s`,
		routablePredicate,
		strings.Join(columns, ", "),
		qualifiedIdent(a.cfg.PGSchema, a.cfg.EventTable),
		ident(alias),
		columnSQL(alias, a.cfg.EventID),
		ident(alias),
	)
}

// batchDrained reports whether the batch selected everything that was pending
// for every route it saw. Each route with pending events gets an even share of
// the batch target (matching the collection query's LIMIT), so a route that
// filled its share may have been truncated and the remaining depth is unknown.
func batchDrained(events []event, target int) bool {
	if len(events) == 0 {
		return true
	}
	perRoute := map[eventRoute]int{}
	for _, evt := range events {
		perRoute[evt.route]++
	}
	share := (target + len(perRoute) - 1) / len(perRoute)
	share = max(1, share)
	for _, count := range perRoute {
		if count >= share {
			return false
		}
	}
	return true
}
