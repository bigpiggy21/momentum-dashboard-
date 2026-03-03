"""Remove condition-41-only trades (no condition 14) from sweep_trades and re-detect."""
import sqlite3, json

conn = sqlite3.connect('momentum_dashboard.db')
conn.row_factory = sqlite3.Row

# Find all trades that have condition 41 but NOT condition 14
all_trades = conn.execute("""
    SELECT id, ticker, trade_date, notional, conditions
    FROM sweep_trades
""").fetchall()

bad_ids = []
bad_notional = 0
for t in all_trades:
    conds = json.loads(t['conditions']) if t['conditions'] else []
    has_14 = 14 in conds
    has_41 = 41 in conds
    # If it has 41 but not 14, it's not a sweep
    if has_41 and not has_14:
        bad_ids.append(t['id'])
        bad_notional += t['notional']
    # Also if it has NO conditions at all, or only non-sweep conditions
    elif not has_14:
        bad_ids.append(t['id'])
        bad_notional += t['notional']

print(f"Total trades in DB: {len(all_trades)}")
print(f"Trades WITHOUT condition 14: {len(bad_ids)} (${bad_notional:,.0f})")
print(f"Trades WITH condition 14 (keepers): {len(all_trades) - len(bad_ids)}")

if bad_ids:
    # Delete in batches
    batch_size = 500
    for i in range(0, len(bad_ids), batch_size):
        batch = bad_ids[i:i+batch_size]
        placeholders = ",".join("?" * len(batch))
        conn.execute(f"DELETE FROM sweep_trades WHERE id IN ({placeholders})", batch)
    conn.commit()
    print(f"\nDeleted {len(bad_ids)} non-sweep trades")

    # Now find events that may need re-evaluation
    # Delete all monster events that came from cond-41-only trades
    # Simplest: clear today's events and re-detect
    deleted_events = conn.execute("""
        DELETE FROM clusterbomb_events WHERE event_date = '2026-03-03'
    """).rowcount
    conn.commit()
    print(f"Cleared {deleted_events} events for today (will re-detect)")

conn.close()

# Re-detect for today
from sweep_engine import detect_today
results = detect_today()
print(f"\nRe-detection results: {results}")
