"""Render a static HTML report from the DuckDB warehouse (for GitHub Pages).

Usage: uv run python scripts/build_report.py site/index.html
"""

import html
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb

DATA = Path(os.environ.get("TAXIFLOW_DATA", Path(__file__).resolve().parents[1] / "data"))
out = Path(sys.argv[1] if len(sys.argv) > 1 else "site/index.html")
con = duckdb.connect(str(DATA / "warehouse.duckdb"), read_only=True)


def q(sql):
    return con.execute(sql).df()


def table(df):
    return df.to_html(index=False, border=0, classes="t", na_rep="")


def bars(df, x, y):
    m = max(float(df[y].max()), 1.0)
    rows = "".join(
        f"<div class=b><span>{html.escape(str(r[x]))}</span>"
        f"<i style='width:{100 * float(r[y]) / m:.1f}%'></i><em>{int(r[y]):,}</em></div>"
        for _, r in df.iterrows()
    )
    return f"<div class=bars>{rows}</div>"


k = q(
    "select sum(trips) trips, round(sum(revenue),0) revenue, count(distinct pickup_date) d "
    "from marts.mart_zone_hourly_demand"
)
hourly = q(
    "select pickup_hour, sum(trips) trips from marts.mart_zone_hourly_demand group by 1 order by 1"
)
zones = q(
    "select zone_name, borough, sum(trips) trips, round(sum(revenue),0) revenue "
    "from marts.mart_zone_hourly_demand group by all order by trips desc limit 15"
)
dq = q("select * from marts.mart_dq_summary")
manifest = q("select * from raw.ingest_manifest order by file_name")
w = q("select * from marts.mart_stream_windows")
late = int(q("select count(*) n from staging.stg_stream_late").n[0])
sha = os.environ.get("GITHUB_SHA", "local")[:7]
now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

page = f"""<!doctype html><html lang=en><meta charset=utf-8>
<meta name=viewport content="width=device-width,initial-scale=1">
<title>TaxiFlow report</title><style>
body{{font:15px/1.5 system-ui,sans-serif;max-width:1000px;margin:auto;padding:1rem;color:#222}}
.k{{display:flex;gap:1rem;flex-wrap:wrap}} 
.k div{{background:#f3f5f8;padding:.8rem 1.2rem;border-radius:8px}}
.k b{{display:block;font-size:1.6rem}}
.t{{border-collapse:collapse;width:100%;font-size:13px;margin:.5rem 0 1.5rem}}
.t td,.t th{{padding:4px 8px;border-bottom:1px solid #ddd;text-align:left}}
.overflow{{overflow-x:auto}}.b{{display:flex;align-items:center;gap:6px;font-size:12px}}
.b span{{width:3em;text-align:right}}.b i{{background:#2a7de1;height:12px;display:block}}
small{{color:#666}}</style>
<h1>TaxiFlow: NYC taxi batch + streaming analytics</h1>
<p><small>Static report rebuilt by GitHub Actions on {now} from commit {sha}, using the
offline sample fixture. Source: <a href="https://github.com/dragon28/taxiflow">dragon28/taxiflow</a>.
The interactive version is the Streamlit dashboard (<code>make dashboard</code>).</small></p>
<h2>Batch demand</h2><div class=k>
<div>Valid trips<b>{int(k.trips[0]):,}</b></div>
<div>Revenue (excl. refunds)<b>${k.revenue[0]:,.0f}</b></div>
<div>Days<b>{int(k.d[0])}</b></div></div>
<h3>Trips by hour of day</h3>{bars(hourly, "pickup_hour", "trips")}
<h3>Top pickup zones</h3><div class=overflow>{table(zones)}</div>
<h2>Data quality</h2><p>Every raw row is either a valid fact or quarantined with a reason.</p>
<div class=overflow>{table(dq)}{table(manifest)}</div>
<h2>Stream (replay)</h2>
<p><small>Historical trips replayed as events. Not live NYC data.</small></p>
<div class=k><div>On-time unique events<b>{int(w.trips.sum()):,}</b></div>
<div>5-min windows<b>{len(w)}</b></div><div>Late events (quarantined)<b>{late}</b></div></div>
</html>"""
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(page)
print(f"wrote {out} ({len(page):,} bytes)")
