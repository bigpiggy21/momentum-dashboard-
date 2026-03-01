"""
Dashboard generator: computes all indicators across all tickers and timeframes,
detects changes, and produces an interactive HTML dashboard.
"""
import json
import os
from datetime import datetime, timedelta
from indicators import (
    TIMEFRAMES, aggregate_to_timeframe, compute_all_indicators,
    calc_30wma, generate_sample_data
)
from tickers import TICKER_UNIVERSE, get_all_groups


def compute_ticker_state(ticker: str, use_sample: bool = True):
    """
    Compute full indicator state for a ticker across all timeframes.
    Returns dict: {timeframe_name: {indicator_values}} + 30WMA state.
    """
    if use_sample:
        df_hourly = generate_sample_data(ticker, days=800)
    else:
        # TODO: pull from Massive.com API
        raise NotImplementedError("Live data not yet connected")
    
    results = {}
    
    for tf_name, tf_hours in TIMEFRAMES.items():
        try:
            df_tf = aggregate_to_timeframe(df_hourly, tf_hours)
            indicators = compute_all_indicators(df_tf)
            if indicators:
                results[tf_name] = indicators
        except Exception as e:
            results[tf_name] = None
    
    # 30WMA (weekly data)
    try:
        df_weekly = aggregate_to_timeframe(df_hourly, 168)
        wma_data = calc_30wma(df_weekly)
        results['_30wma'] = {
            'position': wma_data['position'].iloc[-1] if not wma_data['position'].empty else '',
            'crossed': wma_data['crossed'].iloc[-1] if not wma_data['crossed'].empty else '',
        }
    except:
        results['_30wma'] = {'position': '', 'crossed': ''}
    
    # Basic price data from hourly
    try:
        latest_price = df_hourly['close'].iloc[-1]
        prev_day_close = df_hourly['close'].iloc[-9] if len(df_hourly) > 9 else latest_price
        daily_pct = ((latest_price - prev_day_close) / prev_day_close) * 100
        results['_price'] = {
            'current': round(latest_price, 2),
            'daily_pct': round(daily_pct, 2),
        }
    except:
        results['_price'] = {'current': 0, 'daily_pct': 0}
    
    return results


def generate_sample_changes():
    """Generate realistic sample change events for demonstration."""
    changes = [
        {"time": "14:32", "ticker": "NVDA", "group": "SEMI INDIVIDUALS", "type": "SQZ", "tf": "1D",
         "detail": "Squeeze fired", "from": "black", "to": "green", "importance": "high"},
        {"time": "14:32", "ticker": "NVDA", "group": "SEMI INDIVIDUALS", "type": "BAND", "tf": "4H",
         "detail": "Band flipped bullish", "from": "↓", "to": "↑", "importance": "medium"},
        {"time": "13:15", "ticker": "GOLD", "group": "COMMODITIES - PRECIOUS METALS", "type": "DEV", "tf": "1W",
         "detail": "Deviation BUY signal", "from": "", "to": "buy", "importance": "high"},
        {"time": "12:45", "ticker": "TSLA", "group": "MAG 6", "type": "MOM", "tf": "8H",
         "detail": "Momentum turned positive-rising", "from": "yellow", "to": "aqua", "importance": "high"},
        {"time": "11:30", "ticker": "AMD", "group": "SEMI INDIVIDUALS", "type": "ACC", "tf": "1D",
         "detail": "Acceleration impulse ▲", "from": "red", "to": "green", "importance": "medium"},
        {"time": "11:02", "ticker": "META", "group": "MAG 6", "type": "SQZ", "tf": "4H",
         "detail": "Squeeze entered", "from": "green", "to": "black", "importance": "low"},
        {"time": "10:45", "ticker": "UBER", "group": "OTHER US TECH", "type": "MOM", "tf": "1D",
         "detail": "Momentum turned negative-falling", "from": "blue", "to": "red", "importance": "high"},
        {"time": "09:30", "ticker": "QQQ", "group": "NDX 100", "type": "30WMA", "tf": "W",
         "detail": "Crossed above 30WMA", "from": "below", "to": "above", "importance": "high"},
        {"time": "09:15", "ticker": "BABA", "group": "CHINA INDIVIDUALS", "type": "BAND", "tf": "1D",
         "detail": "Band flipped bearish", "from": "↑", "to": "↓", "importance": "medium"},
        {"time": "09:02", "ticker": "COIN", "group": "CRYPTO", "type": "SQZ", "tf": "1W",
         "detail": "High compression squeeze", "from": "red", "to": "orange", "importance": "high"},
        {"time": "08:45", "ticker": "TSM", "group": "SEMI INDIVIDUALS", "type": "DEV", "tf": "1D",
         "detail": "Deviation SELL signal", "from": "", "to": "sell", "importance": "high"},
        {"time": "08:30", "ticker": "SPX", "group": "S&P 500", "type": "MOM", "tf": "1H",
         "detail": "Momentum rising", "from": "red", "to": "yellow", "importance": "low"},
    ]
    return changes


def build_dashboard():
    """Build the complete HTML dashboard."""
    
    # Compute state for all tickers
    all_states = {}
    for group_name, group_data in TICKER_UNIVERSE.items():
        for ticker_name, ticker_info in group_data['tickers'].items():
            us_ticker = ticker_info['us']
            state = compute_ticker_state(us_ticker, use_sample=True)
            all_states[ticker_name] = state
    
    changes = generate_sample_changes()
    tf_names = list(TIMEFRAMES.keys())
    
    # Serialize data to JSON for the HTML
    dashboard_data = {
        'universe': {},
        'states': {},
        'changes': changes,
        'timeframes': tf_names,
        'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    }
    
    for group_name, group_data in TICKER_UNIVERSE.items():
        dashboard_data['universe'][group_name] = list(group_data['tickers'].keys())
    
    import numpy as np
    def convert_val(v):
        if v is None:
            return None
        if isinstance(v, (np.bool_,)):
            return bool(v)
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, (bool, int, float, str)):
            return v
        return str(v)
    
    for ticker_name, state in all_states.items():
        serialized = {}
        for key, val in state.items():
            if isinstance(val, dict):
                serialized[key] = {k: convert_val(v) for k, v in val.items()}
            else:
                serialized[key] = convert_val(val)
        dashboard_data['states'][ticker_name] = serialized
    
    return generate_html(dashboard_data)


def generate_html(data: dict) -> str:
    """Generate the full HTML dashboard."""
    
    data_json = json.dumps(data, default=str)
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Momentum Dashboard</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@300;400;500;600;700&family=Space+Grotesk:wght@300;400;500;600;700&display=swap');

* {{ margin: 0; padding: 0; box-sizing: border-box; }}

:root {{
    --bg-primary: #0a0b0f;
    --bg-secondary: #12131a;
    --bg-card: #181921;
    --bg-hover: #1e2030;
    --border: #2a2b3d;
    --text-primary: #e8e9ed;
    --text-secondary: #8b8d9e;
    --text-dim: #555670;
    
    --aqua: #00e5ff;
    --blue: #2962ff;
    --yellow: #ffd600;
    --red: #ff1744;
    --green: #00c853;
    --orange: #ff9100;
    --black-sqz: #1a1a2e;
    --green-sqz: #00c853;
    
    --dev-buy: #0b570f;
    --dev-sell: #700909;
    
    --importance-high: #ff1744;
    --importance-medium: #ff9100;
    --importance-low: #555670;
    
    --band-flip: #1a3a6b;
}}

body {{
    background: var(--bg-primary);
    color: var(--text-primary);
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    line-height: 1.4;
    overflow-x: hidden;
}}

/* ========== HEADER ========== */
.header {{
    position: sticky;
    top: 0;
    z-index: 100;
    background: linear-gradient(180deg, var(--bg-primary) 0%, rgba(10,11,15,0.95) 100%);
    backdrop-filter: blur(12px);
    border-bottom: 1px solid var(--border);
    padding: 12px 24px;
    display: flex;
    justify-content: space-between;
    align-items: center;
}}

.header-left {{
    display: flex;
    align-items: center;
    gap: 16px;
}}

.logo {{
    font-size: 16px;
    font-weight: 700;
    letter-spacing: -0.5px;
    background: linear-gradient(135deg, var(--aqua), var(--blue));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}}

.header-time {{
    color: var(--text-secondary);
    font-size: 11px;
}}

.header-status {{
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--text-secondary);
}}

.status-dot {{
    width: 6px; height: 6px;
    border-radius: 50%;
    background: var(--green);
    animation: pulse 2s infinite;
}}

@keyframes pulse {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.4; }}
}}

/* ========== FILTER BAR ========== */
.filter-bar {{
    position: sticky;
    top: 49px;
    z-index: 99;
    background: var(--bg-secondary);
    border-bottom: 1px solid var(--border);
    padding: 8px 24px;
    display: flex;
    gap: 8px;
    align-items: center;
    flex-wrap: wrap;
}}

.filter-btn {{
    padding: 4px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: transparent;
    color: var(--text-secondary);
    font-family: inherit;
    font-size: 11px;
    cursor: pointer;
    transition: all 0.15s;
}}

.filter-btn:hover {{ background: var(--bg-hover); color: var(--text-primary); }}
.filter-btn.active {{ background: var(--blue); border-color: var(--blue); color: white; }}

.filter-label {{
    color: var(--text-dim);
    font-size: 10px;
    text-transform: uppercase;
    letter-spacing: 1px;
    margin-right: 4px;
}}

.filter-separator {{
    width: 1px;
    height: 20px;
    background: var(--border);
    margin: 0 8px;
}}

.search-input {{
    padding: 4px 10px;
    border: 1px solid var(--border);
    border-radius: 4px;
    background: var(--bg-primary);
    color: var(--text-primary);
    font-family: inherit;
    font-size: 11px;
    width: 160px;
    outline: none;
}}

.search-input:focus {{ border-color: var(--blue); }}

/* ========== CHANGES PANEL ========== */
.changes-panel {{
    margin: 16px 24px;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 8px;
    overflow: hidden;
}}

.changes-header {{
    padding: 12px 16px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-bottom: 1px solid var(--border);
    background: var(--bg-secondary);
}}

.changes-title {{
    font-size: 13px;
    font-weight: 600;
    color: var(--text-primary);
}}

.changes-count {{
    background: var(--red);
    color: white;
    padding: 2px 8px;
    border-radius: 10px;
    font-size: 10px;
    font-weight: 600;
}}

.changes-list {{
    max-height: 320px;
    overflow-y: auto;
}}

.change-item {{
    display: grid;
    grid-template-columns: 50px 70px 56px 42px 1fr 100px;
    align-items: center;
    padding: 8px 16px;
    border-bottom: 1px solid rgba(42,43,61,0.4);
    transition: background 0.1s;
    cursor: default;
}}

.change-item:hover {{ background: var(--bg-hover); }}

.change-time {{ color: var(--text-dim); font-size: 11px; }}
.change-ticker {{ font-weight: 600; color: var(--text-primary); }}

.change-type {{
    font-size: 10px;
    padding: 2px 6px;
    border-radius: 3px;
    text-align: center;
    font-weight: 500;
}}

.change-type.MOM {{ background: rgba(0,229,255,0.15); color: var(--aqua); }}
.change-type.SQZ {{ background: rgba(255,145,0,0.15); color: var(--orange); }}
.change-type.BAND {{ background: rgba(41,98,255,0.15); color: #5c8aff; }}
.change-type.ACC {{ background: rgba(0,200,83,0.15); color: var(--green); }}
.change-type.DEV {{ background: rgba(255,23,68,0.15); color: var(--red); }}
.change-type._30WMA {{ background: rgba(255,214,0,0.15); color: var(--yellow); }}

.change-tf {{
    color: var(--text-secondary);
    font-size: 11px;
    text-align: center;
}}

.change-detail {{ color: var(--text-secondary); font-size: 11px; }}

.change-arrow {{
    text-align: right;
    font-size: 11px;
}}

.change-from {{ color: var(--text-dim); }}
.change-to {{ font-weight: 500; }}

.importance-dot {{
    display: inline-block;
    width: 5px; height: 5px;
    border-radius: 50%;
    margin-right: 6px;
}}

.importance-high {{ background: var(--importance-high); }}
.importance-medium {{ background: var(--importance-medium); }}
.importance-low {{ background: var(--importance-low); }}

/* ========== THEMATIC GROUPS ========== */
.group-section {{
    margin: 12px 24px;
}}

.group-header {{
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 0;
    cursor: pointer;
    user-select: none;
}}

.group-toggle {{
    color: var(--text-dim);
    font-size: 10px;
    transition: transform 0.2s;
    width: 16px;
}}

.group-toggle.open {{ transform: rotate(90deg); }}

.group-name {{
    font-size: 11px;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 1.5px;
    color: var(--text-secondary);
}}

.group-count {{
    color: var(--text-dim);
    font-size: 10px;
}}

.group-body {{
    overflow: hidden;
    transition: max-height 0.3s ease;
}}

/* ========== TICKER TABLE ========== */
.ticker-table {{
    width: 100%;
    border-collapse: collapse;
    background: var(--bg-card);
    border: 1px solid var(--border);
    border-radius: 6px;
    overflow: hidden;
    margin-bottom: 4px;
}}

.ticker-table th {{
    background: var(--bg-secondary);
    padding: 6px 4px;
    font-size: 10px;
    font-weight: 500;
    color: var(--text-dim);
    text-transform: uppercase;
    letter-spacing: 0.5px;
    border-bottom: 1px solid var(--border);
    position: sticky;
    top: 89px;
    z-index: 10;
}}

.ticker-table th.tf-header {{ min-width: 38px; text-align: center; }}
.ticker-table th.ticker-header {{ min-width: 80px; text-align: left; padding-left: 12px; }}
.ticker-table th.price-header {{ min-width: 70px; text-align: right; padding-right: 8px; }}
.ticker-table th.wma-header {{ min-width: 50px; text-align: center; }}
.ticker-table th.row-label {{ min-width: 40px; text-align: center; }}

.ticker-block {{
    border-bottom: 2px solid var(--border);
}}

.ticker-block:last-child {{ border-bottom: none; }}

/* Ticker name row */
.ticker-name-row td {{
    padding: 6px 4px;
    background: rgba(18,19,26,0.6);
}}

.ticker-label {{
    font-weight: 700;
    font-size: 12px;
    padding-left: 12px;
    color: var(--text-primary);
}}

.ticker-price {{
    text-align: right;
    padding-right: 8px;
    font-size: 11px;
}}

.price-positive {{ color: var(--green); }}
.price-negative {{ color: var(--red); }}

.wma-badge {{
    display: inline-block;
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 9px;
    font-weight: 600;
    text-align: center;
}}

.wma-above {{ background: rgba(0,200,83,0.2); color: var(--green); }}
.wma-below {{ background: rgba(255,23,68,0.2); color: var(--red); }}
.wma-crossed {{ animation: flash 1s ease 3; }}

@keyframes flash {{
    0%, 100% {{ opacity: 1; }}
    50% {{ opacity: 0.3; }}
}}

/* Indicator rows */
.indicator-row td {{
    padding: 3px 2px;
    text-align: center;
    font-size: 11px;
    border-bottom: 1px solid rgba(42,43,61,0.3);
}}

.indicator-row .row-label-cell {{
    color: var(--text-dim);
    font-size: 9px;
    font-weight: 600;
    letter-spacing: 0.5px;
    text-align: center;
    width: 40px;
    background: rgba(18,19,26,0.4);
}}

/* Cell coloring */
.cell-mom {{
    color: white;
    font-weight: 500;
    font-size: 13px;
    border-radius: 2px;
}}

.cell-mom.aqua {{ background: var(--aqua); color: #0a0b0f; }}
.cell-mom.blue {{ background: var(--blue); }}
.cell-mom.yellow {{ background: var(--yellow); color: #0a0b0f; }}
.cell-mom.red {{ background: var(--red); }}

.cell-sqz {{
    border-radius: 2px;
    min-height: 22px;
}}

.cell-sqz.green {{ background: var(--green-sqz); }}
.cell-sqz.black {{ background: var(--black-sqz); border: 1px solid #333; }}
.cell-sqz.red {{ background: var(--red); }}
.cell-sqz.orange {{ background: var(--orange); }}

.cell-band {{
    color: white;
    font-weight: 600;
    font-size: 13px;
}}

.cell-band.flipped {{
    background: var(--band-flip) !important;
}}

.cell-acc {{
    font-weight: 600;
    font-size: 12px;
}}

.cell-acc.green {{ background: rgba(0,200,83,0.3); color: var(--green); }}
.cell-acc.red {{ background: rgba(255,23,68,0.3); color: var(--red); }}

.cell-dev {{ font-weight: 600; font-size: 12px; }}
.cell-dev.buy {{ background: var(--dev-buy); color: var(--green); }}
.cell-dev.sell {{ background: var(--dev-sell); color: var(--red); }}

/* Cell with recent change indicator */
.cell-changed {{
    position: relative;
}}

.cell-changed::after {{
    content: '';
    position: absolute;
    top: 2px;
    right: 2px;
    width: 4px;
    height: 4px;
    border-radius: 50%;
    background: var(--aqua);
}}

/* ========== SCROLLBAR ========== */
::-webkit-scrollbar {{ width: 6px; }}
::-webkit-scrollbar-track {{ background: var(--bg-primary); }}
::-webkit-scrollbar-thumb {{ background: var(--border); border-radius: 3px; }}
::-webkit-scrollbar-thumb:hover {{ background: var(--text-dim); }}

/* ========== FOOTER ========== */
.footer {{
    padding: 16px 24px;
    text-align: center;
    color: var(--text-dim);
    font-size: 10px;
    border-top: 1px solid var(--border);
    margin-top: 24px;
}}
</style>
</head>
<body>

<div class="header">
    <div class="header-left">
        <div class="logo">MOMENTUM</div>
        <div class="header-time" id="header-time">Loading...</div>
    </div>
    <div class="header-status">
        <span class="status-dot"></span>
        <span>SAMPLE DATA — Connect Massive.com for live</span>
    </div>
</div>

<div class="filter-bar">
    <span class="filter-label">Show changes:</span>
    <button class="filter-btn active" data-filter="all">All</button>
    <button class="filter-btn" data-filter="high">High Only</button>
    <button class="filter-btn" data-filter="1D+">Daily+</button>
    <button class="filter-btn" data-filter="1W+">Weekly+</button>
    <div class="filter-separator"></div>
    <span class="filter-label">Signal type:</span>
    <button class="filter-btn active" data-signal="all">All</button>
    <button class="filter-btn" data-signal="SQZ">SQZ</button>
    <button class="filter-btn" data-signal="MOM">MOM</button>
    <button class="filter-btn" data-signal="BAND">BAND</button>
    <button class="filter-btn" data-signal="DEV">DEV</button>
    <div class="filter-separator"></div>
    <input type="text" class="search-input" placeholder="Search ticker..." id="ticker-search">
</div>

<div class="changes-panel">
    <div class="changes-header">
        <div class="changes-title">⚡ Changes Since Last Check</div>
        <div class="changes-count" id="changes-count">0</div>
    </div>
    <div class="changes-list" id="changes-list"></div>
</div>

<div id="groups-container"></div>

<div class="footer">
    Generated: <span id="gen-time"></span> · Prototype with sample data · Calculations replicate TradingView Pine Script indicators
</div>

<script>
const DATA = {data_json};

const TF_ORDER = DATA.timeframes;
const TF_WEIGHT = {{}};
const WEIGHT_LIST = ['1H','4H','8H','1D','2D','3D','5D','1W','2W','1M','6W','3M','6M','12M'];
WEIGHT_LIST.forEach((tf, i) => TF_WEIGHT[tf] = i);

// ========== RENDER CHANGES ==========
function renderChanges(filter, signalFilter, searchTerm) {{
    const list = document.getElementById('changes-list');
    let changes = DATA.changes;
    
    if (filter === 'high') changes = changes.filter(c => c.importance === 'high');
    if (filter === '1D+') changes = changes.filter(c => TF_WEIGHT[c.tf] >= TF_WEIGHT['1D']);
    if (filter === '1W+') changes = changes.filter(c => TF_WEIGHT[c.tf] >= TF_WEIGHT['1W']);
    if (signalFilter !== 'all') changes = changes.filter(c => c.type === signalFilter);
    if (searchTerm) changes = changes.filter(c => c.ticker.toLowerCase().includes(searchTerm.toLowerCase()));
    
    list.innerHTML = changes.map(c => `
        <div class="change-item">
            <span class="change-time"><span class="importance-dot importance-${{c.importance}}"></span>${{c.time}}</span>
            <span class="change-ticker">${{c.ticker}}</span>
            <span class="change-type ${{c.type}}">${{c.type === '_30WMA' ? '30W' : c.type}}</span>
            <span class="change-tf">${{c.tf}}</span>
            <span class="change-detail">${{c.detail}}</span>
            <span class="change-arrow"><span class="change-from">${{c.from}}</span> → <span class="change-to">${{c.to}}</span></span>
        </div>
    `).join('');
    
    document.getElementById('changes-count').textContent = changes.length;
}}

// ========== RENDER GROUPS ==========
function renderGroups(searchTerm) {{
    const container = document.getElementById('groups-container');
    container.innerHTML = '';
    
    for (const [groupName, tickers] of Object.entries(DATA.universe)) {{
        let filteredTickers = tickers;
        if (searchTerm) {{
            filteredTickers = tickers.filter(t => t.toLowerCase().includes(searchTerm.toLowerCase()));
            if (filteredTickers.length === 0) continue;
        }}
        
        const section = document.createElement('div');
        section.className = 'group-section';
        
        // Group header
        const header = document.createElement('div');
        header.className = 'group-header';
        header.innerHTML = `
            <span class="group-toggle open">▶</span>
            <span class="group-name">${{groupName}}</span>
            <span class="group-count">${{filteredTickers.length}}</span>
        `;
        
        const body = document.createElement('div');
        body.className = 'group-body';
        
        header.addEventListener('click', () => {{
            const toggle = header.querySelector('.group-toggle');
            toggle.classList.toggle('open');
            body.style.display = body.style.display === 'none' ? 'block' : 'none';
        }});
        
        // Build table
        const table = document.createElement('table');
        table.className = 'ticker-table';
        
        // Header row
        let headerHtml = '<thead><tr>';
        headerHtml += '<th class="ticker-header">Ticker</th>';
        headerHtml += '<th class="row-label">Row</th>';
        TF_ORDER.forEach(tf => {{ headerHtml += `<th class="tf-header">${{tf}}</th>`; }});
        headerHtml += '<th class="price-header">Price</th>';
        headerHtml += '<th class="wma-header">30W</th>';
        headerHtml += '</tr></thead>';
        table.innerHTML = headerHtml;
        
        const tbody = document.createElement('tbody');
        
        filteredTickers.forEach(ticker => {{
            const state = DATA.states[ticker];
            if (!state) return;
            
            const price = state._price || {{ current: 0, daily_pct: 0 }};
            const wma = state._30wma || {{ position: '', crossed: '' }};
            const pctClass = price.daily_pct >= 0 ? 'price-positive' : 'price-negative';
            const pctSign = price.daily_pct >= 0 ? '+' : '';
            const wmaClass = wma.position === 'above' ? 'wma-above' : 'wma-below';
            const wmaCrossed = wma.crossed ? ' wma-crossed' : '';
            
            const rows = ['MOM', 'SQZ', 'BAND', 'ACC', 'DEV'];
            
            rows.forEach((rowType, rowIdx) => {{
                const tr = document.createElement('tr');
                tr.className = 'indicator-row';
                
                // Ticker label (only on first row, spanning visually)
                if (rowIdx === 0) {{
                    tr.innerHTML += `<td class="ticker-label" rowspan="${{rows.length}}">${{ticker}}</td>`;
                }}
                
                // Row label
                tr.innerHTML += `<td class="row-label-cell">${{rowType}}</td>`;
                
                // Timeframe cells
                TF_ORDER.forEach(tf => {{
                    const tfState = state[tf];
                    if (!tfState) {{
                        tr.innerHTML += '<td>—</td>';
                        return;
                    }}
                    
                    let cellHtml = '';
                    switch(rowType) {{
                        case 'MOM':
                            const momColor = tfState.mom_color || '';
                            const momDir = tfState.mom_direction || '';
                            cellHtml = `<td class="cell-mom ${{momColor}}">${{momDir}}</td>`;
                            break;
                        case 'SQZ':
                            const sqzState = tfState.sqz_state || '';
                            cellHtml = `<td class="cell-sqz ${{sqzState}}">&nbsp;</td>`;
                            break;
                        case 'BAND':
                            const bandPos = tfState.band_position || '';
                            const flipped = tfState.band_flipped ? ' flipped' : '';
                            cellHtml = `<td class="cell-band${{flipped}}">${{bandPos}}</td>`;
                            break;
                        case 'ACC':
                            const accColor = tfState.acc_color || '';
                            const accImp = tfState.acc_impulse || '';
                            cellHtml = `<td class="cell-acc ${{accColor}}">${{accImp}}</td>`;
                            break;
                        case 'DEV':
                            const devSig = tfState.dev_signal || '';
                            const devClass = devSig ? devSig : '';
                            const devText = devSig === 'buy' ? 'B' : devSig === 'sell' ? 'S' : '';
                            cellHtml = `<td class="cell-dev ${{devClass}}">${{devText}}</td>`;
                            break;
                    }}
                    tr.innerHTML += cellHtml;
                }});
                
                // Price and WMA (only on first row)
                if (rowIdx === 0) {{
                    tr.innerHTML += `<td class="ticker-price ${{pctClass}}" rowspan="${{rows.length}}">${{price.current}}<br><small>${{pctSign}}${{price.daily_pct}}%</small></td>`;
                    tr.innerHTML += `<td rowspan="${{rows.length}}"><span class="wma-badge ${{wmaClass}}${{wmaCrossed}}">${{wma.position}}</span></td>`;
                }}
                
                tbody.appendChild(tr);
            }});
            
            // Add separator
            const sep = document.createElement('tr');
            sep.innerHTML = `<td colspan="${{TF_ORDER.length + 4}}" style="height:4px;background:var(--bg-primary);"></td>`;
            tbody.appendChild(sep);
        }});
        
        table.appendChild(tbody);
        body.appendChild(table);
        section.appendChild(header);
        section.appendChild(body);
        container.appendChild(section);
    }});
}}

// ========== FILTER HANDLERS ==========
let currentFilter = 'all';
let currentSignal = 'all';
let currentSearch = '';

document.querySelectorAll('.filter-btn[data-filter]').forEach(btn => {{
    btn.addEventListener('click', () => {{
        document.querySelectorAll('.filter-btn[data-filter]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentFilter = btn.dataset.filter;
        renderChanges(currentFilter, currentSignal, currentSearch);
    }});
}});

document.querySelectorAll('.filter-btn[data-signal]').forEach(btn => {{
    btn.addEventListener('click', () => {{
        document.querySelectorAll('.filter-btn[data-signal]').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        currentSignal = btn.dataset.signal;
        renderChanges(currentFilter, currentSignal, currentSearch);
    }});
}});

document.getElementById('ticker-search').addEventListener('input', (e) => {{
    currentSearch = e.target.value;
    renderChanges(currentFilter, currentSignal, currentSearch);
    renderGroups(currentSearch);
}});

// ========== INIT ==========
document.getElementById('header-time').textContent = new Date().toLocaleString('en-GB', {{
    weekday: 'short', day: 'numeric', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit'
}});
document.getElementById('gen-time').textContent = DATA.generated_at;

renderChanges(currentFilter, currentSignal, currentSearch);
renderGroups(currentSearch);
</script>
</body>
</html>"""
    
    return html


if __name__ == '__main__':
    print("Computing indicators for all tickers...")
    html = build_dashboard()
    
    output_path = '/mnt/user-data/outputs/momentum_dashboard.html'
    with open(output_path, 'w') as f:
        f.write(html)
    
    print(f"Dashboard saved to {output_path}")
