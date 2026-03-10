/* ====================================================================
   SHARED HEADER — Injects the global navigation bar (row 1)

   Usage: Include in each page AFTER a <div id="headerRow1"></div> placeholder:
     <div id="headerRow1"></div>
     <script src="/static/header.js"></script>

   Set window._activePage before including to highlight the current page.
   Valid values: 'settings','watchlists','log','search','bollingers','rs','hvc','sweeps','analysis','report','backtest','portfolio','home'
   ==================================================================== */

(function(){
  var active = window._activePage || '';

  function btn(href, title, label, page, extra){
    var cls = 'page-btn' + (active === page ? ' page-active' : '') + (extra ? ' ' + extra : '');
    return '<a class="'+cls+'" href="'+href+'" title="'+title+'">'+label+'</a>';
  }

  // Read saved brand from localStorage
  var _brands = ['TBD','Lace','Pigs'];
  var _bIdx = parseInt(localStorage.getItem('brandIdx') || '0', 10);
  if(_bIdx < 0 || _bIdx >= _brands.length) _bIdx = 0;
  var _brandPrefix = _brands[_bIdx];

  var html = ''
    + '<div class="header-left">'

    // Logo
    + '<h1 class="logo-clickable" onclick="window.location.href=\'/\'" title="' + _brandPrefix + ' Technologies">'
    + '<svg class="logo-icon" width="26" height="28" viewBox="0 0 100 100" fill="none" xmlns="http://www.w3.org/2000/svg" style="vertical-align:middle;margin-right:6px;">'
    + '<path d="M8 78 L30 38 L42 55 L52 32 L68 58 L92 78Z" fill="#b4842a" opacity="0.25"/>'
    + '<path d="M15 78 L42 22 L55 48 L62 18 L85 78Z" fill="#f59e0b" opacity="0.75"/>'
    + '<path d="M62 18 L55 42 L58 35 L65 46 L72 38 L78 52 L68 44Z" fill="#fde68a" opacity="0.6"/>'
    + '<path d="M42 22 L38 38 L44 32 L50 42 L55 48Z" fill="#fde68a" opacity="0.5"/>'
    + '</svg> ' + _brandPrefix + ' <span class="accent">Technologies</span></h1>'

    // ? button (links to home / about)
    + '<a class="page-btn page-help-btn" href="/" title="Home">?</a>'

    // Darkpool Intelligence
    + '<div class="header-divider"></div>'
    + '<div class="header-section">'
    + '<div class="section-label">DARKPOOL INTELLIGENCE</div>'
    + '<div class="section-controls">'
    + btn('/sweeps', 'Sweep Tracker', '&#x1F4A3; Sweeps', 'sweeps')
    + btn('/analysis', 'Sweep Analysis', '&#x1F4CA; Analysis', 'analysis')
    + '</div></div>'

    // Strategies
    + '<div class="header-divider"></div>'
    + '<div class="header-section">'
    + '<div class="section-label">STRATEGIES</div>'
    + '<div class="section-controls">'
    + btn('/rs', 'Relative Strength', 'Relative Strength', 'rs')
    + btn('/hvc', 'High Volume Candles', 'High Volume Candles', 'hvc')
    + btn('/bollingers', 'Bollinger Strategy', 'Bollingers', 'bollingers')
    + '</div></div>'

    // Tools
    + '<div class="header-divider"></div>'
    + '<div class="header-section">'
    + '<div class="section-label">TOOLS</div>'
    + '<div class="section-controls">'
    + btn('/watchlists', 'Manage Watchlists', '&#x1F4CB; Watchlists', 'watchlists')
    + btn('/log', 'Event Log', '&#x26A1; Log', 'log')
    + btn('/search', 'Screener', '&#x1F50D; Screener', 'search')
    + btn('/backtest', 'Backtesting', '&#x1F9EA; Backtest', 'backtest')
    + '</div></div>'

    + '</div>'

    // Right side: Last Scan + Scheduler button (no header, darker)
    + '<div class="header-right">'
    + '<div class="status">Last scan: <span id="lastScan">--</span><br>'
    + '<span class="live" id="statusDot">&#x25CF;</span> <span id="statusText">Loading...</span></div>'
    + '<a class="page-btn scheduler-btn" href="/settings" title="Scheduler Settings">&#x1F4E1;</a>'
    + '</div>';

  var el = document.getElementById('headerRow1');
  if(el){
    el.className = 'header-row1';
    el.innerHTML = html;
  }
})();
