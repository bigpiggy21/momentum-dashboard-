# Taking Momentum Dashboard Online — Research Notes

## Current State (Local)
- Python HTTP server (app.py) serving HTML templates
- SQLite database (single file, ~1GB)
- CSV price cache fetched from Polygon API
- Everything runs on one machine, single user

---

## 1. Website / Hosting

### What you need
- A server (or cloud service) that runs Python and serves the web app
- A domain name (e.g. tbdtechnologies.com)
- HTTPS certificate (free via Let's Encrypt)

### Options (simplest → most scalable)

| Option | Cost | Effort | Notes |
|--------|------|--------|-------|
| **VPS (Hetzner, DigitalOcean, Linode)** | ~$10-20/mo | Medium | Full control. Run exactly what you have now. Best starting point. |
| **Railway / Render / Fly.io** | ~$5-25/mo | Low | Deploy from Git. Handles HTTPS, scaling. Good for getting started fast. |
| **AWS/GCP/Azure** | Variable | High | Overkill initially. Makes sense at scale (100+ users). |

### Recommendation
Start with a **VPS** (Hetzner is cheapest, ~$5/mo for 2GB RAM). Deploy your existing code almost as-is. Add nginx in front for HTTPS + static file serving.

### What needs to change
- SQLite → **PostgreSQL** (SQLite doesn't handle concurrent writes from multiple users)
- CSV file cache → needs a shared storage approach (see section 3)
- app.py HTTP server → wrap with **gunicorn** or switch to **Flask/FastAPI** (your current BaseHTTPRequestHandler won't handle concurrent requests well)

---

## 2. User Accounts / Authentication

### What's needed
- User registration (email + password)
- Login sessions (JWT tokens or server-side sessions)
- Per-user saved configs, watchlists, settings
- Password reset flow (requires email sending)

### Build vs Buy

| Approach | Pros | Cons |
|----------|------|------|
| **Build it yourself** | Full control, no dependencies | Security risk if done wrong (password hashing, session management, CSRF). Takes time. |
| **Auth provider (Auth0, Clerk, Supabase Auth)** | Battle-tested security, social login (Google/GitHub), handles password reset. | Monthly cost at scale. Some lock-in. |
| **Supabase** | Free tier generous. Auth + PostgreSQL + API all-in-one. | Couples you to their platform. |

### Recommendation
Use **Supabase Auth** or **Clerk** for authentication. They handle the hard security stuff (bcrypt hashing, JWT, email verification, password reset). You focus on the product. Free tiers are generous (50K monthly active users on Supabase).

### Database changes for multi-user
```
users table:
  id, email, password_hash, created_at, plan_tier

user_configs table:
  id, user_id, name, payload_json, created_at

user_watchlists table:
  id, user_id, name, tickers_json

backtest_cache:
  add user_id column (each user's cache is separate)
```

### Things to think about
- **Free vs paid tiers**: Do you want to monetise? Could offer free tier (limited tickers/backtests per day) and paid tier (full access).
- **Rate limiting**: Prevent abuse. Limit API calls per user per minute.
- **Data isolation**: Each user should only see their own saved configs/watchlists.

---

## 3. Data Pipeline / Price Data

### Current approach
- Polygon API → CSV files on disk → indicators computed locally
- ~3,000 tickers, each with daily OHLCV
- Full recompute takes ~36 minutes

### The problem at scale
- You can't recompute indicators for every user separately — it's the same data
- Price data needs to be fetched once and shared across all users
- Polygon API has rate limits (5 calls/min on free, 100/min on paid)

### Architecture for multi-user

```
                    ┌─────────────────┐
                    │  Polygon API    │
                    │  (price data)   │
                    └────────┬────────┘
                             │ nightly job
                    ┌────────▼────────┐
                    │  Data Worker    │
                    │  (single proc)  │
                    │  Fetches +      │
                    │  computes       │
                    │  indicators     │
                    └────────┬────────┘
                             │ writes to
                    ┌────────▼────────┐
                    │  PostgreSQL     │
                    │  (shared DB)    │
                    │  - indicators   │
                    │  - user data    │
                    │  - cache        │
                    └────────┬────────┘
                             │ reads from
                    ┌────────▼────────┐
                    │  Web Server     │
                    │  (FastAPI)      │
                    │  handles user   │
                    │  requests       │
                    └─────────────────┘
```

### Third-party data providers (faster alternatives)
Instead of computing everything yourself:

| Provider | What it does | Cost |
|----------|-------------|------|
| **Polygon.io** (current) | Raw OHLCV data. You compute indicators yourself. | $29/mo (Starter), $199/mo (Developer) |
| **Alpha Vantage** | Raw data + some technical indicators pre-computed | Free tier limited, $50/mo for premium |
| **Tiingo** | Good quality daily data, affordable | $10/mo for 50K calls |
| **Twelve Data** | Pre-computed technical indicators (SMA, EMA, BB, etc.) | $29/mo, includes indicator APIs |
| **TradingView Pine** | Could export indicator data | Not API-friendly |
| **Yahoo Finance (yfinance)** | Free, but unreliable and rate-limited | Free (unofficial) |

### Recommendation
Keep **Polygon** for raw data (you're already integrated). Run a **single background worker** that:
1. Fetches new daily data after market close (~4:30 PM ET)
2. Recomputes indicators incrementally (only new bars, not full history)
3. Writes to shared PostgreSQL

This worker runs once, serves all users. Users never trigger data fetches — they just query the pre-computed data.

### Incremental computation (key optimisation)
Currently you recompute ALL history. For daily updates, you only need:
- Fetch today's new bar for each ticker
- Append to existing data
- Recompute indicators for the last N bars (where N = lookback period, ~110 bars)
- This takes seconds instead of 36 minutes

### Hourly data — must be server-side
Current hourly data: 3,020 CSVs, 108MB (1-2 years). Scaling to 20 years:
- **Hourly CSVs**: ~5GB (3,000 tickers × 6.5 bars/day × 5,000 days)
- **backtest_indicators with 1H/4H/8H**: could grow to 50-100M rows, DB goes from 2GB to ~15-20GB
- This is NOT something to upload from a laptop. The server should fetch from Polygon directly.
- **Plan**: Deploy with current data (Option A upload), then switch to server-side fetching when hourly backfills are needed. Don't try to do both at once.
- A Hetzner VPS with 80GB SSD + 4GB RAM ($8/mo) handles this comfortably.

---

## 4. Migration Path (Step by Step)

### Phase 1: Get it running on a server (1-2 weeks)
- [ ] Set up a VPS (Hetzner/DigitalOcean)
- [ ] Install Python, nginx, PostgreSQL
- [ ] Migrate SQLite → PostgreSQL (schema is simple, mostly find/replace)
- [ ] Wrap app.py in Flask or FastAPI
- [ ] Set up gunicorn + nginx
- [ ] Point domain to server
- [ ] Set up HTTPS via Let's Encrypt
- [ ] Deploy code via git pull

### Phase 2: Add user accounts (1-2 weeks)
- [ ] Integrate Supabase Auth (or similar)
- [ ] Add login/register pages
- [ ] Per-user saved configs stored in DB
- [ ] Session management (JWT in cookies)
- [ ] Basic rate limiting

### Phase 3: Shared data pipeline (1 week)
- [ ] Background worker for daily data fetch
- [ ] Incremental indicator computation
- [ ] Cron job (run after market close)
- [ ] Indicator data shared across all users

### Phase 4: Polish & launch (1-2 weeks)
- [ ] Error handling, loading states
- [ ] Mobile responsiveness (currently desktop-only)
- [ ] Landing page / marketing site
- [ ] Stripe integration if monetising
- [ ] Monitoring / alerting (Sentry, UptimeRobot)

---

## 5. Cost Estimate (Monthly)

| Item | Cost |
|------|------|
| VPS (Hetzner CX31, 4GB RAM) | $8 |
| Domain name | $1 |
| Polygon API (Starter) | $29 |
| Supabase (free tier) | $0 |
| Email (Resend, for password reset) | $0 (free tier) |
| **Total** | **~$38/mo** |

At scale (100+ users), PostgreSQL might need more RAM → upgrade VPS to $20-40/mo. Still very affordable.

---

## 6. Key Decisions to Make

1. **Framework**: Flask (familiar, simple) vs FastAPI (modern, async, faster)?
2. **Auth**: Build yourself vs Supabase Auth vs Clerk?
3. **Monetisation**: Free-only? Freemium? Subscription?
4. **Data scope**: Russell 3000 only, or expand to global markets?
5. **Real-time**: Do you need intraday data, or is daily close enough?
6. **Mobile**: Do you want a mobile app, or just responsive web?

---

## 7. Things That Are Easier Than You'd Think

- **Deploying to a VPS**: It's literally SSH in, git clone, pip install, run. nginx config is ~20 lines.
- **PostgreSQL migration**: Your SQLite schema maps 1:1. The query syntax is 99% identical.
- **HTTPS**: Certbot (Let's Encrypt) handles it automatically.
- **User auth with Supabase**: ~50 lines of code for login/register/session.

## 8. Things That Are Harder Than You'd Think

- **Security**: SQL injection, XSS, CSRF, password storage. Using an auth provider handles most of this.
- **Concurrent writes**: SQLite locks on write. PostgreSQL handles this natively.
- **Data consistency**: Making sure all users see the same indicator data, even during recomputation.
- **Email deliverability**: Sending password reset emails that don't end up in spam. Use a service (Resend, SendGrid).
- **Monitoring**: Knowing when your server is down or the data pipeline failed. Set up alerts early.
