# CLAUDE.md — hyperliquid-nike-rocket-api

> Keep this file lean (it loads into every session). Detailed change history lives in
> `docs/progress/` — read those on demand, don't paste them here.

## What this is
The **follower / customer service** for $NIKEPIG's Massive Rocket. The master
(`massive-rocket-algos`, a separate repo) trades its own account and broadcasts
signals; this service mirrors those signals onto **paying customers' own Hyperliquid
accounts**. FastAPI, deployed on **Railway** (`railway.json`). **Postgres-backed**
(SQLAlchemy). **Live money, customer-facing** — verify before deploy.

- Customers sign up, then enter their **Hyperliquid API-wallet** credentials at `/setup`.
  Creds are stored **encrypted** (Fernet, `CREDENTIALS_ENCRYPTION_KEY`) in
  `follower_users` — see `User.set_hl_credentials` / `get_hl_credentials`
  (`follower_models.py`). API wallets **cannot withdraw**, only trade.
- Billing is 30-day rolling (`billing_service_30day.py`, `config.py` fee tiers).

## Layout (key files)
- `main.py` — the FastAPI app: routes, admin endpoints, and the customer **dashboard
  HTML** (`/dashboard`, ~L1804) plus `/setup` (`/setup` served from `setup.html`).
  **Huge file.** The dashboard is one big Python **f-string**.
- `follower_endpoints.py` — customer API: `POST /api/setup-agent` (validates + encrypts
  + stores creds, sets `api_wallet_expires_at = now + 180d`, ~L1048), `GET
  /api/agent-status` (~L1079), `/api/start-agent`, `/api/stop-agent`.
- `follower_models.py` — SQLAlchemy models. `User` (`follower_users`) holds
  `hl_private_key_encrypted`, `hl_wallet_address`, `api_wallet_expires_at` (L89), billing.
- `api_expiry_service.py` — **authoritative** API-wallet expiry service: 6-hourly cron
  that emails reminders (30/14/7/3/1 days, Resend) and **deactivates** the agent on
  expiry (`days_left < 0` → `agent_active=false`). Also the admin `/admin/api-expiry/*`.
- `hosted_trading_loop.py` — executes signals per customer. `position_monitor.py`,
  `order_utils.py`, `price_cache.py`, `trade_reconciliation.py` support it.
- `config.py` — fee tiers + `utc_now()` / `ensure_utc_aware()` tz helpers.
- HTML: `setup.html`, `signup.html`, `login.html`; the dashboard is inline in `main.py`.

## ⚠️ Gotchas — verified, high-impact
1. **The `/dashboard` HTML is a Python f-string** (`html = f"""…"""`). Every literal
   CSS/JS brace must be **doubled** (`{{` / `}}`); `{api_key}` is the only interpolation.
   An undoubled brace either fails `py_compile` (if it forms an invalid expression) or —
   worse — silently interpolates a Python name at render and 500s `/dashboard` for
   **every** customer. Always render-test after editing it (see workflow step 4).
2. **Two status renderers both write `#agent-status-display`.** `displayAgentStatus`
   (lean, banner-only) runs on a **30s interval**; `checkAgentStatus` (full — also badge +
   Start/Stop buttons + details) runs on load / refresh / after start-stop. Keep their
   per-state banner copy **byte-identical** or the banner flips every 30s, and remember
   the interval renderer does **not** manage badge/buttons except where explicitly mirrored.
3. **`api_wallet_expires_at` may be NAIVE UTC** (setup writes `datetime.utcnow()`).
   Always stamp `tzinfo=utc` before comparing. **Legacy NULL = unknown, never "expired."**
   The UI `expired` flag is `days_remaining < 0`, deliberately identical to the boundary
   `api_expiry_service.py` deactivates on, so UI and backend never disagree.
4. **Customer creds are encrypted at rest** and API wallets expire (max 180d). Renewal is
   customer self-serve via `/setup?key=<nk_ key>` — the service never sees raw keys long-term.

## Multi-agent development workflow (standing process for substantial changes)
Non-trivial changes here follow a wave-based, multi-agent flow (orchestrated with the
Workflow tool; subagents **read/design/review only** — the main session makes all edits
and git ops so parallel agents never collide on live-money code):

1. **Recon + branch.** Read the real code; create/checkout the feature branch from latest
   `origin/main`.
2. **Wave 1 — Plan → Consensus spec.** Parallel planners diverge (safety / UX /
   correctness lenses) → parallel builders each draft a concrete spec → one consensus
   agent merges into a single authoritative spec (exact files, anchors, code) with
   conflicts resolved.
3. **Implement** the consensus spec in the main session; respect gotcha #1.
4. **Verify.** `python -m py_compile`; render the dashboard f-string in isolation
   (extract via `ast`, `eval` with only `api_key` in scope — proves no stray
   interpolation without needing the full app installed); unit-check server logic on
   edge inputs (NULL / naive / aware / past / today / malformed).
5. **Wave 2 — Adversarial review.** Parallel reviewers (correctness / live-money-safety /
   UX-consistency) file findings; each finding is **independently verified by a skeptic
   that tries to refute it**; only CONFIRMED findings survive.
6. **QC.** Apply confirmed findings + worthwhile nits in the main session; re-verify.
7. **Docs.** Update this file (keep lean) + a dated `docs/progress/` entry (full detail).
8. **Rebase** the feature branch onto latest `origin/main`; push the branch. No PR unless asked.

## Conventions
- Develop on the session's feature branch; commit + push there; **no PRs unless asked**.
- `python -m py_compile` after edits. **Live money** — verify before deploy.
- Timezone-aware datetimes everywhere expiry/billing is compared (naive stored = UTC).

## History
See `docs/progress/` (e.g. `2026-08-10-api-wallet-expiry-surfacing.md`).
