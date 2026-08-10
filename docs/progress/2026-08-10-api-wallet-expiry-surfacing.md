# 2026-08-10 — Surface API-wallet expiry + self-serve renew CTA in the customer dashboard

## Problem
Customer Hyperliquid **API wallets expire** (max 180 days). On expiry, the 6-hourly
`api_expiry_service.py` cron sets `agent_active=false` and emails the customer — but the
**dashboard gave no in-app signal**:
- `GET /api/agent-status` never returned the expiry, so the dashboard couldn't show it.
- An expired wallet rendered the benign yellow **"Ready – Agent configured but stopped"**
  state — **indistinguishable from a manual pause** — with **no renew link**. On expiry
  the cron sets `agent_active=false` but leaves `credentials_set=true`, so the only
  branch that showed a `/setup` link (the `Not Configured` state) was never reached.
- Net effect: a customer whose wallet expired saw a normal-looking "Ready" dashboard and
  had no cue to renew except the reminder email.

## Change

### Server — `follower_endpoints.py`, `GET /api/agent-status`
Added three fields to **both** return branches (configured + not-configured) so the client
JSON shape is branch-independent:
- `api_wallet_expires_at` — ISO string or `null`
- `days_remaining` — int (floor) or `null`
- `expired` — bool

Compute (configured branch only; not-configured hardcodes `null/null/false`):
- **Timezone-aware:** stored value may be **naive UTC** (`setup-agent` writes
  `datetime.utcnow()+180d`); stamp `tzinfo=utc` before subtracting.
- `days_remaining = (expiry - now).days` (floors, matching `api_expiry_service.py:279`).
- `expired = days_remaining < 0` — **deliberately the same predicate** the expiry cron
  deactivates on (`api_expiry_service.py:281`), so the UI verdict can never contradict the
  backend. `days_remaining == 0` = "expires today", **not** expired.
- **Legacy NULL** (`api_wallet_expires_at` column added later; old users have NULL) →
  `(null, null, false)` = *unknown*, never expired. Primary backward-compat contract.
- **`try/except`-wrapped:** any malformed value degrades to `(null,null,false)` + a
  `logger.warning`, so a display field can **never 500** this live customer endpoint.
  Authoritative enforcement stays in `api_expiry_service.py`.
- Zero new imports (`datetime`, `timezone` already imported at L28; `logger` at L51).
- `setup-agent` unchanged — it already sets `now+180d`; the renew CTA just routes to
  `/setup?key=`, which re-runs that path.

### Client — `main.py` `/dashboard` (Python f-string; all new braces doubled)
- New shared JS helper **`apiWalletBanner(dr, expired)`** (inserted before
  `checkAgentStatusAPI`): returns a countdown chip (`· API wallet: N days left`), and at
  `dr <= 7` **or** `expired`, appends a red **"Renew API →"** link to `/setup?key=` +
  `currentApiKey`. Guards `typeof dr !== 'number'` **before** any `<=` compare so legacy
  `null` never trips the CTA (JS coerces `null <= 7` to true). Handles the
  expired-while-active overlap by surfacing the warning inside the green banner.
- **Both** status renderers gained a new **4-state** chain: green `agent_active`
  (+countdown suffix) → **NEW** red **"API wallet expired – Renew now"** (only when
  `agent_configured && !agent_active && expired`) → yellow "Ready" paused (+countdown) →
  red "Not Configured". This is the **mislabel fix**: an expired wallet is now visually
  distinct from a user-paused agent.
- Renew links carry `currentApiKey` (the `nk_` key), which `/setup` prefills from `?key=`.

## Consensus decisions (Wave 1)
- **Branch precedence = `agent_active` first, then `expired`.** The canonical expired case
  is `agent_active=false` (all orderings render red identically). Letting `agent_active`
  win the rare transient overlap (wallet expired but the ≤6h cron hasn't flipped
  `agent_active` yet) avoids hiding Stop from a still-active agent and avoids flapping its
  banner to full red — while the helper still shows an "· API wallet expired · Renew API →"
  chip in the green banner. Consequence: the expired branch is only reached with
  `agent_active=false`, so it hides **both** Start (re-arming a dead wallet is futile) and
  Stop (nothing to stop).
- **One shared helper** over 3–4 split helpers — single source of truth, no copy drift.
- **Renew threshold `dr <= 7`** (matches the config reminder ladder's 7-day tier). Trivially
  bumped to 14 at the single `dr <= 7` if product wants.

## Review + QC (Wave 2)
Adversarial review (correctness / live-money-safety / UX-consistency lenses), each finding
independently verified by a refutation skeptic. 2 findings: 1 rejected, 1 confirmed+fixed.
- **REJECTED (claimed high — reflected XSS via `currentApiKey`):** real vuln but **entirely
  pre-existing** — the taint is the on-load reflection `let currentApiKey = '{api_key}'`
  (main.py:3629) and `value="{api_key}"`, plus pre-existing `innerHTML` sinks in the
  unchanged `Not Configured` branch. The new `/setup?key=` links add **no new exploitable
  surface** and `encodeURIComponent` on them wouldn't neutralize the real on-load vector.
  **Filed as a separate pre-existing-XSS concern, out of scope for this diff.**
- **CONFIRMED (low — UX):** the 30s interval runs the lean **banner-only**
  `displayAgentStatus`, which doesn't manage the badge/buttons. If a wallet expired while
  the dashboard sat idle, the next tick flipped the banner to red "expired" but left a
  stale yellow "Ready" badge and a **clickable Start button** (`checkAgentStatus` hides
  Start when expired, but it's not the timer renderer). **Fix:** mirror the "Expired" badge
  + Start/Stop hide into `displayAgentStatus`'s expired branch (guarded). Safe because the
  expired state is terminal — it exits only via the `/setup` reload, which re-runs the full
  `checkAgentStatus`.

## Verification
- `python -m py_compile main.py follower_endpoints.py` — clean (the f-string brace gate).
- **Isolated dashboard render:** extract the `/dashboard` f-string via `ast`,
  `eval` with only `api_key` in scope (no app deps needed). Renders 158 KB with no stray
  interpolation; asserts helper defined once, renderer-A calls it twice (green+yellow),
  renderer-B computes `walletSuffix` once, both expired branches present, distinct
  `🔴 Expired` vs `🟡 Ready` badges, renew hrefs carry the key.
- **Server compute unit checks** (no DB): NULL→`(null,null,false)`; naive +10d→`days∈{9,10}`,
  not expired; naive −2d→expired, `days<0`; aware +10d→no double-shift; +6h→`days==0`, not
  expired; malformed str→`(null,null,false)` no raise.

## Visible dashboard states after this change
| Server state | Banner | Badge | Buttons |
|---|---|---|---|
| active, >7d | 🟢 Active + "N days left" | 🟢 Running | Stop |
| active, ≤7d or expired-overlap | 🟢 Active + "… · Renew API →" | 🟢 Running | Stop |
| configured, not active, **expired** | 🔴 **API wallet expired – Renew now** | 🔴 Expired | none |
| configured, not active, valid (paused) | 🟡 Ready + countdown | 🟡 Ready | Start |
| not configured | 🔴 Not Configured – Complete setup | 🔴 Not Configured | none |
| legacy NULL expiry | (green/yellow, no chip) | as above | as above |

## Files
- `follower_endpoints.py` — `get_agent_status` (both return branches).
- `main.py` — `/dashboard` f-string: `apiWalletBanner` helper + both status renderers.
- `.gitignore` — added (repo had none) for `__pycache__/*.pyc`.

## Follow-ups / not done here
- **Pre-existing reflected XSS** via the on-load `?key=` reflection (main.py:3629, 2693 and
  the pre-existing `innerHTML` status sinks) — needs its own ticket; not touched by this diff.
- Renew threshold is a flat 7 days; could match the full 30/14/7/3/1 email ladder if desired.
