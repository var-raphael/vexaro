# Vexaro
## The GitHub For Web Data and Ai Datasets

> Turn any website into a clean API and Ai Datasets. Ready in hours.

Vexaro is a public web data infrastructure platform that turns publicly accessible websites into structured APIs and datasets. Think of it as GitHub for web data.

**Status: Actively under development. Not yet launched.**

---

## What is Vexaro

Vexaro gives developers three ways to access structured web data:

**Ready-made APIs** — Curated public APIs across movies, news, music, crypto, and sports. Ready to consume immediately with no setup required.

**Custom API Builder** — Describe what you want, define your JSON schema, and Vexaro builds and maintains a live API endpoint from real web sources.

**URL Import** — Bring your own URLs, define your own schema, and get clean structured data back. Full control for technical users.

---

## How It Works

Vexaro's engine crawls publicly accessible websites through a multi-layer pipeline:

- **Layer 1** — TLS HTTP fetch with Chrome-level fingerprinting
- **Layer 2** — Embedded JSON extraction, JSON-LD, and API endpoint scanning
- **Layer 3** — Full browser rendering via Browserless for JavaScript-heavy sites

Extracted data is cleaned, structured against a user-defined schema, versioned, and served through a clean REST API endpoint.

---

## Key Features

- Clean structured JSON from any public website
- Immutable versioning — every data change is tracked, never overwritten
- Diff viewer — see exactly what changed between versions
- Rollback — revert to any previous version instantly
- Dataset cloning — fork any public dataset and extend it
- Nightly refresh — data stays live automatically
- API tokens — secure access via `vx_` prefixed tokens

---

## Tech Stack

- **Engine** — Go
- **Frontend** — Next.js + TypeScript + Tailwind
- **Database** — SQLite (dev) → MySQL PlanetScale (production)
- **Storage** — Local (dev) → Backblaze B2 (production)
- **Auth** — Supabase (Google OAuth)
- **Payments** — Paystack

---

## Project Status

Vexaro is currently under active development.

- [x] Multi-layer crawling engine
- [x] AI schema extraction
- [x] Versioning pipeline
- [x] Multi-URL merge and deduplication
- [x] Plan limits enforcement
- [x] API token system
- [ ] Frontend (in progress)
- [ ] Auth and payments
- [ ] Public launch

---

## Author

Built by [Raphael Samuel](https://var-raphael.vercel.app) — AI Tooling and Data Infrastructure Engineer.

---

*Public launch coming soon.*
