# Historical Figure Genetic Genealogy & Ancestry Mapper

> Probabilistic genetic ancestry inference between modern individuals and historical figures — built on PostgreSQL, Redis, and FastAPI.

**Live Demo:** [genealogy-mapper frontend](https://CrimsonDarkrai95.github.io/genealogy-mapper/) &nbsp;|&nbsp; **API:** [Railway](https://genealogy-mapper-production.up.railway.app/docs)

---

## Overview

This system infers probabilistic genetic and genealogical relationships between modern genomic samples and historical figures. It integrates ancient DNA datasets, haplogroup reference trees, biographical records, and dynasty data into a unified PostgreSQL schema, then exposes the entire database through a FastAPI backend with Redis caching and a natural language query interface powered by Gemini Flash.

The project was built as a DBMS academic submission demonstrating advanced database design, ETL pipeline construction, caching strategy, and NL-to-SQL query generation.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Static Frontend (GitHub Pages)                             │
│  Single HTML — Structured Query + NL Query + Results Table  │
└────────────────────────┬────────────────────────────────────┘
                         │ HTTPS fetch
┌────────────────────────▼────────────────────────────────────┐
│  FastAPI Backend (Railway)                                   │
│  13 structured routes + POST /query/nl                       │
│  asyncpg connection pool · CORS middleware                   │
└────────────┬───────────────────────────┬────────────────────┘
             │ asyncpg (SSL)             │ redis-py (async)
┌────────────▼──────────┐   ┌───────────▼────────────────────┐
│  PostgreSQL 15         │   │  Redis Cloud (free tier)        │
│  Supabase              │   │  Query cache · NL-SQL cache      │
│  17 tables · ~32K rows │   │  Ancestry cache · Rate limiting  │
└───────────────────────┘   └────────────────────────────────┘
```

### NL-to-SQL Pipeline

```
POST /query/nl  {"query": "Which people share a haplogroup with Genghis Khan?"}
       ↓
sha256(query) → Redis GET nl2sql:{hash}
       ↓
  [Cache HIT] ──────────────────────────────────────────────────→ Execute → Return
       ↓
  [Cache MISS]
       ↓
Gemini Flash API (schema-injected prompt, few-shot examples)
       ↓
SQL Validator (SELECT only · no destructive ops · table whitelist · max 4000 chars)
       ↓
Redis SET nl2sql:{hash} TTL 3600s
       ↓
asyncpg execute → JSON response {generated_sql, cached, results, row_count}
```

---

## Tech Stack

| Layer | Technology | Role |
|---|---|---|
| Database | PostgreSQL 15 (Supabase) | Primary DBMS — recursive CTEs, JSONB, tsvector FTS, UUID PKs |
| Caching | Redis Cloud (redis-py async) | Query cache, NL-SQL cache, ancestry path cache, rate limiting |
| API Framework | FastAPI (Python) | Async REST API, Pydantic response models, OpenAPI docs |
| DB Driver | asyncpg | Binary protocol, COPY bulk insert, SSL session pooler |
| NL Translation | Gemini Flash API | Schema-injected prompt → validated SELECT SQL |
| Backend Host | Railway | Auto-deploy from GitHub, env var management |
| Frontend Host | GitHub Pages | Static HTML, no build step |
| ETL | Python (pandas + asyncpg) | CSV ingestion, upsert logic, FK dependency ordering |

---

## Database Schema — 17 Tables

### Group 1 — Individual Records
| Table | Description |
|---|---|
| `person` | All individuals — modern and historical. UUID PK, `is_historical` flag, `bio_tsv` GIN index for FTS, `birth_year` negative = BCE |
| `person_alias` | Alternate names, transliterations, nicknames per person |
| `person_external_id` | Cross-references to Wikidata, NCBI, AADR external systems |

### Group 2 — Genomic Data
| Table | Description |
|---|---|
| `genome` | One genomic profile per individual per dataset. Y/mt haplogroups, coverage depth, damage pattern, `raw_metadata JSONB` |
| `snp_marker` | SNP definitions from NCBI dbSNP — rsID, chromosome, position, alleles, population frequencies |
| `genome_snp` | Junction table — genome ↔ SNP markers with observed genotype and quality score |
| `haplogroup_reference` | Canonical haplogroup tree (YFull/ISOGG). Self-referencing FK enables recursive CTE traversal |

### Group 3 — Genome Similarity
| Table | Description |
|---|---|
| `genome_match` | Pairwise genome comparison results — similarity score, shared cM, IBD segment count, matching method |
| `population_cluster` | Population ancestry cluster definitions — EUR, EAS, STEPPE_BRONZE_AGE, etc. |
| `genome_cluster_assignment` | Genome ↔ cluster membership with ADMIXTURE/PCA probability |

### Group 4 — Relationship Inference
| Table | Description |
|---|---|
| `inferred_relationship` | Probabilistic genealogical relationships derived from genome matches. `supporting_evidence JSONB` stores scoring breakdown |

### Group 5 — Lineage & Dynasty
| Table | Description |
|---|---|
| `dynasty` | Historical dynasties and empires with FTS on description |
| `dynasty_membership` | Person ↔ dynasty with role, date range, founding member flag |

### Group 6 — Geographic & Temporal
| Table | Description |
|---|---|
| `region` | Geographic regions — self-referencing FK for hierarchy. GeoJSON outline stored as JSONB |
| `person_location_timeline` | Time-anchored geographic associations — birth, death, rule, migration |
| `historical_conflict` | Historical conflicts from H-DATA and OpenDataBay |

### Group 7 — Provenance
| Table | Description |
|---|---|
| `dataset_source` | Metadata for every ingested dataset — reliability score, DOI, record count estimate |

---

## API Routes

| Method | Route | Description |
|---|---|---|
| GET | `/health` | Database + Redis connectivity status |
| GET | `/search?q=` | Full-text search across persons, datasets, dynasties |
| GET | `/person/{id}` | Person profile with aliases and external IDs |
| GET | `/person/{id}/genome` | All genome records for a person |
| GET | `/person/{id}/matches` | Genome similarity matches, filterable by score and method |
| GET | `/person/{id}/ancestry` | Recursive ancestry chain traversal up to N generations |
| GET | `/person/{id}/lineage` | Dynasty memberships and roles |
| GET | `/match/{id}` | Full detail for a genome match pair |
| GET | `/relationship/{id}` | Inferred relationship with supporting evidence |
| GET | `/haplogroup/{code}/members` | All persons sharing a haplogroup (recursive subtree) |
| GET | `/cluster/{id}` | Population cluster members by membership probability |
| GET | `/historical` | Paginated historical figures with year and dynasty filters |
| GET | `/timeline` | Geographic event timeline for a person |
| POST | `/query/nl` | Natural language → validated SQL → results |

---

## Redis Caching Strategy

| Cache | Key Pattern | TTL | Structure |
|---|---|---|---|
| Query result cache | `qcache:{route_hash}` | 15 min | String (JSON blob) |
| Ancestry path cache | `ancestry:{person_id}` | 30 min | String (JSON blob) |
| NL-to-SQL translation | `nl2sql:{sha256_of_query}` | 60 min | String (SQL) |
| Haplogroup members | `haplo:{code}` | 60 min | String (JSON) |
| Cluster members | `cluster:{cluster_id}` | 60 min | String (JSON) |
| Rate limit counter | `ratelimit:{api_key}:{window}` | 60 sec | String (INCR) |

Cache invalidation on new genome ingestion targets `ancestry:{person_id}` and matching `qcache:*` keys via SCAN. NL-to-SQL cache is never flushed on data updates — SQL structure is schema-bound, not data-bound.

---

## Datasets

| Dataset | Target Tables | Est. Volume |
|---|---|---|
| AADR Ancient DNA Resource | `genome`, `person`, `person_location_timeline` | ~10,000 ancient samples |
| 1000 Genomes Project | `genome`, `genome_snp`, `population_cluster` | ~2,504 genomes |
| NCBI dbSNP | `snp_marker` | filtered subset |
| YFull YTree / ISOGG | `haplogroup_reference` | ~5,000 haplogroup nodes |
| Cross-verified Notable People DB | `person`, `region`, `person_location_timeline` | ~3,500 persons |
| AgeDataset-V1 (Wikidata QIDs) | `person` | 1.22M deceased notable persons (filtered) |
| Kaggle Wikipedia People | `person` | ~800K rows (filtered to notable) |
| H-DATA / OpenDataBay Conflicts | `historical_conflict`, `region` | ~5,800 conflict records |
| Genographic Project | `population_cluster`, `region` | ~50 cluster definitions |

---

## Project Structure

```
genealogy-mapper/
├── backend/
│   ├── main.py                  # FastAPI app, lifespan, CORS, health route
│   ├── db/
│   │   └── pool.py              # asyncpg connection pool (Supabase Session Pooler)
│   ├── cache/
│   │   └── client.py            # redis-py async client, init/close
│   ├── routers/
│   │   ├── person.py            # /person routes
│   │   ├── match.py             # /match routes
│   │   ├── relationship.py      # /relationship routes
│   │   ├── haplogroup.py        # /haplogroup routes
│   │   ├── cluster.py           # /cluster routes
│   │   ├── historical.py        # /historical route
│   │   ├── search.py            # /search route
│   │   ├── timeline.py          # /timeline route
│   │   └── nl_query.py          # POST /query/nl
│   ├── nl/
│   │   └── validator.py         # SQL validation — SELECT-only, table whitelist, injection blocking
│   ├── cache/
│   │   └── keys.py              # Redis key builder functions
│   └── etl/                     # ETL scripts per table in FK dependency order
│       ├── dataset_source_etl.py
│       ├── region_etl.py
│       ├── person_etl.py
│       ├── haplogroup_reference_etl.py
│       └── ...
├── docs/                        # Architecture document
├── tests/
│   ├── conftest.py              # AsyncMock fixtures, LifespanManager for integration tests
│   ├── unit/                    # 47 unit tests (fully mocked)
│   └── integration/             # 5 live tests against Supabase + Redis Cloud
├── index.html                   # Frontend (GitHub Pages)
├── requirements.txt
├── pytest.ini
└── .env                         # DATABASE_URL, REDIS_URL, GEMINI_API_KEY (not committed)
```

---

## Running Locally

**Prerequisites:** Python 3.11+, a `.env` file at the project root.

**.env format:**
```
DATABASE_URL=postgresql://postgres.[project-ref]:[password]@aws-0-[region].pooler.supabase.com:5432/postgres?statement_cache_size=0
REDIS_URL=redis://default:[password]@[host]:[port]
GEMINI_API_KEY=your_gemini_api_key
```

**Install and run:**
```bash
pip install -r requirements.txt
uvicorn backend.main:app --reload --port 8000
```

**Verify:**
```
GET http://localhost:8000/health     → {"status":"ok","database":"ok","redis":"ok"}
GET http://localhost:8000/docs       → OpenAPI explorer
```

**Run tests:**
```bash
pytest tests/unit/ -v               # 47 unit tests, no live connections needed
pytest tests/integration/ -v        # 5 integration tests, requires live DB + Redis
```

---

## Deployment

### Backend — Railway

1. Connect GitHub repo to Railway
2. Set **Root Directory** to blank (code is at repo root)
3. Set **Start Command:** `uvicorn backend.main:app --host 0.0.0.0 --port $PORT`
4. Add environment variables: `DATABASE_URL`, `REDIS_URL`, `GEMINI_API_KEY`
5. Set **Healthcheck Path:** `/health`

Railway auto-deploys on every push to `main`.

### Frontend — GitHub Pages

1. Place `index.html` at repo root
2. Go to **Settings → Pages → Deploy from branch → main → / (root)**
3. Live at `https://{username}.github.io/{repo}/`

The frontend has the Railway API URL hardcoded. No build step required.

### Alternative Backend Hosts

This FastAPI app deploys to any ASGI-compatible host with no code changes — only environment variables differ.

| Platform | Build Command | Start Command |
|---|---|---|
| Railway | *(auto-detected)* | `uvicorn backend.main:app --host 0.0.0.0 --port $PORT` |
| Render | `pip install -r requirements.txt` | `uvicorn backend.main:app --host 0.0.0.0 --port $PORT` |
| Fly.io | `pip install -r requirements.txt` | `uvicorn backend.main:app --host 0.0.0.0 --port 8080` |

---

## Test Suite — 71 Tests, 0 Failures

| Layer | Count | Description |
|---|---|---|
| Unit tests | 47 | All 13 routes tested in isolation with `AsyncMock` — no live DB or Redis |
| Integration tests | 5 | Live queries against Supabase and Redis Cloud via `asgi_lifespan.LifespanManager` |
| SQL validator tests | 19 | Direct tests of `validate_sql()` — SELECT whitelist, injection patterns, length limit, table whitelist |

---

## Key Design Decisions

**UUID primary keys throughout** — allows ETL scripts to generate IDs without coordination across concurrent ingestion jobs. SERIAL used only for small lookup tables (`region`, `dataset_source`).

**`birth_year` as INTEGER, negative = BCE** — PostgreSQL `DATE` cannot represent pre-4713 BC dates in some drivers. Integer arithmetic is simpler for generational distance calculations and era-based window functions.

**Recursive CTE for ancestry traversal** — the `inferred_relationship` table is probabilistic and sparse, not a clean tree. Recursive CTEs handle multi-path graphs with depth limits cleanly. Results cached in Redis with 30-minute TTL to offset CTE overhead on deep chains.

**Genome similarity computed externally** — IBD and ADMIXTURE analysis requires PLINK/EIGENSOFT. PostgreSQL stores results as facts in `genome_match`, decoupling compute from the query layer.

**NL-to-SQL validated with regex + sqlparse AST** — blocks all destructive operations (DROP, DELETE, ALTER, TRUNCATE, INSERT, UPDATE, GRANT), stacked queries, comment injections, and queries referencing tables outside the schema whitelist. Supabase RLS policies serve as a second layer.

**Redis free tier (30MB) — aggressive TTL scoping** — short TTLs (15–30 min) on result caches mean stale data expires naturally. NL-to-SQL cache (small strings) and ancestry path cache (JSON capped at 50 results) are sized to coexist within the 30MB limit. LRU eviction configured as the eviction policy.

---

## Normalization

The schema was designed to 3NF/BCNF from the outset. Representative tables:

- **`person`** — every non-key attribute depends solely on `person_id`. Aliases extracted to `person_alias`, external IDs to `person_external_id`, geographic events to `person_location_timeline`.
- **`genome_snp`** — junction table resolving the many-to-many between `genome` and `snp_marker`. `observed_genotype` and `quality_score` are facts about the specific genome-SNP pair, not about either entity alone.
- **`inferred_relationship`** — `confidence_level` and `generational_distance` are facts about the (person_a, person_b, match) triple, not about any individual person.
- **`genome_match`** — `similarity_score`, `total_shared_cm`, `longest_segment_cm` are facts about the genome pair. UNIQUE constraint on `(genome_a_id, genome_b_id)`.

---

## Academic Context

This project was submitted as a DBMS course project (2025–26) following the prescribed template:

**Storyline → Components of Database Design → ER Diagram → Relational Model → Normalization → SQL Queries → Project Demonstration → Self-Learning → Challenges → Conclusion**

The backend stack (FastAPI + Redis + PostgreSQL + NL-to-SQL) serves as the demonstration layer. The frontend makes the database transparent to evaluators without requiring direct Supabase or terminal access.

---

## License

Academic project. Not licensed for commercial use.
