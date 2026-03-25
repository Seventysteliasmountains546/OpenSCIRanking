<p align="center">
  <a href="README.md">
    <img alt="English" src="https://img.shields.io/badge/Language-English-1f6feb">
  </a>
  <a href="README.zh-CN.md">
    <img alt="中文" src="https://img.shields.io/badge/%E8%AF%AD%E8%A8%80-%E4%B8%AD%E6%96%87-c2410c">
  </a>
</p>

<p align="center">
  <img src="assets/banner.png" alt="OpenSCIRanking banner" width="100%" />
</p>

<h1 align="center">OpenSCIRanking</h1>

<p align="center">
  A reproducible, bottom-up journal ranking pipeline built on top of OpenAlex.
</p>

<p align="center">
  <img alt="Python" src="https://img.shields.io/badge/Python-3.11%2B-1f6feb">
  <img alt="Data Source" src="https://img.shields.io/badge/Data-OpenAlex-0f766e">
  <img alt="Ranking" src="https://img.shields.io/badge/Method-Citation%20Graph%20%2B%20PageRank-c2410c">
  <img alt="Workflow" src="https://img.shields.io/badge/Workflow-Reproducible-334155">
</p>

## Overview

**OpenSCIRanking** is an open repository for ranking journals with a transparent, data-driven workflow rather than top-down expert partitioning.

The current repository keeps the full source code and uses **computer science journals** as the worked example, from journal-list preparation all the way to final ranking export.

## Why This Repo

- Bottom-up rather than top-down: rankings come from citation-network structure and explicit adjustment rules.
- Reproducible workflow: users can clone the repo and rerun the pipeline on their own journal lists.
- Transparent scoring: the method is inspectable, tunable, and easy to audit.
- GitHub-friendly layout: code is versioned, while local data products are excluded.

## Method Snapshot

The default worked-example recipe in this repository is:

- build an inter-journal citation graph from OpenAlex works
- drop journal self-citations
- compute base `PageRank`
- apply `sqrt_norm` to reduce volume bias
- apply a mega-journal penalty to suppress extremely large venues
- apply a mild review-heavy penalty based on average references per paper

## Repository Structure

- `inputs/`: example journal lists and seed files
- `src/opensci_v2/`: reusable package code
- `scripts/`: command-line entrypoints for each pipeline stage
- `assets/`: README visual assets
- `data/`: local fetched data and outputs, ignored by git
- `cas_data/`: local raw spreadsheets, ignored by git

## Quick Start

Requirements:

- Python `>= 3.11`
- network access to the OpenAlex API
- enough local disk for Parquet outputs

```bash
python -m pip install -r requirements.txt
```

or:

```bash
make install
```

## Worked Example: Computer Science

The repository already includes:

- [inputs/computer_science_journals.csv](inputs/computer_science_journals.csv)

### 1. Resolve journals to OpenAlex sources

```bash
PYTHONPATH=src python scripts/resolve_openalex_sources.py \
  --input inputs/computer_science_journals.csv \
  --output data/raw/computer_science_sources_resolved.csv \
  --search-fallback
```

### 2. Build the normalized sources table

```bash
PYTHONPATH=src python scripts/build_sources_table.py \
  --input data/raw/computer_science_sources_resolved.csv \
  --output data/bronze/computer_science_sources.parquet
```

### 3. Fetch recent works in resumable batches

```bash
PYTHONPATH=src python scripts/fetch_openalex_works_batch.py \
  --sources data/bronze/computer_science_sources.parquet \
  --output-dir data/bronze/computer_science_works_shards \
  --state-path data/raw/computer_science_fetch_state.csv \
  --merged-output data/bronze/computer_science_works.parquet \
  --start-year 2023 \
  --end-year 2025 \
  --resume \
  --retry-failures
```

### 4. Build the de-self-cited journal graph

```bash
PYTHONPATH=src python scripts/build_journal_graph.py \
  --works data/bronze/computer_science_works.parquet \
  --output data/silver/computer_science_journal_edges.parquet \
  --drop-self-citations
```

### 5. Compute the base PageRank

```bash
PYTHONPATH=src python scripts/compute_pagerank.py \
  --edges data/silver/computer_science_journal_edges.parquet \
  --output data/gold/computer_science_journal_pagerank.parquet
```

### 6. Export the final ranking

```bash
PYTHONPATH=src python scripts/compute_final_ranking.py \
  --rankings data/gold/computer_science_journal_pagerank.parquet \
  --resolved data/raw/computer_science_sources_resolved.csv \
  --works data/bronze/computer_science_works.parquet \
  --output-prefix data/gold/computer_science_journal_ranking_final \
  --size-mode sqrt_norm \
  --mega-journal-cap 12000 \
  --review-penalty \
  --review-threshold-quantile 0.75 \
  --review-penalty-exp 0.15
```

Outputs:

- `data/gold/computer_science_journal_ranking_final.csv`
- `data/gold/computer_science_journal_ranking_final.xlsx`
- `data/gold/computer_science_journal_ranking_final.parquet`
- `data/gold/computer_science_journal_ranking_final_top100.csv`

## Make Targets

```bash
make example-resolve
make example-sources
make example-fetch
make example-graph
make example-pagerank
make example-final
```

For a smaller smoke test, use `inputs/journals.sample.csv` or pass `--limit` during source resolution.

## Design Notes

- `fetch_openalex_works_batch.py` writes one shard per source and persists a CSV state file after every source.
- `build_journal_graph.py` can explicitly remove self-citations.
- `compute_final_ranking.py` is the GitHub-facing entrypoint for the current ranking recipe.
- `igraph` is preferred for PageRank; the code can fall back to `networkx` where applicable.

## Versioning Policy

Tracked in git:

- source code
- configuration files
- example input lists
- README and visual assets

Ignored locally:

- raw Excel workbooks
- fetched OpenAlex datasets
- intermediate Parquet shards
- local ranking outputs

## License

This repository keeps the upstream [LICENSE](LICENSE) file already present in the GitHub repository.
