# OpenSci V2

`OpenSci V2` is a reproducible journal-ranking pipeline built on top of OpenAlex.

这是一个面向 GitHub 公开使用的期刊排序项目模板。当前仓库以“计算机科学期刊排序”为完整示例，保留了从期刊名单解析、OpenAlex 抓取、期刊引用图构建，到最终排名输出的全流程源码。

The project is designed for bottom-up ranking rather than top-down expert partitioning:

- resolve journals to OpenAlex sources
- fetch recent works in resumable batches
- build an inter-journal citation graph
- compute PageRank on the de-self-cited graph
- apply transparent post-adjustments such as `sqrt_norm`, mega-journal penalties, and a mild review-heavy penalty

The current repository uses **computer science** as the worked example.

## Repository goals

- keep the codebase clean enough for GitHub
- let other users clone the repo and reproduce the ranking workflow
- provide a realistic example subject list under `inputs/computer_science_journals.csv`
- separate source code from local data products

## Project layout

- `inputs/`: seed lists and example journal lists
- `src/opensci_v2/`: reusable Python package code
- `scripts/`: CLI entrypoints
- `data/`: local intermediate and output files, ignored by git
- `cas_data/`: local raw workbooks, ignored by git

## Install

Requirements:

- Python `>= 3.11`
- network access to OpenAlex API
- enough local disk for fetched Parquet data

```bash
python -m pip install -r requirements.txt
```

or:

```bash
make install
```

## Example workflow: Computer Science

The repository includes an extracted computer science journal list:

- `inputs/computer_science_journals.csv`

### 1. Resolve journals to OpenAlex sources

```bash
PYTHONPATH=src python scripts/resolve_openalex_sources.py \
  --input inputs/computer_science_journals.csv \
  --output data/raw/computer_science_sources_resolved.csv \
  --search-fallback
```

### 2. Convert resolved sources into a working Parquet table

```bash
PYTHONPATH=src python scripts/build_sources_table.py \
  --input data/raw/computer_science_sources_resolved.csv \
  --output data/bronze/computer_science_sources.parquet
```

### 3. Fetch recent works with resumable batching

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

### 5. Compute base PageRank

```bash
PYTHONPATH=src python scripts/compute_pagerank.py \
  --edges data/silver/computer_science_journal_edges.parquet \
  --output data/gold/computer_science_journal_pagerank.parquet
```

### 6. Compute the final ranking

The current default recipe in this repo is:

- base PageRank on a de-self-cited journal graph
- `sqrt_norm` size normalization
- mega-journal penalty with `cap = 12000`
- mild review-heavy penalty based on average references per paper

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

This produces:

- `data/gold/computer_science_journal_ranking_final.csv`
- `data/gold/computer_science_journal_ranking_final.xlsx`
- `data/gold/computer_science_journal_ranking_final.parquet`
- `data/gold/computer_science_journal_ranking_final_top100.csv`

## Make targets

For the computer science example, you can also use:

```bash
make example-resolve
make example-sources
make example-fetch
make example-graph
make example-pagerank
make example-final
```

If you only want a quick smoke test after cloning, run the same workflow on a smaller list such as `inputs/journals.sample.csv` or add `--limit` when resolving sources.

## Design notes

- `build_journal_graph.py` removes self-citations when requested.
- `fetch_openalex_works_batch.py` writes one shard per source and persists a state file after every source.
- `compute_final_ranking.py` is the GitHub-facing entrypoint for the current scoring recipe.
- `igraph` is preferred for PageRank; if unavailable, the code falls back to `networkx` where possible.

## What is and is not versioned

Tracked:

- source code
- configuration
- example input lists
- README and workflow documentation

Ignored:

- local raw Excel workbooks
- fetched OpenAlex data
- intermediate Parquet shards
- local ranking outputs
