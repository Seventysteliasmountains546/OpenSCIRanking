PYTHON ?= python
PYTHONPATH := src

.PHONY: install example-resolve example-sources example-fetch example-graph example-pagerank example-final

install:
	$(PYTHON) -m pip install -r requirements.txt

example-resolve:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/resolve_openalex_sources.py \
		--input inputs/computer_science_journals.csv \
		--output data/raw/computer_science_sources_resolved.csv \
		--search-fallback

example-sources:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/build_sources_table.py \
		--input data/raw/computer_science_sources_resolved.csv \
		--output data/bronze/computer_science_sources.parquet

example-fetch:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/fetch_openalex_works_batch.py \
		--sources data/bronze/computer_science_sources.parquet \
		--output-dir data/bronze/computer_science_works_shards \
		--state-path data/raw/computer_science_fetch_state.csv \
		--merged-output data/bronze/computer_science_works.parquet \
		--start-year 2023 \
		--end-year 2025 \
		--resume \
		--retry-failures

example-graph:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/build_journal_graph.py \
		--works data/bronze/computer_science_works.parquet \
		--output data/silver/computer_science_journal_edges.parquet \
		--drop-self-citations

example-pagerank:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/compute_pagerank.py \
		--edges data/silver/computer_science_journal_edges.parquet \
		--output data/gold/computer_science_journal_pagerank.parquet

example-final:
	PYTHONPATH=$(PYTHONPATH) $(PYTHON) scripts/compute_final_ranking.py \
		--rankings data/gold/computer_science_journal_pagerank.parquet \
		--resolved data/raw/computer_science_sources_resolved.csv \
		--works data/bronze/computer_science_works.parquet \
		--output-prefix data/gold/computer_science_journal_ranking_final \
		--size-mode sqrt_norm \
		--mega-journal-cap 12000 \
		--review-penalty \
		--review-threshold-quantile 0.75 \
		--review-penalty-exp 0.15
