from __future__ import annotations

import re
import time
from typing import Iterator

import requests
from requests import Response


BASE_URL = "https://api.openalex.org"


class OpenAlexClient:
    def __init__(
        self,
        mailto: str | None = None,
        per_page: int = 200,
        delay_seconds: float = 0.1,
        max_retries: int = 4,
        backoff_seconds: float = 2.0,
    ):
        self.mailto = mailto
        self.per_page = per_page
        self.delay_seconds = delay_seconds
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = requests.Session()

    def _params(self, extra: dict[str, str]) -> dict[str, str]:
        params = {"per-page": str(self.per_page), **extra}
        if self.mailto:
            params["mailto"] = self.mailto
        return params

    def paginate(self, entity: str, params: dict[str, str]) -> Iterator[dict]:
        cursor = "*"
        while True:
            current = self._params({**params, "cursor": cursor})
            response = self._request("GET", f"{BASE_URL}/{entity}", params=current)
            payload = response.json()
            for result in payload.get("results", []):
                yield result

            meta = payload.get("meta", {})
            next_cursor = meta.get("next_cursor")
            if not next_cursor:
                break
            cursor = next_cursor
            time.sleep(self.delay_seconds)

    @staticmethod
    def _normalize_openalex_id(identifier: str) -> str:
        return identifier.rstrip("/").split("/")[-1]

    @staticmethod
    def normalize_issn(value: str | None) -> str:
        if not value:
            return ""
        cleaned = re.sub(r"[^0-9Xx]", "", str(value)).upper()
        if len(cleaned) != 8:
            return ""
        return f"{cleaned[:4]}-{cleaned[4:]}"

    def get_entity(self, entity: str, identifier: str) -> dict:
        normalized_id = self._normalize_openalex_id(identifier)
        params = {}
        if self.mailto:
            params["mailto"] = self.mailto
        response = self._request("GET", f"{BASE_URL}/{entity}/{normalized_id}", params=params)
        return response.json()

    def get_source_by_issn(self, issn: str) -> dict:
        normalized_issn = self.normalize_issn(issn)
        if not normalized_issn:
            raise ValueError(f"Invalid ISSN: {issn}")

        params = {}
        if self.mailto:
            params["mailto"] = self.mailto
        response = self._request("GET", f"{BASE_URL}/sources/issn:{normalized_issn}", params=params)
        return response.json()

    def search_sources(self, query: str, per_page: int = 10) -> list[dict]:
        if not query or not query.strip():
            return []
        response = self._request(
            "GET",
            f"{BASE_URL}/sources",
            params=self._params({"search": query.strip(), "per-page": str(per_page)}),
        )
        return response.json().get("results", [])

    def get_sources_by_ids(self, openalex_ids: list[str]) -> list[dict]:
        return [self.get_entity("sources", openalex_id) for openalex_id in openalex_ids if openalex_id]

    def get_works_for_source(self, source_id: str, start_year: int, end_year: int | None = None) -> Iterator[dict]:
        year_filter = f"from_publication_date:{start_year}-01-01"
        if end_year:
            year_filter += f",to_publication_date:{end_year}-12-31"
        filter_value = f"primary_location.source.id:{source_id},{year_filter},type:article|review"
        return self.paginate("works", {"filter": filter_value, "select": ",".join([
            "id",
            "title",
            "publication_year",
            "cited_by_count",
            "referenced_works",
            "primary_location",
            "authorships",
        ])})

    def _request(self, method: str, url: str, **kwargs) -> Response:
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.request(method, url, timeout=60, **kwargs)
                if response.status_code in (429, 500, 502, 503, 504):
                    response.raise_for_status()
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise
                sleep_seconds = self.backoff_seconds * attempt
                time.sleep(sleep_seconds)
        if last_error is not None:
            raise last_error
        raise RuntimeError("OpenAlex request failed without an exception")
