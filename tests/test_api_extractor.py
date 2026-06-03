"""Tests for APIExtractor using the `responses` library to mock HTTP."""

from __future__ import annotations

import json

import pytest
import responses

from ETLS.Extraction_Scripts.API_Extractor import APIExtractor
from ETLS.core.exceptions import ExtractionError, ValidationError

URL = "https://api.example.com/items"


# -- validation --------------------------------------------------------------


def test_validate_rejects_non_http():
    ext = APIExtractor("ftp://example.com/data")
    with pytest.raises(ValidationError, match="must be http"):
        ext.validate()


def test_validate_missing_auth_field():
    ext = APIExtractor(URL, auth={"type": "bearer"})  # no token
    with pytest.raises(ValidationError, match="missing required field"):
        ext.validate()


# -- single page -------------------------------------------------------------


@responses.activate
def test_single_page_records_path():
    responses.add(
        responses.GET,
        URL,
        json={"data": {"items": [{"id": 1}, {"id": 2}]}},
        status=200,
    )
    ext = APIExtractor(URL, records_path="data.items")
    result = ext.extract()
    assert result.source_type == "api"
    assert result.row_count == 2
    assert list(result.data["id"]) == [1, 2]
    assert result.metadata["pages_fetched"] == 1


@responses.activate
def test_whole_body_is_records_when_no_path():
    responses.add(responses.GET, URL, json=[{"a": 1}, {"a": 2}], status=200)
    result = APIExtractor(URL).extract()
    assert result.row_count == 2


@responses.activate
def test_empty_response_yields_empty_frame():
    responses.add(responses.GET, URL, json=[], status=200)
    result = APIExtractor(URL).extract()
    assert result.is_empty


@responses.activate
def test_single_dict_becomes_one_row():
    responses.add(responses.GET, URL, json={"id": 99}, status=200)
    result = APIExtractor(URL).extract()
    assert result.row_count == 1


# -- auth --------------------------------------------------------------------


@responses.activate
def test_bearer_auth_header_sent():
    responses.add(responses.GET, URL, json=[{"x": 1}], status=200)
    ext = APIExtractor(URL, auth={"type": "bearer", "token": "abc123"})
    ext.extract()
    assert responses.calls[0].request.headers["Authorization"] == "Bearer abc123"


@responses.activate
def test_api_key_header_auth():
    responses.add(responses.GET, URL, json=[{"x": 1}], status=200)
    ext = APIExtractor(
        URL, auth={"type": "api_key", "key": "K", "name": "X-Key", "in": "header"}
    )
    ext.extract()
    assert responses.calls[0].request.headers["X-Key"] == "K"


@responses.activate
def test_api_key_query_auth():
    responses.add(responses.GET, URL, json=[{"x": 1}], status=200)
    ext = APIExtractor(
        URL, auth={"type": "api_key", "key": "K", "name": "api_key", "in": "query"}
    )
    ext.extract()
    assert "api_key=K" in responses.calls[0].request.url


def test_unsupported_auth_type():
    ext = APIExtractor(URL, auth={"type": "oauth1"})
    with pytest.raises(ValidationError, match="Unsupported auth type"):
        ext.validate()


# -- HTTP errors -------------------------------------------------------------


@responses.activate
def test_4xx_raises_extraction_error():
    responses.add(responses.GET, URL, json={"error": "nope"}, status=404)
    with pytest.raises(ExtractionError, match="HTTP 404"):
        APIExtractor(URL).extract()


@responses.activate
def test_5xx_is_retried_then_fails():
    for _ in range(4):  # 4 attempts configured on _request
        responses.add(responses.GET, URL, json={}, status=503)
    # After exhausting retries the last ConnectionError propagates; BaseExtractor
    # translates the non-ETL error into an ExtractionError.
    with pytest.raises(ExtractionError):
        APIExtractor(URL).extract()
    assert len(responses.calls) == 4


@responses.activate
def test_5xx_then_success_recovers():
    responses.add(responses.GET, URL, json={}, status=500)
    responses.add(responses.GET, URL, json=[{"id": 1}], status=200)
    result = APIExtractor(URL).extract()
    assert result.row_count == 1
    assert len(responses.calls) == 2


# -- pagination --------------------------------------------------------------


@responses.activate
def test_page_pagination():
    def callback(request):
        from urllib.parse import parse_qs, urlparse

        qs = parse_qs(urlparse(request.url).query)
        page = int(qs["page"][0])
        size = int(qs["page_size"][0])
        # 2 full pages of `size`, then a short page that stops iteration.
        if page == 1:
            rows = [{"id": i} for i in range(size)]
        elif page == 2:
            rows = [{"id": i} for i in range(size)]
        else:
            rows = [{"id": 999}]
        return (200, {}, json.dumps({"items": rows}))

    responses.add_callback(responses.GET, URL, callback=callback)
    ext = APIExtractor(
        URL,
        records_path="items",
        pagination={"type": "page", "page_size": 2},
    )
    result = ext.extract()
    assert result.row_count == 5  # 2 + 2 + 1
    assert result.metadata["pages_fetched"] == 3


@responses.activate
def test_offset_pagination():
    def callback(request):
        from urllib.parse import parse_qs, urlparse

        qs = parse_qs(urlparse(request.url).query)
        offset = int(qs["offset"][0])
        limit = int(qs["limit"][0])
        if offset == 0:
            rows = [{"id": i} for i in range(limit)]  # full page
        else:
            rows = [{"id": 100}]  # short page -> stop
        return (200, {}, json.dumps(rows))

    responses.add_callback(responses.GET, URL, callback=callback)
    ext = APIExtractor(URL, pagination={"type": "offset", "limit": 3})
    result = ext.extract()
    assert result.row_count == 4


@responses.activate
def test_cursor_pagination_with_next_url():
    page2 = URL + "?cursor=2"
    responses.add(
        responses.GET, URL,
        json={"items": [{"id": 1}], "next": page2}, status=200,
    )
    responses.add(
        responses.GET, page2,
        json={"items": [{"id": 2}], "next": None}, status=200,
    )
    ext = APIExtractor(
        URL,
        records_path="items",
        pagination={"type": "cursor", "next_path": "next"},
    )
    result = ext.extract()
    assert list(result.data["id"]) == [1, 2]


@responses.activate
def test_unsupported_pagination_type():
    responses.add(responses.GET, URL, json=[{"id": 1}], status=200)
    ext = APIExtractor(URL, pagination={"type": "keyset"})
    # ValidationError is an ETLError, so BaseExtractor lets it propagate unwrapped.
    with pytest.raises(ValidationError, match="Unsupported pagination type"):
        ext.extract()


@responses.activate
def test_max_pages_caps_iteration():
    # Every page is "full" so paging would never stop on its own.
    responses.add_callback(
        responses.GET,
        URL,
        callback=lambda r: (200, {}, json.dumps({"items": [{"id": 1}, {"id": 2}]})),
    )
    ext = APIExtractor(
        URL,
        records_path="items",
        pagination={"type": "page", "page_size": 2},
        max_pages=3,
    )
    result = ext.extract()
    assert result.metadata["pages_fetched"] == 3


# -- record digging ----------------------------------------------------------


@responses.activate
def test_records_path_missing_yields_empty():
    responses.add(responses.GET, URL, json={"other": [1, 2]}, status=200)
    result = APIExtractor(URL, records_path="data.items").extract()
    assert result.is_empty


@responses.activate
def test_non_list_non_dict_at_path_raises():
    responses.add(responses.GET, URL, json={"data": 42}, status=200)
    with pytest.raises(ExtractionError, match="Expected list/dict"):
        APIExtractor(URL, records_path="data").extract()


# -- config / lifecycle ------------------------------------------------------


def test_from_config(tmp_path):
    cfg = tmp_path / "api.yaml"
    cfg.write_text(
        "api:\n"
        f"  base_url: {URL}\n"
        "  records_path: data.items\n"
        "  auth:\n"
        "    type: bearer\n"
        "    token: tok\n",
        encoding="utf-8",
    )
    ext = APIExtractor.from_config(cfg)
    assert ext.base_url == URL
    assert ext.records_path == "data.items"
    assert ext.auth_spec["token"] == "tok"


def test_context_manager_closes_session():
    with APIExtractor(URL) as ext:
        _ = ext.session
        assert ext._session is not None
    assert ext._session is None
