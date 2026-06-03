"""Tests for SAPODataExtractor using `responses` to mock an OData service."""

from __future__ import annotations

import json
from urllib.parse import parse_qs, urlparse

import pytest
import responses

from ETLS.Extraction_Scripts.ERP_SAP_Extractor import SAPODataExtractor
from ETLS.core.exceptions import SourceConnectionError, ValidationError

BASE = "https://sap.example.com/sap/opu/odata/sap/ZSALES_SRV"
ENTITY = "SalesOrderSet"
URL = f"{BASE}/{ENTITY}"


# -- validation --------------------------------------------------------------


def test_validate_non_http():
    ext = SAPODataExtractor("ftp://x", ENTITY)
    with pytest.raises(ValidationError, match="must be http"):
        ext.validate()


def test_validate_missing_entity():
    ext = SAPODataExtractor(BASE, "")
    with pytest.raises(ValidationError, match="entity_set is required"):
        ext.validate()


def test_validate_bad_version():
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=3)
    with pytest.raises(ValidationError, match="odata_version must be 2 or 4"):
        ext.validate()


# -- V2 parsing & paging -----------------------------------------------------


@responses.activate
def test_v2_single_page():
    responses.add(
        responses.GET,
        URL,
        json={"d": {"results": [{"Id": "1"}, {"Id": "2"}]}},
        status=200,
    )
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=2)
    result = ext.extract()
    assert result.source_type == "sap_odata"
    assert result.row_count == 2
    assert result.metadata["entity_set"] == ENTITY
    assert result.metadata["odata_version"] == 2


@responses.activate
def test_v2_format_json_param_sent():
    responses.add(responses.GET, URL, json={"d": {"results": []}}, status=200)
    SAPODataExtractor(BASE, ENTITY).extract()
    assert "%24format=json" in responses.calls[0].request.url or \
        "$format=json" in responses.calls[0].request.url


@responses.activate
def test_v2_manual_skip_paging():
    page_size = 2

    def callback(request):
        qs = parse_qs(urlparse(request.url).query)
        skip = int(qs.get("$skip", ["0"])[0])
        if skip == 0:
            rows = [{"Id": "1"}, {"Id": "2"}]  # full page -> continue
        else:
            rows = [{"Id": "3"}]  # short page -> stop
        return (200, {}, json.dumps({"d": {"results": rows}}))

    responses.add_callback(responses.GET, URL, callback=callback)
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=2, page_size=page_size)
    result = ext.extract()
    assert result.row_count == 3
    assert result.metadata["pages_fetched"] == 2


@responses.activate
def test_v2_follows_next_link():
    next_url = f"{URL}?$skip=2&token=abc"
    responses.add(
        responses.GET,
        URL,
        json={"d": {"results": [{"Id": "1"}], "__next": next_url}},
        status=200,
    )
    responses.add(
        responses.GET,
        next_url,
        json={"d": {"results": [{"Id": "2"}]}},
        status=200,
    )
    # page_size > rows-per-page so the final (link-less) page is "short" and
    # iteration stops cleanly instead of falling into manual $skip paging.
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=2, page_size=10)
    result = ext.extract()
    assert list(result.data["Id"]) == ["1", "2"]


# -- V4 parsing & paging -----------------------------------------------------


@responses.activate
def test_v4_single_page():
    responses.add(
        responses.GET,
        URL,
        json={"value": [{"Id": 1}, {"Id": 2}, {"Id": 3}]},
        status=200,
    )
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=4)
    assert ext.extract().row_count == 3


@responses.activate
def test_v4_follows_odata_next_link():
    next_url = f"{URL}?$skiptoken=xyz"
    responses.add(
        responses.GET,
        URL,
        json={"value": [{"Id": 1}], "@odata.nextLink": next_url},
        status=200,
    )
    responses.add(responses.GET, next_url, json={"value": [{"Id": 2}]}, status=200)
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=4, page_size=10)
    result = ext.extract()
    assert list(result.data["Id"]) == [1, 2]


@responses.activate
def test_empty_entity_set_yields_empty_frame():
    responses.add(responses.GET, URL, json={"value": []}, status=200)
    assert SAPODataExtractor(BASE, ENTITY, odata_version=4).extract().is_empty


# -- query options, client, auth ---------------------------------------------


@responses.activate
def test_query_options_and_sap_client_sent():
    responses.add(responses.GET, URL, json={"d": {"results": []}}, status=200)
    ext = SAPODataExtractor(
        BASE,
        ENTITY,
        sap_client="100",
        query_options={"filter": "Year eq 2025", "select": "Id,Amount"},
    )
    ext.extract()
    sent = urlparse(responses.calls[0].request.url).query
    qs = parse_qs(sent)
    assert qs["sap-client"] == ["100"]
    assert qs["$filter"] == ["Year eq 2025"]
    assert qs["$select"] == ["Id,Amount"]


@responses.activate
def test_basic_auth_applied():
    responses.add(responses.GET, URL, json={"d": {"results": []}}, status=200)
    ext = SAPODataExtractor(
        BASE, ENTITY, auth={"type": "basic", "username": "u", "password": "p"}
    )
    ext.extract()
    # requests sends Basic auth as a base64 Authorization header.
    assert responses.calls[0].request.headers["Authorization"].startswith("Basic ")


@responses.activate
def test_bearer_auth_applied():
    responses.add(responses.GET, URL, json={"d": {"results": []}}, status=200)
    ext = SAPODataExtractor(BASE, ENTITY, auth={"type": "bearer", "token": "T"})
    ext.extract()
    assert responses.calls[0].request.headers["Authorization"] == "Bearer T"


def test_unsupported_auth_type_raises_on_session():
    ext = SAPODataExtractor(BASE, ENTITY, auth={"type": "kerberos"})
    with pytest.raises(ValidationError, match="Unsupported SAP auth type"):
        _ = ext.session


# -- CSRF --------------------------------------------------------------------


@responses.activate
def test_csrf_token_fetched_and_reused():
    # First call is the CSRF fetch ($top=0), returning a token header.
    responses.add(
        responses.GET,
        URL,
        json={"d": {"results": []}},
        status=200,
        headers={"X-CSRF-Token": "TKN-123"},
    )
    responses.add(responses.GET, URL, json={"d": {"results": [{"Id": "1"}]}}, status=200)
    ext = SAPODataExtractor(BASE, ENTITY, odata_version=2, fetch_csrf=True)
    result = ext.extract()
    assert result.row_count == 1
    # The data request (2nd call) carries the fetched token.
    assert responses.calls[1].request.headers.get("X-CSRF-Token") == "TKN-123"


# -- HTTP errors -------------------------------------------------------------


@responses.activate
def test_401_raises_connection_error():
    responses.add(responses.GET, URL, json={}, status=401)
    with pytest.raises(SourceConnectionError, match="authentication failed"):
        SAPODataExtractor(BASE, ENTITY).extract()


@responses.activate
def test_4xx_raises_extraction_error():
    from ETLS.core.exceptions import ExtractionError

    responses.add(responses.GET, URL, body="bad request", status=400)
    with pytest.raises(ExtractionError, match="SAP HTTP 400"):
        SAPODataExtractor(BASE, ENTITY).extract()


@responses.activate
def test_5xx_retried_then_recovers():
    responses.add(responses.GET, URL, json={}, status=502)
    responses.add(responses.GET, URL, json={"d": {"results": [{"Id": "1"}]}}, status=200)
    result = SAPODataExtractor(BASE, ENTITY, odata_version=2).extract()
    assert result.row_count == 1
    assert len(responses.calls) == 2


# -- config ------------------------------------------------------------------


def test_from_config(tmp_path):
    cfg = tmp_path / "sap.yaml"
    cfg.write_text(
        "sap:\n"
        f"  base_url: {BASE}\n"
        f"  entity_set: {ENTITY}\n"
        "  sap_client: '200'\n"
        "  odata_version: 4\n"
        "  auth:\n"
        "    type: bearer\n"
        "    token: tok\n",
        encoding="utf-8",
    )
    ext = SAPODataExtractor.from_config(cfg)
    assert ext.entity_set == ENTITY
    assert ext.sap_client == "200"
    assert ext.odata_version == 4
    assert ext.auth_spec["token"] == "tok"


def test_base_url_trailing_slash_stripped():
    ext = SAPODataExtractor(BASE + "/", ENTITY)
    assert ext.base_url == BASE
