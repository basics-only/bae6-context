import asyncio
import json
import traceback
from pathlib import Path

import httpx
import pytest

from bae6_context import (
    ContextApiError,
    ContextError,
    ContextInvalidResponseError,
    ContextTimeoutError,
    ContextTransportError,
    DataClient,
    Transport,
)

CASES = Path(__file__).parents[3] / "contracts/vendor/fixtures/transport/cases"


def _kind(err: ContextError) -> str:
    if isinstance(err, ContextApiError):
        return "api"
    if isinstance(err, ContextTimeoutError):
        return "timeout"
    if isinstance(err, ContextTransportError):
        return "connection"
    if isinstance(err, ContextInvalidResponseError):
        return "invalid_response"
    return "unknown"


@pytest.mark.parametrize("path", sorted(CASES.glob("*.json")), ids=lambda p: p.stem)
def test_transport_conformance(path: Path) -> None:
    case = json.loads(path.read_text())
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        responses = case["responses"]
        r = responses[min(len(seen), len(responses)) - 1]
        if r.get("drop"):
            raise httpx.RemoteProtocolError("dropped", request=request)
        await asyncio.sleep(r.get("delayMs", 0) / 1000)
        body = r.get("body")
        if isinstance(body, str):
            return httpx.Response(
                r.get("status", 200), headers=r.get("headers"), text=body
            )
        return httpx.Response(r.get("status", 200), headers=r.get("headers"), json=body)

    timeout = case.get("client", {}).get("timeoutMs", 10_000) / 1000
    req = case["request"]

    async def go() -> object:
        t = Transport("http://ctx", "svc-token", timeout, httpx.MockTransport(handler))
        try:
            return await t.request(
                req["method"],
                req["path"],
                body=req.get("body"),
                idempotency_key=req.get("idempotencyKey"),
            )
        finally:
            await t.aclose()

    expect = case["expect"]
    if "error" in expect:
        with pytest.raises(ContextError) as info:
            asyncio.run(go())
        err = info.value
        want = expect["error"]
        assert _kind(err) == want["kind"]
        fields = {
            "status": "status",
            "code": "code",
            "requestId": "request_id",
            "retryAfterSeconds": "retry_after_seconds",
        }
        for k, attr in fields.items():
            if k in want:
                assert getattr(err, attr) == want[k], k
        assert "svc-token" not in "".join(traceback.format_exception(err)) + repr(err)
    else:
        assert asyncio.run(go()) == expect["result"]
    assert len(seen) == case["expectRequestCount"]
    for s in seen:
        assert s.headers["authorization"] == "Bearer svc-token"
        if s.content:
            assert s.headers["content-type"] == "application/json"
        if "idempotencyKey" in req:
            assert s.headers["idempotency-key"] == req["idempotencyKey"]


def run(handler, call):
    async def go():
        async with DataClient(
            "http://ctx", "svc-token", transport=httpx.MockTransport(handler)
        ) as d:
            return await call(d)

    return asyncio.run(go())


def test_query_sends_source_and_end_user_tokens_and_encodes_the_source_id() -> None:
    seen: list[httpx.Request] = []
    result = {"columns": [], "rows": [], "truncated": False, "access": "delegated"}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=result)

    got = run(
        handler,
        lambda d: d.query(
            "a/b",
            "SELECT :x",
            parameters={"x": 1},
            source_token="dbx-token",
            end_user_token="user-token",
        ),
    )
    assert got == result
    assert seen[0].url.raw_path == b"/v1/sources/a%2Fb/query"
    assert seen[0].headers["bae6-source-token"] == "dbx-token"
    assert seen[0].headers["bae6-end-user-token"] == "user-token"
    assert json.loads(seen[0].content) == {
        "statement": "SELECT :x",
        "parameters": {"x": 1},
    }
    with pytest.raises(ContextInvalidResponseError):
        run(
            lambda _: httpx.Response(200, json={"rows": "x"}),
            lambda d: d.query("db", "SELECT 1"),
        )


def test_reads_a_range_and_reports_the_whole_object_size() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            206,
            content=b"world",
            headers={
                "etag": '"e1"',
                "last-modified": "Thu, 01 Jan 2026 00:00:00 GMT",
                "content-range": "bytes 6-10/11",
                "bae6-version-id": "v1",
            },
        )

    r = run(handler, lambda d: d.read_object("docs", "a b.txt", range=(6, 10)))
    assert (r.body, r.size, r.etag, r.version_id) == (b"world", 11, "e1", "v1")
    assert r.last_modified == "2026-01-01T00:00:00+00:00"
    assert seen[0].headers["range"] == "bytes=6-10"
    assert seen[0].url.params["key"] == "a b.txt"

    def no_range(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            206,
            content=b"x",
            headers={"etag": '"e"', "last-modified": "Thu, 01 Jan 2026 00:00:00 GMT"},
        )

    with pytest.raises(ContextInvalidResponseError):
        run(no_range, lambda d: d.read_object("docs", "a"))


def test_lists_and_stats_objects() -> None:
    info = {"key": "k", "size": 1, "etag": "e", "lastModified": "2026-01-01T00:00:00Z"}
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.path.endswith("/metadata"):
            return httpx.Response(200, json=info)
        if request.url.path == "/v1/sources":
            return httpx.Response(
                200, json={"items": [{"kind": "data_source", "id": "db"}]}
            )
        return httpx.Response(200, json={"items": [info], "nextCursor": "c"})

    page = run(handler, lambda d: d.list_objects("docs", prefix="p/", limit=5))
    assert page == {"items": [info], "nextCursor": "c"}
    assert dict(seen[0].url.params) == {"prefix": "p/", "limit": "5"}
    assert run(handler, lambda d: d.stat_object("docs", "k", version_id="v")) == info
    assert dict(seen[1].url.params) == {"key": "k", "versionId": "v"}
    assert run(handler, lambda d: d.sources())[0]["id"] == "db"
