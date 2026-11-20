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
    EmbedClient,
    McpGatewayClient,
    RegistryClient,
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
                200,
                json={
                    "items": [{"kind": "data_source", "id": "db"}],
                    "unavailable": ["lake"],
                },
            )
        return httpx.Response(200, json={"items": [info], "nextCursor": "c"})

    page = run(handler, lambda d: d.list_objects("docs", prefix="p/", limit=5))
    assert page == {"items": [info], "nextCursor": "c"}
    assert dict(seen[0].url.params) == {"prefix": "p/", "limit": "5"}
    assert run(handler, lambda d: d.stat_object("docs", "k", version_id="v")) == info
    assert dict(seen[1].url.params) == {"key": "k", "versionId": "v"}
    listed = run(handler, lambda d: d.sources())
    assert (listed["items"][0]["id"], listed["unavailable"]) == ("db", ["lake"])


def run_embed(handler, call):
    async def go():
        async with EmbedClient(
            "http://ctx", "svc-token", transport=httpx.MockTransport(handler)
        ) as e:
            return await call(e)

    return asyncio.run(go())


def test_embed_calls_hit_the_documented_routes_and_encode_caller_parts() -> None:
    seen: list[httpx.Request] = []
    sync = {"id": "s1", "state": "queued", "prefix": "docs/", "attempts": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.method == "PUT":
            return httpx.Response(200, json={"id": "kb", "model": "m", "dimensions": 8})
        if request.method == "DELETE":
            return httpx.Response(204)
        return httpx.Response(202 if request.method == "POST" else 200, json=sync)

    assert run_embed(handler, lambda e: e.create_collection("kb"))["dimensions"] == 8
    assert run_embed(handler, lambda e: e.start_sync("kb", prefix="docs/")) == sync
    assert run_embed(handler, lambda e: e.get_sync("kb", "s/1")) == sync
    assert run_embed(handler, lambda e: e.delete_document("kb", "a b&c.txt")) is None
    assert [(r.method, r.url.raw_path.split(b"?")[0]) for r in seen] == [
        ("PUT", b"/v1/collections/kb"),
        ("POST", b"/v1/collections/kb/syncs"),
        ("GET", b"/v1/collections/kb/syncs/s%2F1"),
        ("DELETE", b"/v1/collections/kb/documents"),
    ]
    assert json.loads(seen[1].content) == {"prefix": "docs/"}
    assert seen[3].url.params["key"] == "a b&c.txt"


def test_embed_search_returns_hits_and_never_sends_an_end_user_token() -> None:
    seen: list[httpx.Request] = []
    hit = {"chunkId": "c", "key": "k", "revision": "r", "start": 0, "end": 1}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"hits": [hit]})

    assert run_embed(handler, lambda e: e.search("kb", "q", limit=3)) == [hit]
    assert json.loads(seen[0].content) == {"query": "q", "limit": 3}
    assert "bae6-end-user-token" not in seen[0].headers


def test_embed_errors_surface_the_envelope_and_a_queued_sync_is_not_retried() -> None:
    envelope = {"code": "not_found", "message": "Not found.", "requestId": "r1"}
    with pytest.raises(ContextApiError) as err:
        run_embed(
            lambda _: httpx.Response(404, json=envelope),
            lambda e: e.search("nope", "q"),
        )
    assert (err.value.status, err.value.code, err.value.request_id) == (
        404,
        "not_found",
        "r1",
    )
    with pytest.raises(ContextInvalidResponseError):
        run_embed(
            lambda _: httpx.Response(200, json={"nothits": []}),
            lambda e: e.search("kb", "q"),
        )
    calls = 0

    def unavailable(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"code": "unavailable", "message": "x"})

    with pytest.raises(ContextApiError):
        run_embed(unavailable, lambda e: e.start_sync("kb"))
    assert calls == 1


def test_embed_refuses_dot_segments_and_unknown_sync_states() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200, json={"id": "s1", "state": "bogus", "prefix": "", "attempts": 0}
        )

    with pytest.raises(ContextError):
        run_embed(handler, lambda e: e.get_sync("..", "s1"))
    with pytest.raises(ContextError):
        run_embed(handler, lambda e: e.search(".", "q"))
    assert seen == []
    with pytest.raises(ContextInvalidResponseError):
        run_embed(handler, lambda e: e.get_sync("kb", "s1"))


def run_registry(handler, call):
    async def go():
        async with RegistryClient(
            "http://ctx", "svc-token", transport=httpx.MockTransport(handler)
        ) as r:
            return await call(r)

    return asyncio.run(go())


VERSION = {
    "version": "1.0.0",
    "endpoint": "https://mcp.example.com/mcp",
    "protocolVersion": "2025-06-18",
    "transport": "streamable-http",
    "authentication": {"method": "none"},
}
MCP = {
    "id": "docs",
    "organization": "acme",
    "team": "platform",
    "name": "Docs",
    "createdAt": "2026-01-01T00:00:00Z",
    "versions": [],
}


def test_registry_calls_hit_the_documented_routes_and_carry_the_end_user_token() -> (
    None
):
    seen: list[httpx.Request] = []
    health = {"version": "1.0.0", "status": "healthy", "checkedAt": "2026-01-01"}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.raw_path.split(b"?")[0]
        if path == b"/v1/servers" and request.method == "GET":
            return httpx.Response(200, json={"items": [MCP], "nextCursor": "c2"})
        if path.endswith(b"/health"):
            return httpx.Response(200, json=health)
        return httpx.Response(201 if request.method == "POST" else 200, json=MCP)

    calls = [
        lambda r: r.create_server(
            "docs", "platform", "Docs", VERSION, end_user_token="user-token"
        ),
        lambda r: r.create_version("docs", VERSION),
        lambda r: r.approve_version("docs", "1.0.0", scan_status="clean"),
        lambda r: r.revoke_version("docs"),
        lambda r: r.get_server("docs"),
        lambda r: r.list_servers(cursor="c1", limit=5),
        lambda r: r.get_server_health("docs", version="1.0.0"),
    ]
    results = [run_registry(handler, call) for call in calls]
    assert [(r.method, r.url.raw_path) for r in seen] == [
        ("POST", b"/v1/servers"),
        ("POST", b"/v1/servers/docs/versions"),
        ("POST", b"/v1/servers/docs/approve"),
        ("POST", b"/v1/servers/docs/revoke"),
        ("GET", b"/v1/servers/docs"),
        ("GET", b"/v1/servers?cursor=c1&limit=5"),
        ("GET", b"/v1/servers/docs/health?version=1.0.0"),
    ]
    assert seen[0].headers["bae6-end-user-token"] == "user-token"
    assert "bae6-end-user-token" not in seen[1].headers
    assert json.loads(seen[2].content) == {"version": "1.0.0", "scanStatus": "clean"}
    assert json.loads(seen[3].content) == {}
    assert results[5]["nextCursor"] == "c2"


def test_registry_errors_surface_the_envelope_and_a_mutation_is_not_retried() -> None:
    envelope = {"code": "conflict", "message": "exists", "requestId": "r2"}
    with pytest.raises(ContextApiError) as err:
        run_registry(
            lambda _: httpx.Response(409, json=envelope),
            lambda r: r.create_server("docs", "platform", "Docs", VERSION),
        )
    assert (err.value.status, err.value.code, err.value.request_id) == (
        409,
        "conflict",
        "r2",
    )
    calls = 0

    def unavailable(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"code": "unavailable", "message": "x"})

    with pytest.raises(ContextApiError):
        run_registry(unavailable, lambda r: r.approve_version("docs", "1.0.0"))
    assert calls == 1
    with pytest.raises(ContextApiError):
        run_registry(unavailable, lambda r: r.get_server("docs"))
    assert calls == 4


def test_registry_refuses_dot_segments_and_unusable_bodies() -> None:
    seen: list[httpx.Request] = []

    def respond(body: object):
        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json=body)

        return handler

    with pytest.raises(ContextError):
        run_registry(respond(MCP), lambda r: r.get_server(".."))
    with pytest.raises(ContextError):
        run_registry(respond(MCP), lambda r: r.revoke_version("."))
    assert seen == []
    with pytest.raises(ContextInvalidResponseError):
        run_registry(respond({"id": "docs"}), lambda r: r.get_server("docs"))
    with pytest.raises(ContextInvalidResponseError):
        run_registry(respond({"items": [{"id": "docs"}]}), lambda r: r.list_servers())
    bad = {"version": "1", "status": "meh", "checkedAt": "x"}
    with pytest.raises(ContextInvalidResponseError):
        run_registry(respond(bad), lambda r: r.get_server_health("docs"))


def test_registry_refuses_an_empty_id_and_a_response_about_another_server() -> None:
    seen: list[httpx.Request] = []

    def respond(body: object):
        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return httpx.Response(200, json=body)

        return handler

    with pytest.raises(ContextError):
        run_registry(respond(MCP), lambda r: r.get_server(""))
    assert seen == []
    no_team = {k: v for k, v in MCP.items() if k != "team"}
    for bad in (
        {**MCP, "id": "other"},
        {**MCP, "versions": [None]},
        {**MCP, "versions": [{"state": "bogus"}]},
        no_team,
    ):
        with pytest.raises(ContextInvalidResponseError):
            run_registry(respond(bad), lambda r: r.approve_version("docs", "1.0.0"))
    with pytest.raises(ContextInvalidResponseError):
        run_registry(
            respond({"items": [MCP], "nextCursor": 5}), lambda r: r.list_servers()
        )
    stale = {"version": "9", "status": "healthy", "checkedAt": "x"}
    with pytest.raises(ContextInvalidResponseError):
        run_registry(respond(stale), lambda r: r.get_server_health("docs", version="1"))


def test_registry_sends_bare_urls_and_full_create_bodies() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        path = request.url.raw_path
        if path == b"/v1/servers" and request.method == "GET":
            return httpx.Response(200, json={"items": []})
        if path.endswith(b"/health"):
            return httpx.Response(
                200, json={"version": "1", "status": "healthy", "checkedAt": "x"}
            )
        return httpx.Response(201, json=MCP)

    run_registry(handler, lambda r: r.list_servers())
    run_registry(handler, lambda r: r.get_server_health("docs"))
    run_registry(
        handler,
        lambda r: r.create_server("docs", "platform", "Docs", VERSION, description="d"),
    )
    run_registry(handler, lambda r: r.create_version("docs", VERSION))
    assert [r.url.raw_path for r in seen] == [
        b"/v1/servers",
        b"/v1/servers/docs/health",
        b"/v1/servers",
        b"/v1/servers/docs/versions",
    ]
    assert json.loads(seen[2].content) == {
        "id": "docs",
        "team": "platform",
        "name": "Docs",
        "description": "d",
        "version": VERSION,
    }
    assert json.loads(seen[3].content) == VERSION
    assert all(r.headers["authorization"] == "Bearer svc-token" for r in seen)


def test_a_dropped_connection_on_a_registry_mutation_is_not_retried() -> None:
    calls = 0

    def drop(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.RemoteProtocolError("dropped", request=request)

    with pytest.raises(ContextTransportError):
        run_registry(drop, lambda r: r.revoke_version("docs"))
    assert calls == 1


def run_gateway(handler, call):
    async def go():
        async with McpGatewayClient(
            "http://ctx", "svc-token", transport=httpx.MockTransport(handler)
        ) as g:
            return await call(g)

    return asyncio.run(go())


CALLED = {"server": "docs", "version": "1.0.0", "tool": "echo", "result": {"x": 1}}


def test_call_tool_posts_to_the_documented_route_with_the_end_user_token() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=CALLED)

    out = run_gateway(
        handler,
        lambda g: g.call_tool(
            "docs", "echo", {"a": 1}, version="1.0.0", end_user_token="user-token"
        ),
    )
    assert out["result"] == {"x": 1}
    assert (seen[0].method, seen[0].url.path) == ("POST", "/v1/servers/docs/tools/call")
    assert seen[0].headers["bae6-end-user-token"] == "user-token"
    assert json.loads(seen[0].content) == {
        "tool": "echo",
        "arguments": {"a": 1},
        "version": "1.0.0",
    }


def test_call_tool_is_never_retried_and_surfaces_the_envelope() -> None:
    calls = 0

    def timeout(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            504, json={"code": "upstream_timeout", "message": "x", "requestId": "r9"}
        )

    with pytest.raises(ContextApiError) as err:
        run_gateway(timeout, lambda g: g.call_tool("docs", "echo"))
    assert (err.value.status, err.value.code, err.value.request_id) == (
        504,
        "upstream_timeout",
        "r9",
    )

    def drop(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        raise httpx.RemoteProtocolError("dropped", request=request)

    with pytest.raises(ContextTransportError):
        run_gateway(drop, lambda g: g.call_tool("docs", "echo"))
    assert calls == 2


def test_call_tool_refuses_dot_segments_and_a_reply_about_another_server_tool_or_version() -> (
    None
):
    calls = 0

    def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=CALLED)

    with pytest.raises(ContextError):
        run_gateway(handler, lambda g: g.call_tool("..", "echo"))
    assert calls == 0
    for bad in (
        {**CALLED, "server": "other"},
        {**CALLED, "tool": "rm"},
        {**CALLED, "version": "2.0.0"},
        {k: v for k, v in CALLED.items() if k != "result"},
    ):
        with pytest.raises(ContextInvalidResponseError):
            run_gateway(
                lambda _, bad=bad: httpx.Response(200, json=bad),
                lambda g: g.call_tool("docs", "echo", version="1.0.0"),
            )
