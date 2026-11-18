import asyncio
import json
import random
import re
from dataclasses import dataclass
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Any, Self
from urllib.parse import quote

import httpx

MAX_RESPONSE_BYTES = 64 * 1024 * 1024
MAX_ATTEMPTS = 3
RETRYABLE = {502, 503, 504}


class ContextError(Exception):
    def __init__(self, message: str, request_id: str | None = None) -> None:
        super().__init__(message)
        self.request_id = request_id


class ContextApiError(ContextError):
    """The service answered with an error; `code` is None when the body was not an envelope."""

    def __init__(
        self,
        status: int,
        code: str | None,
        message: str,
        request_id: str | None = None,
        retry_after_seconds: int | None = None,
    ) -> None:
        super().__init__(message, request_id)
        self.status = status
        self.code = code
        self.retry_after_seconds = retry_after_seconds


class ContextInvalidResponseError(ContextError):
    """A 2xx response this SDK cannot use."""


class ContextTransportError(ContextError):
    """The service was not reached or the connection failed."""


class ContextTimeoutError(ContextTransportError):
    pass


@dataclass(frozen=True)
class ObjectContent:
    body: bytes
    size: int
    """The whole object's size, also for a range."""
    etag: str
    last_modified: str
    version_id: str | None = None


def _parse(raw: bytes) -> object:
    try:
        return json.loads(raw)
    except (ValueError, RecursionError):
        return None


def _api_error(status: int, raw: bytes) -> ContextApiError:
    body = _parse(raw)
    envelope = (
        body if isinstance(body, dict) and isinstance(body.get("code"), str) else {}
    )
    message = envelope.get("message")
    request_id = envelope.get("requestId")
    retry = envelope.get("retryAfterSeconds")
    return ContextApiError(
        status,
        envelope.get("code"),
        message if isinstance(message, str) else f"the service returned HTTP {status}",
        request_id if isinstance(request_id, str) else None,
        retry if isinstance(retry, int) and not isinstance(retry, bool) else None,
    )


def _invalid() -> ContextInvalidResponseError:
    return ContextInvalidResponseError("the service returned an unusable response")


class Transport:
    """Shared bae6 SDK transport: per-attempt timeout, no redirects, bounded retries of
    idempotent requests only."""

    def __init__(
        self,
        base_url: str,
        credentials: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._timeout = timeout
        self._http = httpx.AsyncClient(
            base_url=base_url,
            headers={"authorization": f"Bearer {credentials}"},
            timeout=None,
            # Credentials must not follow a redirect to another host.
            follow_redirects=False,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._http.aclose()

    async def request(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> Any:
        response = await self.raw(
            method,
            path,
            body=body,
            headers=headers,
            params=params,
            idempotency_key=idempotency_key,
        )
        parsed = _parse(response.content)
        if parsed is None and response.content.strip() != b"null":
            raise _invalid()
        return parsed

    async def raw(
        self,
        method: str,
        path: str,
        *,
        body: Any = None,
        headers: dict[str, str] | None = None,
        params: dict[str, str] | None = None,
        idempotency_key: str | None = None,
    ) -> httpx.Response:
        idempotent = method == "GET" or idempotency_key is not None
        send_headers = dict(headers or {})
        if idempotency_key is not None:
            send_headers["idempotency-key"] = idempotency_key
        attempt = 0
        while True:
            attempt += 1
            last = not idempotent or attempt == MAX_ATTEMPTS
            try:
                response = await self._once(method, path, body, send_headers, params)
            except ContextTransportError:
                if last:
                    raise
                await _backoff(attempt)
                continue
            if 200 <= response.status_code < 300:
                return response
            error = _api_error(response.status_code, response.content)
            if (
                last
                or response.status_code not in RETRYABLE
                or (error.retry_after_seconds or 0) > 1
            ):
                raise error
            await _backoff(attempt)

    async def _once(
        self,
        method: str,
        path: str,
        body: Any,
        headers: dict[str, str],
        params: dict[str, str] | None,
    ) -> httpx.Response:
        try:
            async with asyncio.timeout(self._timeout):
                request = self._http.build_request(
                    method,
                    path,
                    content=None if body is None else json.dumps(body).encode(),
                    headers={**headers, "content-type": "application/json"}
                    if body is not None
                    else headers,
                    params=params,
                )
                response = await self._http.send(request, stream=True)
                try:
                    chunks = []
                    size = 0
                    async for chunk in response.aiter_bytes():
                        size += len(chunk)
                        if size > MAX_RESPONSE_BYTES:
                            raise ContextTransportError(
                                "the service response is too large"
                            )
                        chunks.append(chunk)
                finally:
                    await response.aclose()
        except TimeoutError:
            raise ContextTimeoutError(
                f"the service did not answer within {self._timeout} s"
            ) from None
        except httpx.HTTPError as e:
            # Raised `from None`: the cause can quote a header value, i.e. a token.
            raise ContextTransportError(
                f"could not reach the service ({type(e).__name__})"
            ) from None
        return httpx.Response(
            response.status_code, headers=response.headers, content=b"".join(chunks)
        )


async def _backoff(attempt: int) -> None:
    await asyncio.sleep(random.random() * 0.04 * attempt)


def _source(source_id: str) -> str:
    return f"/v1/sources/{quote(source_id, safe='')}"


class DataClient:
    """The bae6-context data API: tenant-scoped, read-only access to configured sources.

    Use `async with DataClient(...) as data` so connections are closed.
    """

    def __init__(
        self,
        base_url: str,
        credentials: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """`timeout` is per attempt; raise it above the source's query timeout."""
        self._http = Transport(base_url, credentials, timeout, transport)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def sources(self, *, end_user_token: str | None = None) -> dict[str, Any]:
        """`unavailable`: IDs of sources that could not be opened just now."""
        body = await self._http.request(
            "GET", "/v1/sources", headers=_headers(end_user_token)
        )
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("items"), list)
            or not isinstance(body.get("unavailable"), list)
        ):
            raise _invalid()
        return body

    async def query(
        self,
        source_id: str,
        statement: str,
        *,
        parameters: list[Any] | dict[str, Any] | None = None,
        max_rows: int | None = None,
        timeout_ms: int | None = None,
        source_token: str | None = None,
        end_user_token: str | None = None,
    ) -> dict[str, Any]:
        """`source_token`: a token issued for the source itself, for delegated sources only."""
        payload: dict[str, Any] = {"statement": statement}
        if parameters is not None:
            payload["parameters"] = parameters
        if max_rows is not None:
            payload["maxRows"] = max_rows
        if timeout_ms is not None:
            payload["timeoutMs"] = timeout_ms
        headers = _headers(end_user_token)
        if source_token:
            headers["bae6-source-token"] = source_token
        body = await self._http.request(
            "POST", f"{_source(source_id)}/query", body=payload, headers=headers
        )
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("rows"), list)
            or not isinstance(body.get("columns"), list)
        ):
            raise _invalid()
        return body

    async def list_objects(
        self,
        source_id: str,
        *,
        prefix: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        end_user_token: str | None = None,
    ) -> dict[str, Any]:
        params = {
            k: str(v)
            for k, v in (("prefix", prefix), ("cursor", cursor), ("limit", limit))
            if v is not None
        }
        body = await self._http.request(
            "GET",
            f"{_source(source_id)}/objects",
            params=params,
            headers=_headers(end_user_token),
        )
        if not isinstance(body, dict) or not isinstance(body.get("items"), list):
            raise _invalid()
        return body

    async def stat_object(
        self,
        source_id: str,
        key: str,
        *,
        version_id: str | None = None,
        end_user_token: str | None = None,
    ) -> dict[str, Any]:
        body = await self._http.request(
            "GET",
            f"{_source(source_id)}/objects/metadata",
            params=_target(key, version_id),
            headers=_headers(end_user_token),
        )
        if not isinstance(body, dict) or not isinstance(body.get("key"), str):
            raise _invalid()
        return body

    async def read_object(
        self,
        source_id: str,
        key: str,
        *,
        version_id: str | None = None,
        range: tuple[int, int] | None = None,
        end_user_token: str | None = None,
    ) -> ObjectContent:
        """The object's bytes, or the inclusive `range` (start, end) of them."""
        headers = _headers(end_user_token)
        if range is not None:
            headers["range"] = f"bytes={range[0]}-{range[1]}"
        response = await self._http.raw(
            "GET",
            f"{_source(source_id)}/objects/content",
            params=_target(key, version_id),
            headers=headers,
        )
        etag = response.headers.get("etag")
        modified = response.headers.get("last-modified")
        total = re.search(r"/(\d+)$", response.headers.get("content-range", ""))
        if (
            not etag
            or not modified
            or (response.status_code == 206) != (total is not None)
        ):
            raise _invalid()
        try:
            last_modified = parsedate_to_datetime(modified).isoformat()
        except (TypeError, ValueError):
            raise _invalid() from None
        return ObjectContent(
            body=response.content,
            size=int(total.group(1)) if total else len(response.content),
            etag=etag.strip('"'),
            last_modified=last_modified,
            version_id=response.headers.get("bae6-version-id"),
        )


SYNC_STATES = {"queued", "running", "succeeded", "failed"}


def _segment(identifier: str) -> str:
    # httpx resolves "." and ".." segments, which would send the request elsewhere.
    if identifier in (".", ".."):
        raise ContextError("not a valid identifier")
    return quote(identifier, safe="")


def _collection(collection_id: str) -> str:
    return f"/v1/collections/{_segment(collection_id)}"


class EmbedClient:
    """The bae6-context embed API: tenant-scoped sync and search over configured collections.

    Acts as the workload token's subject; there is no end-user delegation.
    Use `async with EmbedClient(...) as embed` so connections are closed.
    """

    def __init__(
        self,
        base_url: str,
        credentials: str,
        timeout: float = 10.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._http = Transport(base_url, credentials, timeout, transport)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    async def create_collection(self, collection_id: str) -> dict[str, Any]:
        """Provisions the configured collection's storage; a no-op when it exists."""
        body = await self._http.request("PUT", _collection(collection_id))
        dimensions = body.get("dimensions") if isinstance(body, dict) else None
        if (
            not isinstance(body, dict)
            or not isinstance(body.get("id"), str)
            or not isinstance(body.get("model"), str)
            or not isinstance(dimensions, int)
            or isinstance(dimensions, bool)
        ):
            raise _invalid()
        return body

    async def start_sync(
        self, collection_id: str, *, prefix: str | None = None
    ) -> dict[str, Any]:
        """Not retried: each call queues a sync. `prefix` also matches keys that extend it."""
        body = await self._http.request(
            "POST",
            f"{_collection(collection_id)}/syncs",
            body={} if prefix is None else {"prefix": prefix},
        )
        return _sync(body)

    async def get_sync(self, collection_id: str, sync_id: str) -> dict[str, Any]:
        body = await self._http.request(
            "GET", f"{_collection(collection_id)}/syncs/{_segment(sync_id)}"
        )
        return _sync(body)

    async def search(
        self, collection_id: str, query: str, *, limit: int | None = None
    ) -> list[dict[str, Any]]:
        """Ranked for the workload token's subject: only chunks that subject may read."""
        payload: dict[str, Any] = {"query": query}
        if limit is not None:
            payload["limit"] = limit
        body = await self._http.request(
            "POST", f"{_collection(collection_id)}/search", body=payload
        )
        if not isinstance(body, dict) or not isinstance(body.get("hits"), list):
            raise _invalid()
        if not all(
            isinstance(h, dict) and isinstance(h.get("chunkId"), str)
            for h in body["hits"]
        ):
            raise _invalid()
        return body["hits"]

    async def delete_document(self, collection_id: str, key: str) -> None:
        """Removes the key from the index only; the source object is untouched."""
        await self._http.raw(
            "DELETE", f"{_collection(collection_id)}/documents", params={"key": key}
        )


def _sync(body: object) -> dict[str, Any]:
    if (
        not isinstance(body, dict)
        or not isinstance(body.get("id"), str)
        or body.get("state") not in SYNC_STATES
    ):
        raise _invalid()
    return body


def _headers(end_user_token: str | None) -> dict[str, str]:
    return {"bae6-end-user-token": end_user_token} if end_user_token else {}


def _target(key: str, version_id: str | None) -> dict[str, str]:
    return {"key": key} if version_id is None else {"key": key, "versionId": version_id}
