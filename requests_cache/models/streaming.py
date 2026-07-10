"""Deferred cache writes for streaming responses."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterator, cast

from requests import Response

from .raw_response import _reset_fp
from .response import OriginalResponse

if TYPE_CHECKING:
    from ..policy.actions import CacheActions
    from ..session import CacheMixin


class DeferredCacheResponse(OriginalResponse):
    """Return a live streamed response immediately and write to the cache after the body is read."""

    _deferred_session: CacheMixin | None = None
    _deferred_actions: CacheActions | None = None
    _cache_written: bool = False
    _writing_cache: bool = False
    _accumulated_body: bytearray

    @classmethod
    def wrap(
        cls,
        session: CacheMixin,
        response: Response,
        actions: CacheActions,
    ) -> DeferredCacheResponse:
        OriginalResponse.wrap_response(response, actions)
        response.__class__ = cls
        deferred = cast(DeferredCacheResponse, response)
        deferred._deferred_session = session
        deferred._deferred_actions = actions
        deferred._cache_written = False
        deferred._writing_cache = False
        deferred._accumulated_body = bytearray()
        return deferred

    def _record_chunk(self, chunk: bytes | str) -> None:
        if not chunk:
            return
        if isinstance(chunk, bytes):
            self._accumulated_body.extend(chunk)
        else:
            encoding = self.encoding if isinstance(self.encoding, str) else 'utf-8'
            self._accumulated_body.extend(chunk.encode(encoding))

    def _is_content_consumed(self) -> bool:
        return bool(getattr(self, '_content_consumed', False))

    def _body_is_pending(self) -> bool:
        return cast(Any, self._content) is False

    def _finalize_body(self) -> None:
        """Make the full response body available for cache serialization."""
        if not self._body_is_pending():
            return
        if not self._is_content_consumed():
            return
        if not self._accumulated_body:
            return
        body = bytes(self._accumulated_body)
        self._content = body
        if self.raw is not None:
            _reset_fp(self.raw, body)

    def _maybe_write_cache(self) -> None:
        if (
            self._cache_written
            or self._writing_cache
            or self._deferred_session is None
            or self._deferred_actions is None
        ):
            return
        if not self._is_content_consumed():
            return
        self._finalize_body()
        if self._body_is_pending():
            return
        self._writing_cache = True
        try:
            self._deferred_session.cache.save_response(
                self,
                self._deferred_actions.cache_key,
                self._deferred_actions.expires,
            )
            self._cache_written = True
        finally:
            self._writing_cache = False

    def iter_content(
        self,
        chunk_size: int | None = 1,
        decode_unicode: bool = False,
    ) -> Iterator[Any]:
        for chunk in Response.iter_content(
            self,
            chunk_size=chunk_size,
            decode_unicode=decode_unicode,
        ):
            self._record_chunk(chunk)
            yield chunk
        self._finalize_body()
        self._maybe_write_cache()

    def iter_lines(
        self,
        chunk_size: int | None = 512,
        decode_unicode: bool = False,
        delimiter: str | bytes | None = None,
    ) -> Iterator[Any]:
        for line in Response.iter_lines(
            self,
            chunk_size=chunk_size,
            decode_unicode=decode_unicode,
            delimiter=delimiter,
        ):
            yield line
        self._finalize_body()
        self._maybe_write_cache()

    @property
    def content(self) -> bytes:
        if self._writing_cache:
            return cast(bytes, self._content)
        if self._body_is_pending():
            data = cast(bytes, Response.content.fget(self))  # type: ignore[attr-defined]
            if data:
                self._accumulated_body.extend(data)
            self._finalize_body()
            self._maybe_write_cache()
        else:
            self._maybe_write_cache()
        return cast(bytes, self._content)

    def close(self) -> None:
        try:
            self._finalize_body()
            self._maybe_write_cache()
        finally:
            Response.close(self)
