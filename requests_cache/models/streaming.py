"""Deferred cache writes for streaming responses."""

from __future__ import annotations

from typing import TYPE_CHECKING, Iterator, Optional

from requests import Response

from .raw_response import _reset_fp
from .response import OriginalResponse

if TYPE_CHECKING:
    from ..policy.actions import CacheActions
    from ..session import CachedSession


class DeferredCacheResponse(OriginalResponse):
    """Return a live streamed response immediately and write to the cache after the body is read."""

    _deferred_session: Optional['CachedSession'] = None
    _deferred_actions: Optional['CacheActions'] = None
    _cache_written: bool = False
    _writing_cache: bool = False
    _accumulated_body: bytearray

    @classmethod
    def wrap(
        cls,
        session: 'CachedSession',
        response: Response,
        actions: 'CacheActions',
    ) -> 'DeferredCacheResponse':
        wrapped = OriginalResponse.wrap_response(response, actions)
        wrapped.__class__ = cls
        wrapped._deferred_session = session
        wrapped._deferred_actions = actions
        wrapped._cache_written = False
        wrapped._writing_cache = False
        wrapped._accumulated_body = bytearray()
        return wrapped  # type: ignore[return-value]

    def _record_chunk(self, chunk: bytes | str) -> None:
        if not chunk:
            return
        if isinstance(chunk, bytes):
            self._accumulated_body.extend(chunk)
        else:
            self._accumulated_body.extend(chunk.encode(self.encoding or 'utf-8'))

    def _finalize_body(self) -> None:
        """Make the full response body available for cache serialization."""
        if self._content is not False or not self._content_consumed:
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
        if not self._content_consumed:
            return
        self._finalize_body()
        if self._content is False:
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
        chunk_size: int = 1,
        decode_unicode: bool = False,
    ) -> Iterator[bytes | str]:
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
        chunk_size: int = 512,
        decode_unicode: bool = False,
        delimiter: Optional[bytes] = None,
    ) -> Iterator[bytes | str]:
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
            return self._content  # type: ignore[return-value]
        if self._content is False:
            data = Response.content.fget(self)  # type: ignore[attr-defined]
            if self._content is False and data:
                self._accumulated_body.extend(data)
            self._finalize_body()
            self._maybe_write_cache()
            return self._content  # type: ignore[return-value]
        self._maybe_write_cache()
        return self._content  # type: ignore[return-value]

    def close(self) -> None:
        try:
            self._finalize_body()
            self._maybe_write_cache()
        finally:
            Response.close(self)
