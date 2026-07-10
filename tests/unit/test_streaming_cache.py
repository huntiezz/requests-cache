from io import BytesIO

from urllib3.response import HTTPResponse

from requests_cache.models.streaming import DeferredCacheResponse
from tests.conftest import MOCKED_URL


def test_deferred_response_record_chunk_edge_cases():
    response = DeferredCacheResponse()
    response._accumulated_body = bytearray()

    response._record_chunk(b'')
    response._record_chunk('')
    response.encoding = None
    response._record_chunk('ab')

    assert bytes(response._accumulated_body) == b'ab'


def test_finalize_body_without_raw():
    response = DeferredCacheResponse()
    response._content = False
    response._content_consumed = True
    response._accumulated_body = bytearray(b'cached-body')
    response.raw = None

    response._finalize_body()

    assert response._content == b'cached-body'


def test_stream_iter_lines_writes_cache(mock_session):
    url = f'{MOCKED_URL}/stream-lines'
    body = b'line-one\nline-two\n'
    mock_session.mock_adapter.register_uri(
        'GET',
        url,
        status_code=200,
        raw=HTTPResponse(
            body=BytesIO(body),
            status=200,
            request_method='GET',
            decode_content=False,
            preload_content=False,
        ),
    )

    save_calls = []
    original_save = mock_session.cache.save_response

    def track_save(*args, **kwargs):
        save_calls.append(1)
        return original_save(*args, **kwargs)

    mock_session.cache.save_response = track_save

    response = mock_session.get(url, stream=True)
    assert save_calls == []
    lines = list(response.iter_lines())
    assert lines == [b'line-one', b'line-two']
    assert save_calls == [1]


def test_stream_decode_unicode_records_text_chunks(mock_session):
    url = f'{MOCKED_URL}/stream-unicode'
    mock_session.mock_adapter.register_uri('GET', url, text='hello')

    save_calls = []
    original_save = mock_session.cache.save_response

    def track_save(*args, **kwargs):
        save_calls.append(1)
        return original_save(*args, **kwargs)

    mock_session.cache.save_response = track_save

    response = mock_session.get(url, stream=True)
    chunks = list(response.iter_content(decode_unicode=True, chunk_size=1024))
    assert chunks == ['hello']
    assert save_calls == [1]


def test_stream_empty_body_not_cached(mock_session):
    url = f'{MOCKED_URL}/stream-empty'
    mock_session.mock_adapter.register_uri(
        'GET',
        url,
        status_code=200,
        raw=HTTPResponse(
            body=BytesIO(b''),
            status=200,
            request_method='GET',
            decode_content=False,
            preload_content=False,
        ),
    )

    save_calls = []
    mock_session.cache.save_response = lambda *args, **kwargs: save_calls.append(1)

    response = mock_session.get(url, stream=True)
    assert list(response.iter_content()) == []
    assert save_calls == []

    retry = mock_session.get(url, stream=True)
    assert retry.from_cache is False


def test_stream_empty_content_property_is_cached(mock_session):
    url = f'{MOCKED_URL}/stream-empty-content'
    mock_session.mock_adapter.register_uri(
        'GET',
        url,
        status_code=200,
        raw=HTTPResponse(
            body=BytesIO(b''),
            status=200,
            request_method='GET',
            decode_content=False,
            preload_content=False,
        ),
    )

    save_calls = []
    original_save = mock_session.cache.save_response

    def track_save(*args, **kwargs):
        save_calls.append(1)
        return original_save(*args, **kwargs)

    mock_session.cache.save_response = track_save

    response = mock_session.get(url, stream=True)
    assert response.content == b''
    assert save_calls == [1]

    cached = mock_session.get(url, stream=True)
    assert cached.from_cache is True
    assert cached.content == b''


def test_stream_content_can_be_read_twice(mock_session):
    url = f'{MOCKED_URL}/stream-content-twice'
    mock_session.mock_adapter.register_uri('GET', url, text='repeat-me')

    save_calls = []
    original_save = mock_session.cache.save_response

    def track_save(*args, **kwargs):
        save_calls.append(1)
        return original_save(*args, **kwargs)

    mock_session.cache.save_response = track_save

    response = mock_session.get(url, stream=True)
    assert response.content == b'repeat-me'
    assert response.content == b'repeat-me'
    assert save_calls == [1]


def test_stream_defers_cache_write_until_body_read(mock_session):
    """Cache write should not run until a streaming response body is consumed."""
    url = f'{MOCKED_URL}/stream-deferred'
    body = b'alpha-beta-gamma'
    mock_session.mock_adapter.register_uri(
        'GET',
        url,
        status_code=200,
        raw=HTTPResponse(
            body=BytesIO(body),
            status=200,
            request_method='GET',
            decode_content=False,
            preload_content=False,
        ),
    )

    save_calls = []
    original_save = mock_session.cache.save_response

    def track_save(*args, **kwargs):
        save_calls.append(1)
        return original_save(*args, **kwargs)

    mock_session.cache.save_response = track_save

    response = mock_session.get(url, stream=True)
    assert response.from_cache is False
    assert save_calls == []

    chunks = list(response.iter_content(chunk_size=4))
    assert b''.join(chunks) == body
    assert save_calls == [1]

    cached = mock_session.get(url, stream=True)
    assert cached.from_cache is True
    assert list(cached.iter_content(chunk_size=4)) == chunks


def test_stream_partial_read_skips_cache_write(mock_session):
    """If a streaming response is not fully read, it should not be cached."""
    url = f'{MOCKED_URL}/stream-partial'
    body = b'0123456789'
    mock_session.mock_adapter.register_uri(
        'GET',
        url,
        status_code=200,
        raw=HTTPResponse(
            body=BytesIO(body),
            status=200,
            request_method='GET',
            decode_content=False,
            preload_content=False,
        ),
    )

    save_calls = []
    mock_session.cache.save_response = lambda *args, **kwargs: save_calls.append(1)

    response = mock_session.get(url, stream=True)
    iterator = response.iter_content(chunk_size=4)
    next(iterator)
    response.close()

    assert save_calls == []

    retry = mock_session.get(url, stream=True)
    assert retry.from_cache is False


def test_stream_content_property_writes_cache(mock_session):
    url = f'{MOCKED_URL}/stream-content'
    mock_session.mock_adapter.register_uri('GET', url, text='hello stream')

    save_calls = []
    original_save = mock_session.cache.save_response

    def track_save(*args, **kwargs):
        save_calls.append(1)
        return original_save(*args, **kwargs)

    mock_session.cache.save_response = track_save

    response = mock_session.get(url, stream=True)
    assert save_calls == []
    assert response.content == b'hello stream'
    assert save_calls == [1]
