from io import BytesIO

from urllib3.response import HTTPResponse

from tests.conftest import MOCKED_URL


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
