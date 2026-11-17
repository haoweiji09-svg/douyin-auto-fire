import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.send_diagnostics import _is_send_response, _safe_response_summary, capture_send_diagnostics


def test_only_send_paths_on_known_hosts_are_observed():
    assert _is_send_response('https://imapi.amemv.com/v1/message/send?secret=hidden')
    assert not _is_send_response('https://douyin.com.evil.test/message/send')
    assert not _is_send_response('https://www.douyin.com/passport/login')
    assert not _is_send_response('https://www.douyin.com/message/list')


def test_summary_never_contains_raw_payload_or_credentials():
    payload = {'status_code': 403, 'status_msg': 'unauthorized SECRET_SESSION',
               'cookie': 'SECRET_COOKIE', 'url': '?token=SECRET_TOKEN',
               'data': {'code': 123456789, 'message': 'PRIVATE_CHAT'},
               'base_resp': {'code': 7, 'message': '操作频繁'}}
    summary = _safe_response_summary(200, payload)
    assert summary == {'http_status': 200, 'server_codes': [403, 7],
                       'categories': ['authentication', 'rate_limit']}
    text = json.dumps(summary)
    assert 'SECRET' not in text and 'PRIVATE_CHAT' not in text and '123456789' not in text


@pytest.mark.parametrize('payload', [None, [], 'private text', b'private binary'])
def test_non_object_bodies_only_return_http_status(payload):
    assert _safe_response_summary(500, payload) == {
        'http_status': 500, 'server_codes': [], 'categories': []}


class FakePage:
    def on(self, event, callback):
        self.callback = callback
    def remove_listener(self, event, callback):
        assert self.callback == callback
        self.callback = None


def response(content_type='application/json', length='100'):
    async def header(name):
        assert name in ('content-type', 'content-length')
        return content_type if name == 'content-type' else length
    return SimpleNamespace(url='https://imapi.amemv.com/v1/message/send?token=SECRET_URL',
                           status=403, header_value=header,
                           json=AsyncMock(return_value={'code': 7, 'message': 'forbidden SECRET_BODY'}))


@pytest.mark.asyncio
async def test_failure_logs_only_safe_summary_and_preserves_exception(caplog):
    page = FakePage()
    error = RuntimeError('send failed')
    with pytest.raises(RuntimeError) as caught:
        async with capture_send_diagnostics(page):
            page.callback(response())
            raise error
    assert caught.value is error
    assert 'permission' in caplog.text and '403' in caplog.text
    assert 'SECRET' not in caplog.text
    assert page.callback is None


@pytest.mark.asyncio
async def test_binary_response_is_not_decoded_and_success_is_not_logged(caplog):
    page = FakePage()
    reply = response(content_type='application/x-protobuf')
    async with capture_send_diagnostics(page):
        page.callback(reply)
    reply.json.assert_not_awaited()
    assert not caplog.text


@pytest.mark.asyncio
async def test_no_observable_request_does_not_claim_network_or_auth_cause(caplog):
    with pytest.raises(RuntimeError):
        async with capture_send_diagnostics(FakePage()):
            raise RuntimeError('send failed')
    assert 'no_observable_http_send_response' in caplog.text
    assert 'authentication' not in caplog.text


@pytest.mark.asyncio
async def test_response_observation_is_bounded(caplog):
    page = FakePage()
    replies = [response() for _ in range(20)]
    with pytest.raises(RuntimeError):
        async with capture_send_diagnostics(page):
            for reply in replies:
                page.callback(reply)
            raise RuntimeError('send failed')
    assert sum(reply.json.await_count for reply in replies) == 8
