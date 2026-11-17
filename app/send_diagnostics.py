"""Bounded send-failure diagnostics: never log URLs, headers or payloads."""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

LOGGER = logging.getLogger('douyin_sender')
_CODE_KEYS = ('status_code', 'statusCode', 'error_code', 'errorCode', 'code', 'StatusCode')
_TEXT_KEYS = ('status_msg', 'statusMessage', 'error_msg', 'message', 'msg', 'StatusMessage')
_CATEGORIES = {
    'authentication': ('未登录', '登录失效', 'login required', 'unauthorized', 'not logged'),
    'verification': ('安全验证', '验证身份', 'captcha', 'verify'),
    'rate_limit': ('操作频繁', '发送频繁', 'too many', 'rate limit'),
    'permission': ('无权限', '禁止', 'forbidden', 'permission denied'),
    'network': ('网络异常', '网络错误', 'network error'),
    'unsupported': ('不支持', 'unsupported'),
}


def _is_send_response(url: str) -> bool:
    try:
        parsed = urlsplit(url)
        host = (parsed.hostname or '').lower()
        trusted = any(host == suffix or host.endswith('.' + suffix)
                      for suffix in ('douyin.com', 'byteim.com', 'snssdk.com', 'amemv.com'))
        path = parsed.path.lower()
        return trusted and 'message' in path and 'send' in path
    except ValueError:
        return False


def _safe_response_summary(status: int, payload: object = None) -> dict:
    result = {'http_status': status, 'server_codes': [], 'categories': []}
    if not isinstance(payload, dict):
        return result
    # No recursive traversal: message content, user IDs and auth fields are ignored.
    containers = [payload]
    for key in ('error', 'base_resp', 'BaseResp'):
        if isinstance(payload.get(key), dict):
            containers.append(payload[key])
    for item in containers:
        for key in _CODE_KEYS:
            code = item.get(key)
            if isinstance(code, int) and not isinstance(code, bool) and abs(code) <= 2**31:
                if code not in result['server_codes']:
                    result['server_codes'].append(code)
        for key in _TEXT_KEYS:
            text = item.get(key)
            if isinstance(text, str):
                text = text[:1024].lower()
                for category, words in _CATEGORIES.items():
                    if any(word in text for word in words) and category not in result['categories']:
                        result['categories'].append(category)
    return result


@asynccontextmanager
async def capture_send_diagnostics(page):
    summaries = []
    pending = set()
    observed = 0

    async def inspect(response):
        summary = _safe_response_summary(response.status)
        try:
            # Only this exact response body is inspected; no request, Cookie,
            # Set-Cookie, storage state or raw response data is read or logged.
            content_type = await response.header_value('content-type') or ''
            length = await response.header_value('content-length')
            if 'json' in content_type and length and length.isdecimal() and int(length) <= 65536:
                payload = await asyncio.wait_for(response.json(), timeout=1)
                summary = _safe_response_summary(response.status, payload)
        except Exception:
            pass
        summaries.append(summary)

    def on_response(response):
        nonlocal observed
        if observed >= 8 or not _is_send_response(response.url):
            return
        observed += 1
        task = asyncio.create_task(inspect(response))
        pending.add(task)
        task.add_done_callback(pending.discard)

    page.on('response', on_response)
    failed = False
    try:
        yield
    except Exception:
        failed = True
        raise
    finally:
        page.remove_listener('response', on_response)
        if pending:
            done, unfinished = await asyncio.wait(tuple(pending), timeout=1.5)
            for task in unfinished:
                task.cancel()
            if unfinished:
                await asyncio.gather(*unfinished, return_exceptions=True)
        if failed:
            LOGGER.error('发送接口安全诊断: %s', json.dumps(
                {'observed_send_responses': observed, 'responses': summaries,
                 'note': 'no_observable_http_send_response' if not observed else 'status_only'},
                ensure_ascii=False))
