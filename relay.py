#!/usr/bin/env python3
"""Zed's Anthropic-compatible provider on a Claude subscription, through the stock Claude Code CLI.

Zed sends ordinary Messages API requests to this loopback server. Each one is handed to a fresh
`claude -p` process (stream-json in and out, no built-in tools, a single turn) that authenticates
with the user's own Claude Code login and builds the upstream request itself. The CLI's
ANTHROPIC_BASE_URL points at a per-request admission proxy that forwards exactly one request to
api.anthropic.com, streams the answer back to the CLI and to Zed, and refuses any further request.
Credentials are never read, copied or logged; only the CLI's own HTTP request passes through.

The admission proxy, the tool-result restoration and the cache-breakpoint pinning follow the
Hermes Agent plugin claude-subscription-directsdk (MIT, Nous Research and contributors).
"""
import argparse
import atexit
import codecs
import copy
import datetime
import http.client
import ipaddress
import json
import logging
import os
import queue
import re
import secrets
import select
import shutil
import signal
import socket
import ssl
import stat
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit

log = logging.getLogger('zed-claude-relay')

PREFIX = 'mcp__zed__'
DEFAULT_PORT = 7865
DEFAULT_UPSTREAM = 'https://api.anthropic.com'
INERT_TOOLS = str(Path(__file__).with_name('inert_tools.py'))

CONTEXT_WINDOWS = {
    'claude-fable-5-1': 1_000_000,
    'claude-opus-5-5': 1_000_000,
    'claude-opus-5': 1_000_000,
    'claude-opus-4-8': 1_000_000,
    'claude-sonnet-5-5': 1_000_000,
    'claude-sonnet-5': 1_000_000,
    'claude-haiku-4-5-20251001': 200_000,
}
MAX_OUTPUT = {'claude-haiku-4-5-20251001': 64_000}
DISPLAY_NAMES = {
    'claude-fable-5-1': 'Claude Fable 5.1',
    'claude-opus-5-5': 'Claude Opus 5.5',
    'claude-opus-5': 'Claude Opus 5',
    'claude-opus-4-8': 'Claude Opus 4.8',
    'claude-sonnet-5-5': 'Claude Sonnet 5.5',
    'claude-sonnet-5': 'Claude Sonnet 5',
    'claude-haiku-4-5-20251001': 'Claude Haiku 4.5',
}
ALIASES = {
    'fable': 'claude-fable-5-1',
    'opus': 'claude-opus-5-5',
    'sonnet': 'claude-sonnet-5-5',
    'haiku': 'claude-haiku-4-5-20251001',
    'claude-haiku-4-5': 'claude-haiku-4-5-20251001',
}
ADAPTIVE_THINKING = {m for m, w in CONTEXT_WINDOWS.items() if w == 1_000_000}

# The CLI's own codes for errors it answers without any upstream request.
NATIVE_ERROR_STATUS = {
    'rate_limit': 429,
    'billing_error': 402,
    'authentication_failed': 401,
    'overloaded': 529,
    'server_error': 503,
}
ERROR_TYPES = {400: 'invalid_request_error', 401: 'authentication_error', 402: 'billing_error',
               403: 'permission_error', 429: 'rate_limit_error', 529: 'overloaded_error'}
AUTH_CONFLICTS = ('ANTHROPIC_API_KEY', 'ANTHROPIC_AUTH_TOKEN', 'ANTHROPIC_BASE_URL', 'ANTHROPIC_FOUNDRY_API_KEY',
                  'CLAUDE_CODE_USE_BEDROCK', 'CLAUDE_CODE_USE_VERTEX', 'CLAUDE_CODE_USE_FOUNDRY',
                  'CLAUDE_CODE_EXTRA_BODY', 'CLAUDE_CODE_EFFORT_LEVEL', 'CLAUDE_CODE_MAX_OUTPUT_TOKENS')
CLI_ENV = {
    'ENABLE_TOOL_SEARCH': 'false',
    'CLAUDE_CODE_MAX_RETRIES': '0',
    'DISABLE_AUTO_COMPACT': '1',
    'DISABLE_COMPACT': '1',
    'CLAUDE_CODE_TOTAL_TOKENS_REMINDER': 'off',
    'DISABLE_AUTOUPDATER': '1',
    'DISABLE_FEEDBACK_COMMAND': '1',
}
QUIET_ENV = {'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC': '1', 'DISABLE_TELEMETRY': '1', 'DISABLE_ERROR_REPORTING': '1'}
HOME_PREFIXES = ('.local/bin', '.claude/local', 'bin', '.npm-global/bin', '.bun/bin', '.volta/bin')
SYSTEM_PREFIXES = ('/opt/homebrew/bin', '/usr/local/bin')
HOP_BY_HOP = {'connection', 'transfer-encoding', 'content-length', 'keep-alive', 'proxy-authorization',
              'proxy-connection', 'accept-encoding', 'host', 'te', 'upgrade', 'server', 'date'}
UNCACHEABLE = ('thinking', 'redacted_thinking')
TOOL_NAME = re.compile(r'[A-Za-z0-9_-]{1,64}')


class BadRequest(Exception):
    status = 400


class RelayError(Exception):
    def __init__(self, message, status=502):
        super().__init__(message)
        self.status = status


# --- model catalog -------------------------------------------------------------------------------

def canonical_model(model):
    base = model[:-4] if model.endswith('[1m]') else model
    return ALIASES.get(base, base)


def cli_model(model):
    canonical = canonical_model(model)
    window = CONTEXT_WINDOWS.get(canonical)
    if window == 1_000_000:
        return canonical + '[1m]'
    if window == 200_000 and model.endswith('[1m]'):
        raise BadRequest(f'{canonical} has no 1M context window')
    return canonical if window else model


def supported(flag=True):
    return {'supported': flag}


def model_listing():
    created = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc).isoformat()
    data = []
    for model, window in CONTEXT_WINDOWS.items():
        adaptive = model in ADAPTIVE_THINKING
        data.append({
            'id': model, 'type': 'model', 'display_name': DISPLAY_NAMES[model], 'created_at': created,
            'max_input_tokens': window, 'max_tokens': MAX_OUTPUT.get(model, 128_000),
            'capabilities': {
                'thinking': {'supported': True, 'types': {'adaptive': supported(adaptive), 'enabled': supported(not adaptive)}},
                'image_input': supported(),
                'effort': {'supported': adaptive, **{level: supported(adaptive) for level in ('low', 'medium', 'high', 'xhigh', 'max')}},
            },
        })
    return {'data': data, 'has_more': False, 'first_id': data[0]['id'], 'last_id': data[-1]['id']}


# --- request translation (Zed's Messages request -> what the CLI is given) -----------------------

TOP_LEVEL = {'model', 'max_tokens', 'messages', 'system', 'tools', 'tool_choice', 'thinking', 'output_config',
             'stop_sequences', 'stream', 'temperature', 'top_p', 'top_k', 'cache_control'}
THINKING_FIELDS = ('type', 'budget_tokens', 'display')
TOOL_FIELDS = ('description', 'input_schema', 'eager_input_streaming', 'strict')


class Prepared:
    def __init__(self):
        self.model = self.native_model = self.system = ''
        self.frames = []
        self.manifest = []
        self.extra = {}
        self.max_tokens = None
        self.effort = None
        self.stream = True


def plain(block):
    return {k: v for k, v in block.items() if k != 'cache_control'} if isinstance(block, dict) else block


def text_of(content, what):
    if content is None:
        return ''
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for block in content:
            if not isinstance(block, dict) or block.get('type') != 'text' or not isinstance(block.get('text'), str):
                raise BadRequest(f'{what} supports text blocks only')
            parts.append(block['text'])
        return '\n\n'.join(parts)
    raise BadRequest(f'{what} must be a string or a list of text blocks')


def user_blocks(content):
    if isinstance(content, str):
        return [{'type': 'text', 'text': content}] if content else []
    if not isinstance(content, list):
        raise BadRequest('user content must be a string or a list of blocks')
    blocks = []
    for block in content:
        kind = block.get('type') if isinstance(block, dict) else None
        if kind in ('text', 'image', 'document'):
            blocks.append(plain(block))
        elif kind == 'tool_result':
            result = plain(block)
            if isinstance(result.get('content'), list):
                result['content'] = [plain(b) for b in result['content']]
            blocks.append(result)
        else:
            raise BadRequest(f'unsupported user content block: {kind}')
    return blocks


def assistant_blocks(content, native_names):
    if isinstance(content, str):
        return [{'type': 'text', 'text': content}] if content else []
    if not isinstance(content, list):
        raise BadRequest('assistant content must be a string or a list of blocks')
    blocks = []
    for block in content:
        kind = block.get('type') if isinstance(block, dict) else None
        if kind in ('text', 'thinking', 'redacted_thinking'):
            blocks.append(plain(block))
        elif kind == 'tool_use':
            call = plain(block)
            call['name'] = native_names.get(call.get('id'), PREFIX + str(call.get('name', '')))
            blocks.append(call)
        else:
            raise BadRequest(f'unsupported assistant content block: {kind}')
    return blocks


def frames_of(messages, native_names):
    if not isinstance(messages, list) or not messages:
        raise BadRequest('messages must be a nonempty list')
    frames = []
    for message in messages:
        role = message.get('role') if isinstance(message, dict) else None
        if role == 'user':
            blocks = user_blocks(message.get('content'))
        elif role == 'assistant':
            blocks = assistant_blocks(message.get('content'), native_names)
        else:
            raise BadRequest(f'unsupported message role: {role}')
        if not blocks:
            continue
        if frames and frames[-1]['role'] == role:
            frames[-1]['content'].extend(blocks)
        else:
            frames.append({'role': role, 'content': blocks})
    if not frames or frames[-1]['role'] != 'user':
        raise BadRequest('messages must end with a user turn; assistant prefill is unsupported')
    return frames


def tools_of(tools):
    manifest, wire, names = [], [], set()
    allowed = {'name', 'type', 'cache_control', *TOOL_FIELDS}
    for tool in tools or []:
        if not isinstance(tool, dict):
            raise BadRequest('tool definitions must be objects')
        if set(tool) - allowed:
            raise BadRequest('unsupported tool definition fields: ' + ', '.join(sorted(set(tool) - allowed)))
        if tool.get('type') not in (None, 'custom'):
            raise BadRequest(f'unsupported tool type: {tool.get("type")}')
        name = tool.get('name')
        if not isinstance(name, str) or not TOOL_NAME.fullmatch(name) or len(PREFIX + name) > 64 or name in names:
            raise BadRequest('tool names must be unique ASCII identifiers of at most 54 characters')
        schema = tool.get('input_schema', {'type': 'object'})
        if not isinstance(schema, dict) or not isinstance(tool.get('description', ''), str):
            raise BadRequest('tool input_schema must be an object and description a string')
        names.add(name)
        manifest.append({'name': name, 'description': tool.get('description', ''), 'inputSchema': schema})
        entry = {'name': PREFIX + name}
        entry.update({k: tool[k] for k in TOOL_FIELDS if k in tool})
        entry.setdefault('description', '')
        entry['input_schema'] = schema
        wire.append(entry)
    return manifest, wire


def translate(body, native_names):
    if not isinstance(body, dict):
        raise BadRequest('request body must be a JSON object')
    unknown = set(body) - TOP_LEVEL
    if unknown:
        raise BadRequest('unsupported request fields: ' + ', '.join(sorted(unknown)))
    prepared = Prepared()
    model = body.get('model')
    if not isinstance(model, str) or not model:
        raise BadRequest('model is required')
    prepared.model = canonical_model(model)
    prepared.native_model = cli_model(model)
    prepared.stream = bool(body.get('stream', False))
    prepared.system = text_of(body.get('system'), 'system')
    prepared.frames = frames_of(body.get('messages'), native_names)
    prepared.manifest, wire_tools = tools_of(body.get('tools'))
    extra = {'tools': wire_tools}
    thinking = body.get('thinking')
    if thinking is not None:
        if not isinstance(thinking, dict) or not isinstance(thinking.get('type'), str):
            raise BadRequest('thinking must be an object with a type')
        extra['thinking'] = {k: thinking[k] for k in THINKING_FIELDS if k in thinking}
        if thinking['type'] == 'disabled':
            extra['context_management'] = {'edits': []}
    if body.get('tool_choice') is not None:
        extra['tool_choice'] = body['tool_choice']
    output_config = body.get('output_config')
    if output_config is not None:
        if not isinstance(output_config, dict) or set(output_config) - {'effort'}:
            raise BadRequest('output_config supports effort only')
        effort = output_config.get('effort')
        if effort is not None:
            if effort not in ('low', 'medium', 'high', 'xhigh', 'max'):
                raise BadRequest(f'unsupported effort: {effort}')
            prepared.effort = effort
            extra['output_config'] = {'effort': effort}
    stops = body.get('stop_sequences')
    if stops:
        if not isinstance(stops, list) or not all(isinstance(s, str) and s for s in stops):
            raise BadRequest('stop_sequences must be a list of nonempty strings')
        extra['stop_sequences'] = stops
    max_tokens = body.get('max_tokens')
    if max_tokens is not None:
        if type(max_tokens) is not int or max_tokens < 1:
            raise BadRequest('max_tokens must be a positive integer')
        prepared.max_tokens = max_tokens
        extra['max_tokens'] = max_tokens
    prepared.extra = extra
    return prepared


def stream_json_frames(prepared):
    lines = []
    last = len(prepared.frames) - 1
    for index, frame in enumerate(prepared.frames):
        if frame['role'] == 'user':
            line = {'type': 'user', 'message': {'role': 'user', 'content': frame['content']}}
            if index < last:
                line['shouldQuery'] = False
        else:
            calls = any(b.get('type') == 'tool_use' for b in frame['content'])
            line = {'type': 'assistant', 'message': {
                'id': f'msg_zed_{index}', 'type': 'message', 'role': 'assistant', 'model': prepared.model,
                'content': frame['content'], 'stop_reason': 'tool_use' if calls else 'end_turn', 'stop_sequence': None,
                'usage': {'input_tokens': 0, 'output_tokens': 0}}}
        lines.append(line)
    return lines


# --- wire hygiene on the CLI's request (content never changes; markers and reminders only) -------

class QueriedTurnMismatch(ValueError):
    pass


def bare_text(block):
    return isinstance(block, dict) and set(block) == {'type', 'text'} and block['type'] == 'text' and isinstance(block['text'], str)


def restore_queried_turn(payload, queried):
    """Give the queried tool-result turn back the host's byte representation.

    The CLI appends its per-request reminders to the text of the newest tool_result; the host
    replays the turn next time without them. Only trailing bare text is removable, and only when
    the frame maps unambiguously (same ordered tool_use ids, same non-text blocks). Anything the
    function cannot prove lossless forwards exactly as the CLI built it."""
    if not isinstance(queried, list) or not any(isinstance(b, dict) and b.get('type') == 'tool_result' for b in queried):
        return payload

    def mismatch():
        raise QueriedTurnMismatch('the CLI request does not uniquely match the queried tool results')

    def restore_content(sent, host):
        if isinstance(host, str) or isinstance(sent, str):
            def text(value):
                if isinstance(value, str):
                    return value
                if isinstance(value, list) and all(bare_text(b) for b in value):
                    return ''.join(b['text'] for b in value)
                return None
            left, right = text(sent), text(host)
            if left is None or right is None or not left.startswith(right):
                mismatch()
            return copy.deepcopy(host)
        if isinstance(sent, list) and isinstance(host, list):
            return restore_sequence(sent, host)
        if sent != host:
            mismatch()
        return copy.deepcopy(host)

    def restore_block(sent, host):
        if not isinstance(sent, dict) or not isinstance(host, dict):
            mismatch()
        restored = copy.deepcopy(host)
        if host.get('type') == 'tool_result':
            left_error, right_error = sent.get('is_error', False), host.get('is_error', False)
            if type(left_error) is not bool or type(right_error) is not bool or left_error != right_error:
                mismatch()
            ignored = {'content', 'is_error', 'cache_control'}
            if {k: v for k, v in sent.items() if k not in ignored} != {k: v for k, v in host.items() if k not in ignored}:
                mismatch()
            if ('content' in sent) != ('content' in host):
                mismatch()
            if 'content' in host:
                restored['content'] = restore_content(sent['content'], host['content'])
        elif plain(sent) != plain(host):
            mismatch()
        if 'cache_control' in sent:
            if 'cache_control' in host and sent['cache_control'] != host['cache_control']:
                mismatch()
            restored['cache_control'] = copy.deepcopy(sent['cache_control'])
        return restored

    def restore_sequence(sent, host):
        restored = []
        i = j = 0
        while i < len(host):
            if j >= len(sent):
                mismatch()
            if bare_text(host[i]) and bare_text(sent[j]):
                host_end, sent_end = i + 1, j + 1
                while host_end < len(host) and bare_text(host[host_end]):
                    host_end += 1
                while sent_end < len(sent) and bare_text(sent[sent_end]):
                    sent_end += 1
                original = ''.join(b['text'] for b in host[i:host_end])
                actual = ''.join(b['text'] for b in sent[j:sent_end])
                if not (actual.startswith(original) if host_end == len(host) else actual == original):
                    mismatch()
                restored.extend(copy.deepcopy(host[i:host_end]))
                i, j = host_end, sent_end
            else:
                restored.append(restore_block(sent[j], host[i]))
                i += 1
                j += 1
        if any(not bare_text(b) for b in sent[j:]):
            mismatch()
        return restored

    try:
        body = json.loads(payload)
        messages = body['messages']
        last_assistant = max((i for i, m in enumerate(messages) if m.get('role') == 'assistant'), default=-1)
        newest = messages[last_assistant + 1] if last_assistant + 1 < len(messages) else {}
        native = newest.get('content') if newest.get('role') == 'user' else None
        if not isinstance(native, list):
            mismatch()
        result_ids = [b.get('tool_use_id') for b in queried if isinstance(b, dict) and b.get('type') == 'tool_result']
        if any(not isinstance(r, str) or not r for r in result_ids) or len(set(result_ids)) != len(result_ids):
            mismatch()
        native_ids = [b.get('tool_use_id') for b in native if isinstance(b, dict) and b.get('type') == 'tool_result']
        if result_ids != native_ids:
            mismatch()
        restored = restore_sequence(native, queried)
        if native == restored:
            return payload
        newest['content'] = restored
        return json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    except QueriedTurnMismatch:
        raise
    except (ValueError, TypeError, KeyError, AttributeError, IndexError):
        mismatch()


def pin_message_breakpoint(payload, queried):
    """Keep the message cache breakpoint on content the next request replays unchanged.

    The CLI marks its trailing per-request system message (today's date, a prompt nudge), which
    never recurs, so the written prefix is never read back and every round re-writes the history.
    What recurs is everything through the last assistant message plus the leading blocks of the
    newest turn that equal the queried frame. Marks after that span fold into one on its last
    cacheable block; marks inside it stay. The count never grows, system and tools marks are
    untouched, and the moved marker keeps the CLI's own TTL."""
    if not queried:
        return payload
    try:
        body = json.loads(payload)
        messages = body['messages']
        blocks = [(i, j, b) for i, m in enumerate(messages) if isinstance(m.get('content'), list)
                  for j, b in enumerate(m['content'])]
        marked = [(i, j, b) for i, j, b in blocks if isinstance(b, dict) and 'cache_control' in b]
        if not marked:
            return payload
        last = max((i for i, m in enumerate(messages) if m.get('role') == 'assistant'), default=-1)
        stable = [(i, j, b) for i, j, b in blocks if i <= last]
        newest = messages[last + 1] if last + 1 < len(messages) else {}
        if newest.get('role') == 'user' and isinstance(newest.get('content'), list):
            prefix = []
            for j, (sent, host) in enumerate(zip(newest['content'], queried)):
                if plain(sent) != plain(host):
                    break
                prefix.append((last + 1, j, sent))
            rest = newest['content'][len(prefix):]
            if not (rest and isinstance(rest[0], dict) and rest[0].get('type') == 'tool_result'):
                stable += prefix
        target = next(((i, j, b) for i, j, b in reversed(stable) if isinstance(b, dict) and b.get('type') not in UNCACHEABLE), None)
        after = [b for i, j, b in marked if target is not None and (target[0], target[1]) < (i, j)]
        if target is None or not after:
            return payload
        moved = [b.pop('cache_control') for b in after][0]
        target[2].setdefault('cache_control', moved)
        return json.dumps(body, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
    except (ValueError, TypeError, KeyError, AttributeError, IndexError):
        return payload


# --- the answer stream ---------------------------------------------------------------------------

FRAME_END = re.compile(rb'\r?\n\r?\n')


def sse_frames(buffer):
    """Split complete SSE frames off the front of ``buffer``; returns (frames, remainder)."""
    frames = []
    while True:
        match = FRAME_END.search(buffer)
        if not match:
            return frames, buffer
        frames.append(buffer[:match.end()])
        buffer = buffer[match.end():]


def frame_event(frame):
    data = b'\n'.join(line[5:].lstrip() for line in frame.split(b'\n') if line.startswith(b'data:'))
    if not data:
        return None
    try:
        return json.loads(data)
    except ValueError:
        return None


def unprefixed(name):
    return name[len(PREFIX):] if isinstance(name, str) and name.startswith(PREFIX) else name


class SseRewriter:
    """Hands Zed the upstream stream byte for byte, except that tool names lose the MCP prefix."""

    def __init__(self, on_tool_use=None):
        self.buffer = b''
        self.on_tool_use = on_tool_use

    def feed(self, chunk):
        frames, self.buffer = sse_frames(self.buffer + chunk)
        return b''.join(self.rewrite(frame) for frame in frames)

    def flush(self):
        rest, self.buffer = self.buffer, b''
        return rest

    def rewrite(self, frame):
        if b'tool_use' not in frame:
            return frame
        event = frame_event(frame)
        if not isinstance(event, dict) or event.get('type') != 'content_block_start':
            return frame
        block = event.get('content_block')
        if not isinstance(block, dict) or block.get('type') != 'tool_use':
            return frame
        if self.on_tool_use:
            self.on_tool_use(block.get('id'), block.get('name'))
        if not isinstance(block.get('name'), str) or not block['name'].startswith(PREFIX):
            return frame
        block['name'] = unprefixed(block['name'])
        return b'event: content_block_start\ndata: ' + json.dumps(event, ensure_ascii=False, separators=(',', ':')).encode('utf-8') + b'\n\n'


class Capture:
    """Reassembles the upstream message from its stream, for logging and non-streaming answers."""

    def __init__(self):
        self.message = None
        self.complete = False
        self.buffer = self.raw = b''
        self.arguments = {}

    def feed(self, chunk):
        if len(self.raw) < 1 << 20:
            self.raw += chunk
        frames, self.buffer = sse_frames(self.buffer + chunk)
        for frame in frames:
            event = frame_event(frame)
            if isinstance(event, dict):
                self.event(event)

    def event(self, event):
        kind = event.get('type')
        try:
            if kind == 'message_start':
                self.message = copy.deepcopy(event['message'])
            elif kind == 'content_block_start':
                self.message['content'].append(copy.deepcopy(event['content_block']))
            elif kind == 'content_block_delta':
                delta, block = event['delta'], self.message['content'][event['index']]
                field = {'text_delta': 'text', 'thinking_delta': 'thinking', 'signature_delta': 'signature'}.get(delta['type'])
                if field:
                    block[field] = block.get(field, '') + delta[field]
                elif delta['type'] == 'input_json_delta':
                    self.arguments[event['index']] = self.arguments.get(event['index'], '') + delta['partial_json']
            elif kind == 'content_block_stop':
                raw = self.arguments.pop(event['index'], None)
                if raw is not None:
                    self.message['content'][event['index']]['input'] = json.loads(raw) if raw.strip() else {}
            elif kind == 'message_delta':
                self.message.update(event.get('delta', {}))
                self.message.setdefault('usage', {}).update(event.get('usage', {}))
            elif kind == 'message_stop':
                self.complete = bool(self.message and self.message.get('stop_reason') and not self.arguments)
        except (KeyError, TypeError, ValueError, IndexError, AttributeError):
            pass

    def usage_line(self):
        usage = (self.message or {}).get('usage') or {}
        return 'in=%s cache_read=%s cache_write=%s out=%s stop=%s' % (
            usage.get('input_tokens'), usage.get('cache_read_input_tokens'), usage.get('cache_creation_input_tokens'),
            usage.get('output_tokens'), (self.message or {}).get('stop_reason'))


# --- admission: the one upstream request the CLI may make ----------------------------------------

CA_BUNDLES = ('/etc/ssl/cert.pem', '/etc/ssl/certs/ca-certificates.crt', '/etc/pki/tls/certs/ca-bundle.crt')


def tls_context():
    """The default context, or the system bundle when this Python ships without roots (python.org macOS builds)."""
    context = ssl.create_default_context()
    if context.cert_store_stats()['x509'] == 0:
        for bundle in [os.environ.get('SSL_CERT_FILE')] + list(CA_BUNDLES):
            if bundle and os.path.exists(bundle):
                context.load_verify_locations(bundle)
                break
        else:
            try:
                import certifi
                context.load_verify_locations(certifi.where())
            except ImportError:
                log.warning('no CA bundle found: HTTPS to the upstream will fail; set SSL_CERT_FILE')
    return context


class Admission:
    def __init__(self, upstream, queried, inbox, idle):
        self.upstream = urlsplit(upstream)
        host = self.upstream.hostname
        try:
            local = ipaddress.ip_address(host).is_loopback
        except ValueError:
            local = host == 'localhost'
        if not host or self.upstream.scheme != 'https' and not (self.upstream.scheme == 'http' and local):
            raise ValueError('the upstream must be HTTPS or a loopback HTTP fixture')
        self.queried = queried
        self.inbox = inbox
        self.idle = idle
        self.lock = threading.Lock()
        self.sockets = set()
        self.cancelled = self.used = False
        self.denied = 0
        self.status = self.request_id = self.failure = self.unrestored = None
        self.prefix = '/admit/' + secrets.token_urlsafe(24)
        self.server = HTTPServer(('127.0.0.1', 0), AdmissionHandler)
        self.server.admission = self
        self.url = f'http://127.0.0.1:{self.server.server_port}{self.prefix}'
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={'poll_interval': .05}, daemon=True)
        self.thread.start()

    def abort(self):
        with self.lock:
            self.cancelled = True
            for sock in self.sockets:
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def close(self):
        self.abort()
        self.server.shutdown()
        self.thread.join()
        self.server.server_close()


class AdmissionHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def send_json(self, status, body):
        payload = json.dumps(body).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self):
        gate = self.server.admission
        if urlsplit(self.path).path != gate.prefix + '/v1/messages' or self.headers.get('Origin'):
            self.send_error(404)
            return
        with gate.lock:
            if gate.cancelled or gate.used:
                gate.denied += 1
                self.send_json(400, {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'ZED_CLAUDE_RELAY_ADMISSION_CONSUMED'}})
                return
            gate.used = True
            gate.sockets.add(self.connection)
        conn = upstream_socket = None
        started = False
        try:
            self.connection.settimeout(gate.idle)
            payload = self.rfile.read(int(self.headers['Content-Length']))
            try:
                payload = restore_queried_turn(payload, gate.queried)
            except QueriedTurnMismatch as exc:
                gate.unrestored = str(exc)
            payload = pin_message_breakpoint(payload, gate.queried)
            target = gate.upstream
            if target.scheme == 'https':
                conn = http.client.HTTPSConnection(target.hostname, target.port, timeout=gate.idle, context=tls_context())
            else:
                conn = http.client.HTTPConnection(target.hostname, target.port, timeout=gate.idle)
            conn.connect()
            upstream_socket = conn.sock
            with gate.lock:
                if gate.cancelled:
                    return
                gate.sockets.add(upstream_socket)
            headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP_BY_HOP}
            headers['Accept-Encoding'] = 'identity'
            route = target.path.rstrip('/') + '/v1/messages' + (('?' + urlsplit(self.path).query) if urlsplit(self.path).query else '')
            conn.request('POST', route, payload, headers)
            del headers, payload
            response = conn.getresponse()
            gate.request_id = response.getheader('request-id') or response.getheader('x-request-id')
            gate.status = response.status
            forwarded = [(k, v) for k, v in response.getheaders() if k.lower() not in HOP_BY_HOP]
            gate.inbox.put(('up', 'start', (response.status, forwarded)))
            started = True
            self.send_response(response.status)
            for key, value in forwarded:
                self.send_header(key, value)
            self.send_header('Connection', 'close')
            self.end_headers()
            cli_alive = True
            while True:
                chunk = response.read1(65536)
                if not chunk:
                    break
                gate.inbox.put(('up', 'chunk', chunk))
                if cli_alive:
                    try:
                        self.wfile.write(chunk)
                        self.wfile.flush()
                    except OSError:
                        cli_alive = False
            gate.inbox.put(('up', 'end', None))
        except (OSError, http.client.HTTPException, ValueError, KeyError, TypeError) as exc:
            gate.failure = type(exc).__name__
            gate.inbox.put(('up', 'end' if started else 'failed', gate.failure))
        finally:
            with gate.lock:
                gate.sockets.discard(self.connection)
                gate.sockets.discard(upstream_socket)
            if conn:
                conn.close()
            self.close_connection = True


# --- one Claude Code process per request ---------------------------------------------------------

def resolve_claude(env):
    override = env.get('ZED_CLAUDE_RELAY_CLAUDE')
    if override:
        return override if os.access(override, os.X_OK) else None
    exe = shutil.which('claude', path=env.get('PATH') or os.defpath)
    if exe:
        return exe
    home = env.get('HOME')
    prefixes = [os.path.join(home, p) for p in HOME_PREFIXES] if home else []
    return shutil.which('claude', path=os.pathsep.join(prefixes + list(SYSTEM_PREFIXES)))


def private_dir(path):
    info = os.lstat(path)
    return stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077


_fallback_cwd = None


def stable_workdir():
    """One cwd for every request: the CLI writes it into its environment reminder, so a moving
    directory would move the cached prefix of every conversation."""
    global _fallback_cwd
    path = Path(tempfile.gettempdir()) / f'zed-claude-relay-cwd-{os.getuid()}'
    try:
        parent = path.parent.stat().st_mode
        if not (parent & 0o022 and not parent & stat.S_ISVTX):
            try:
                os.makedirs(path, mode=0o700)
            except FileExistsError:
                pass
            if private_dir(path):
                return str(path)
    except OSError:
        pass
    if _fallback_cwd is None:
        _fallback_cwd = tempfile.mkdtemp(prefix='zed-claude-relay-cwd-')
        atexit.register(shutil.rmtree, _fallback_cwd, ignore_errors=True)
    return _fallback_cwd


class Turn:
    def __init__(self, prepared, config, inbox):
        self.prepared, self.config, self.inbox = prepared, config, inbox
        self.process = None
        self.admission = None
        self.tmp = Path(tempfile.mkdtemp(prefix='zed-claude-relay-'))
        self.lock = threading.Lock()
        self.killed = False
        self.native_error = self.native_error_code = None
        self.results = []

    def start(self):
        root = self.tmp
        (root / 'tools.json').write_text(json.dumps(self.prepared.manifest), encoding='utf-8')
        (root / 'system.md').write_text(self.prepared.system, encoding='utf-8')
        extra = json.dumps(self.prepared.extra, separators=(',', ':'), allow_nan=False)
        (root / 'settings.json').write_text(json.dumps({'env': {'CLAUDE_CODE_EXTRA_BODY': extra}}), encoding='utf-8')
        mcp = {'mcpServers': {'zed': {'command': sys.executable, 'args': [INERT_TOOLS, str(root / 'tools.json')]}}}
        env = dict(self.config.env)
        self.admission = Admission(self.config.upstream, self.prepared.frames[-1]['content'], self.inbox, self.config.idle)
        env['ANTHROPIC_BASE_URL'] = self.admission.url
        if self.prepared.max_tokens:
            env['CLAUDE_CODE_MAX_OUTPUT_TOKENS'] = str(self.prepared.max_tokens)
        command = [self.config.claude, '-p', '--model', self.prepared.native_model,
                   '--input-format', 'stream-json', '--output-format', 'stream-json', '--verbose', '--include-partial-messages',
                   '--tools', '', '--system-prompt-file', str(root / 'system.md'), '--settings', str(root / 'settings.json'),
                   '--setting-sources', '', '--strict-mcp-config', '--disable-slash-commands', '--max-turns', '1',
                   '--permission-mode', 'dontAsk', '--no-session-persistence', '--mcp-config', json.dumps(mcp)]
        if self.prepared.effort:
            command += ['--effort', self.prepared.effort]
        self.stderr = open(root / 'stderr.txt', 'w')
        with self.lock:
            if self.killed:
                raise RelayError('request cancelled', 499)
            self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
                                            text=True, encoding='utf-8', cwd=self.config.workdir, env=env, start_new_session=True)
        threading.Thread(target=self.read_events, daemon=True).start()

    def read_events(self):
        process = self.process
        try:
            for line in process.stdout:
                try:
                    self.inbox.put(('cli', json.loads(line)))
                except ValueError:
                    self.inbox.put(('cli', {'type': 'relay_unparsed', 'line': line[:300]}))
        except (OSError, ValueError):
            pass
        finally:
            process.wait()
            self.inbox.put(('cli', None))

    def replay(self):
        """Feed the conversation; every earlier user turn is acknowledged with a zero-turn result."""
        lines = stream_json_frames(self.prepared)
        for line in lines:
            try:
                self.process.stdin.write(json.dumps(line, allow_nan=False) + '\n')
                self.process.stdin.flush()
            except OSError:
                raise RelayError(self.exit_message('Claude Code stopped reading the history'), 502)
            if line.get('shouldQuery') is False:
                deadline = time.monotonic() + self.config.replay_timeout
                while True:
                    try:
                        item = self.inbox.get(timeout=max(.01, deadline - time.monotonic()))
                    except queue.Empty:
                        raise RelayError('Claude Code did not acknowledge the replayed history in time', 504)
                    if item[0] != 'cli':
                        raise RelayError('unexpected upstream traffic during history replay')
                    event = item[1]
                    if event is None:
                        raise RelayError(self.exit_message('Claude Code exited while replaying the history'), 502)
                    self.note(event)
                    if event.get('type') == 'result':
                        if event.get('num_turns') != 0 or event.get('is_error'):
                            raise RelayError('Claude Code did not accept the replayed history: ' + str(event.get('result') or event.get('subtype')), 502)
                        break
        try:
            self.process.stdin.close()
        except OSError:
            pass

    def note(self, event):
        kind = event.get('type')
        if kind == 'assistant' and (event.get('error') or event.get('message', {}).get('error')):
            self.native_error_code = event.get('error') or event.get('message', {}).get('error')
            self.native_error = '\n'.join(b.get('text', '') for b in event.get('message', {}).get('content', []) if b.get('type') == 'text')
        elif kind == 'result':
            self.results.append(event)

    def exit_message(self, prefix):
        code = self.process.poll() if self.process else None
        tail = ''
        try:
            self.stderr.flush()
            tail = (self.tmp / 'stderr.txt').read_text(encoding='utf-8', errors='replace')[-400:].strip()
        except OSError:
            pass
        return f'{prefix} (exit {code})' + (f': {tail}' if tail else '')

    def native_failure(self):
        """The error to hand Zed when the CLI never made an upstream request."""
        if self.native_error_code or self.native_error:
            status = NATIVE_ERROR_STATUS.get(str(self.native_error_code), 502)
            hint = ' Run `claude auth login` as the user running the relay.' if status == 401 else ''
            return RelayError(f'Claude Code: {self.native_error or self.native_error_code}.{hint}', status)
        result = self.results[-1] if self.results else {}
        detail = result.get('result') or result.get('subtype') or 'no result'
        return RelayError(self.exit_message(f'Claude Code made no request: {detail}'), 502)

    def kill(self):
        with self.lock:
            self.killed = True
            process = self.process
        if process is not None and process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
        if self.admission is not None:
            self.admission.abort()

    def close(self):
        self.kill()
        if self.process is not None:
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            for pipe in (self.process.stdin, self.process.stdout):
                if pipe and not pipe.closed:
                    pipe.close()
        if self.admission is not None:
            self.admission.close()
        try:
            self.stderr.close()
        except (OSError, AttributeError):
            pass
        shutil.rmtree(self.tmp, ignore_errors=True)


# --- the server Zed talks to ---------------------------------------------------------------------

class Config:
    def __init__(self, claude, upstream, env, workdir, idle=300, replay_timeout=60):
        self.claude, self.upstream, self.env, self.workdir = claude, upstream, env, workdir
        self.idle, self.replay_timeout = idle, replay_timeout
        self.native_names = {}
        self.names_lock = threading.Lock()

    def remember_tool_use(self, call_id, name):
        if not isinstance(call_id, str) or not isinstance(name, str):
            return
        with self.names_lock:
            if len(self.native_names) > 4096:
                for key in list(self.native_names)[:1024]:
                    del self.native_names[key]
            self.native_names[call_id] = name

    def remembered_names(self):
        with self.names_lock:
            return dict(self.native_names)


def estimate_tokens(body):
    text = len(json.dumps(body.get('system', ''))) + len(json.dumps(body.get('tools', [])))
    images = 0
    for message in body.get('messages') or []:
        content = message.get('content') if isinstance(message, dict) else None
        if isinstance(content, str):
            text += len(content)
        elif isinstance(content, list):
            for block in content:
                if isinstance(block, dict) and block.get('type') in ('image', 'document'):
                    images += 1
                else:
                    text += len(json.dumps(block))
    return text // 4 + images * 1600


class RelayHandler(BaseHTTPRequestHandler):
    server_version = 'zed-claude-relay/1'

    def log_message(self, *args):
        pass

    def send_json(self, status, body):
        payload = json.dumps(body).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.send_header('Connection', 'close')
        self.end_headers()
        self.wfile.write(payload)

    def send_error_json(self, status, message):
        self.send_json(status, {'type': 'error', 'error': {'type': ERROR_TYPES.get(status, 'api_error'), 'message': message}})

    def admitted(self):
        if self.headers.get('Origin'):
            self.send_error_json(403, 'browser requests are refused')
            return False
        if not self.headers.get('X-Api-Key') and not self.headers.get('Authorization'):
            self.send_error_json(401, 'set any API key in Zed; the relay only checks that one is present')
            return False
        return True

    def read_json(self):
        length = self.headers.get('Content-Length')
        if length is None:
            raise BadRequest('Content-Length required')
        try:
            return json.loads(self.rfile.read(int(length)))
        except ValueError as exc:
            raise BadRequest(f'invalid JSON body: {exc}')

    def client_gone(self):
        try:
            readable, _, _ = select.select([self.connection], [], [], 0)
            return bool(readable) and self.connection.recv(1, socket.MSG_PEEK) == b''
        except OSError:
            return True

    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/':
            self.send_json(200, {'ok': True, 'service': 'zed-claude-relay'})
        elif path == '/v1/models':
            if self.admitted():
                self.send_json(200, model_listing())
        else:
            self.send_error_json(404, 'not found')

    def do_POST(self):
        path = urlsplit(self.path).path
        if not self.admitted():
            return
        try:
            if path == '/v1/messages/count_tokens':
                self.send_json(200, {'input_tokens': estimate_tokens(self.read_json())})
            elif path == '/v1/messages':
                self.messages(self.read_json())
            else:
                self.send_error_json(404, 'not found')
        except BadRequest as exc:
            self.send_error_json(400, str(exc))

    def messages(self, body):
        config = self.server.config
        prepared = translate(body, config.remembered_names())
        inbox = queue.Queue()
        turn = Turn(prepared, config, inbox)
        started = time.monotonic()
        label = f'{prepared.native_model} turns={len(prepared.frames)} tools={len(prepared.manifest)}'
        try:
            turn.start()
            turn.replay()
            self.pump(turn, inbox, prepared, label, started)
        except RelayError as exc:
            log.info('%s -> %s %s', label, exc.status, exc)
            try:
                self.send_error_json(exc.status if exc.status != 499 else 400, str(exc))
            except OSError:
                pass
        except Exception as exc:
            log.exception('%s -> relay failure', label)
            try:
                self.send_error_json(500, f'relay failure: {type(exc).__name__}: {exc}')
            except OSError:
                pass
        finally:
            turn.close()

    def pump(self, turn, inbox, prepared, label, started):
        config = self.server.config
        capture = Capture()
        rewriter = SseRewriter(config.remember_tool_use)
        answered = False
        last_activity = time.monotonic()
        while True:
            if self.client_gone():
                log.info('%s -> cancelled by Zed', label)
                return
            try:
                item = inbox.get(timeout=.25)
            except queue.Empty:
                if time.monotonic() - last_activity > config.idle:
                    if answered:
                        log.info('%s -> abandoned after %gs without upstream bytes', label, config.idle)
                        return
                    raise RelayError(f'no activity for {config.idle:g}s', 504)
                continue
            last_activity = time.monotonic()
            if item[0] == 'cli':
                event = item[1]
                if event is None:
                    if not turn.admission.used:
                        raise turn.native_failure()
                    continue
                turn.note(event)
                continue
            kind, payload = item[1], item[2]
            if kind == 'failed':
                raise RelayError(f'the upstream connection failed before an answer: {payload}', 502)
            if kind == 'start':
                status, headers = payload
                if prepared.stream:
                    self.send_response(status)
                    for key, value in headers:
                        self.send_header(key, value)
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    answered = True
                else:
                    self.pending = (status, headers)
            elif kind == 'chunk':
                capture.feed(payload)
                if prepared.stream:
                    try:
                        self.wfile.write(rewriter.feed(payload))
                        self.wfile.flush()
                    except OSError:
                        log.info('%s -> cancelled by Zed mid-stream', label)
                        return
            elif kind == 'end':
                if prepared.stream:
                    try:
                        self.wfile.write(rewriter.flush())
                        self.wfile.flush()
                    except OSError:
                        pass
                else:
                    self.answer_unstreamed(turn, capture)
                failure = f' relay_failure={payload}' if payload else ''
                log.info('%s -> %s %s request_id=%s denied=%s%s%s %.1fs', label, turn.admission.status, capture.usage_line(),
                         turn.admission.request_id, turn.admission.denied, failure,
                         ' unrestored' if turn.admission.unrestored else '', time.monotonic() - started)
                return

    def answer_unstreamed(self, turn, capture):
        status, headers = getattr(self, 'pending', (502, []))
        if status == 200 and capture.complete:
            message = copy.deepcopy(capture.message)
            for block in message.get('content', []):
                if isinstance(block, dict) and block.get('type') == 'tool_use':
                    turn.config.remember_tool_use(block.get('id'), block.get('name'))
                    block['name'] = unprefixed(block.get('name'))
            self.send_json(200, message)
        elif status == 200:
            self.send_error_json(502, 'the upstream stream ended before the message was complete')
        else:
            body = capture.raw
            self.send_response(status)
            for key, value in headers:
                if key.lower() != 'content-type':
                    self.send_header(key, value)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(body)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(body)


class RelayServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def child_environment(quiet):
    env = dict(os.environ)
    conflicts = [key for key in AUTH_CONFLICTS if env.pop(key, None)]
    if conflicts:
        log.warning('ignoring %s for Claude Code: the relay uses the CLI login only', ', '.join(conflicts))
    env.update(CLI_ENV)
    if quiet:
        env.update(QUIET_ENV)
    return env


def build_config(args):
    env = child_environment(not args.telemetry)
    claude = resolve_claude(env)
    if not claude:
        sys.exit('claude not found: install Claude Code (npm install -g @anthropic-ai/claude-code) or set ZED_CLAUDE_RELAY_CLAUDE')
    try:
        version = subprocess.run([claude, '--version'], capture_output=True, text=True, timeout=20, env=env).stdout.strip()
    except (OSError, subprocess.SubprocessError) as exc:
        version = f'version unknown ({exc})'
    log.info('using %s (%s)', claude, version)
    return Config(claude, args.upstream, env, stable_workdir(), idle=args.idle_timeout)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Serve the Anthropic Messages API to Zed through the Claude Code CLI login.')
    parser.add_argument('--port', type=int, default=int(os.environ.get('ZED_CLAUDE_RELAY_PORT', DEFAULT_PORT)))
    parser.add_argument('--upstream', default=os.environ.get('ANTHROPIC_BASE_URL') or DEFAULT_UPSTREAM,
                        help='where the CLI request goes (default: api.anthropic.com)')
    parser.add_argument('--idle-timeout', type=float, default=300, help='seconds without upstream bytes before a request is abandoned')
    parser.add_argument('--telemetry', action='store_true', help="leave Claude Code's telemetry and feature-flag traffic at its defaults")
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stderr)
    config = build_config(args)
    server = RelayServer(('127.0.0.1', args.port), RelayHandler)
    server.config = config
    log.info('listening on http://127.0.0.1:%d, upstream %s, cwd %s', args.port, config.upstream, config.workdir)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == '__main__':
    main()
