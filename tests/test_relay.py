"""End-to-end tests against a fake loopback upstream.

The real Claude Code CLI runs (it must be installed and logged in), but nothing leaves the
machine: the relay's upstream is a local Messages API stand-in that records what the CLI sent
and answers with synthetic streams. Authorization headers are never stored or printed.
"""
import http.client
import json
import os
import sys
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import relay  # noqa: E402

SECRET_HEADERS = ('authorization', 'x-api-key', 'cookie', 'proxy-authorization')


class FakeUpstream(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        peer = self.server
        raw = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        record = {'path': self.path, 'header_names': sorted(k.lower() for k in self.headers.keys()),
                  'accept_encoding': self.headers.get('Accept-Encoding'), 'has_authorization': bool(self.headers.get('Authorization')),
                  'has_api_key': bool(self.headers.get('X-Api-Key')), 'beta': self.headers.get('anthropic-beta', ''),
                  'user_agent': self.headers.get('User-Agent', ''), 'body': json.loads(raw)}
        peer.requests.append(record)
        if self.path.split('?')[0] != '/v1/messages':
            self.send_error(404)
            return
        if peer.error:
            payload = json.dumps(peer.error[1]).encode()
            self.send_response(peer.error[0])
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(payload)))
            self.send_header('request-id', 'req_fake_error')
            self.end_headers()
            self.wfile.write(payload)
            return
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('request-id', 'req_fake_%d' % len(peer.requests))
        self.end_headers()

        def emit(kind, data):
            self.wfile.write(('event: %s\ndata: %s\n\n' % (kind, json.dumps({'type': kind, **data}))).encode())
            self.wfile.flush()

        body = record['body']
        usage = {'input_tokens': 100, 'output_tokens': 1, 'cache_read_input_tokens': 20, 'cache_creation_input_tokens': 30}
        emit('message_start', {'message': {'id': 'msg_fake', 'type': 'message', 'role': 'assistant', 'model': body['model'],
                                           'content': [], 'stop_reason': None, 'stop_sequence': None, 'usage': usage}})
        index = 0
        if peer.thinking:
            emit('content_block_start', {'index': index, 'content_block': {'type': 'thinking', 'thinking': ''}})
            emit('content_block_delta', {'index': index, 'delta': {'type': 'thinking_delta', 'thinking': 'pondering'}})
            emit('content_block_delta', {'index': index, 'delta': {'type': 'signature_delta', 'signature': 'c2ln'}})
            emit('content_block_stop', {'index': index})
            index += 1
        if peer.tool_name:
            emit('content_block_start', {'index': index, 'content_block': {'type': 'tool_use', 'id': 'toolu_fake', 'name': peer.tool_name, 'input': {}}})
            emit('content_block_delta', {'index': index, 'delta': {'type': 'input_json_delta', 'partial_json': '{"path": "a.rs"}'}})
            emit('content_block_stop', {'index': index})
            stop = 'tool_use'
        else:
            emit('content_block_start', {'index': index, 'content_block': {'type': 'text', 'text': ''}})
            emit('content_block_delta', {'index': index, 'delta': {'type': 'text_delta', 'text': 'ok'}})
            emit('content_block_stop', {'index': index})
            stop = 'end_turn'
        emit('message_delta', {'delta': {'stop_reason': stop, 'stop_sequence': None}, 'usage': {'output_tokens': 7}})
        emit('message_stop', {})


def sse_events(raw):
    events = []
    for frame in raw.decode('utf-8').split('\n\n'):
        data = '\n'.join(line[5:].lstrip() for line in frame.split('\n') if line.startswith('data:'))
        if data:
            events.append(json.loads(data))
    return events


def zed_request(**overrides):
    body = {
        'model': 'claude-fable-5-1',
        'max_tokens': 128000,
        'system': 'You are the editor assistant.',
        'messages': [{'role': 'user', 'content': [{'type': 'text', 'text': 'Hello there'}]}],
        'tools': [{'name': 'edit_file', 'description': 'Edit a file', 'input_schema': {'type': 'object', 'properties': {'path': {'type': 'string'}}},
                   'eager_input_streaming': True}],
        'thinking': {'type': 'adaptive', 'display': 'summarized', 'block_binding': {'prefix_mismatch_behavior': 'drop_block'}},
        'output_config': {'effort': 'high'},
        'temperature': 1.0,
        'cache_control': {'type': 'ephemeral'},
        'stream': True,
    }
    body.update(overrides)
    return body


class RelayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        env = relay.child_environment(quiet=True)
        claude = relay.resolve_claude(env)
        if not claude:
            raise unittest.SkipTest('claude is not installed')
        cls.peer = ThreadingHTTPServer(('127.0.0.1', 0), FakeUpstream)
        threading.Thread(target=cls.peer.serve_forever, daemon=True).start()
        config = relay.Config(claude, 'http://127.0.0.1:%d' % cls.peer.server_port, env, relay.stable_workdir(), idle=60, replay_timeout=30)
        cls.server = relay.RelayServer(('127.0.0.1', 0), relay.RelayHandler)
        cls.server.config = config
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.peer.shutdown()
        cls.peer.server_close()

    def setUp(self):
        self.peer.requests = []
        self.peer.error = None
        self.peer.tool_name = None
        self.peer.thinking = False

    def post(self, path, body, headers=None):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=60)
        conn.request('POST', path, json.dumps(body), {'X-Api-Key': 'anything', 'Content-Type': 'application/json', **(headers or {})})
        response = conn.getresponse()
        raw = response.read()
        conn.close()
        return response.status, dict(response.getheaders()), raw

    def get(self, path):
        conn = http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=10)
        conn.request('GET', path, headers={'X-Api-Key': 'anything'})
        response = conn.getresponse()
        raw = response.read()
        conn.close()
        return response.status, raw

    def upstream_body(self):
        self.assertEqual(len(self.peer.requests), 1, 'exactly one upstream request per turn')
        return self.peer.requests[0]['body']

    def test_text_turn_is_translated_and_streamed_back(self):
        status, headers, raw = self.post('/v1/messages', zed_request())
        self.assertEqual(status, 200)
        self.assertTrue(headers.get('Content-Type', '').startswith('text/event-stream'))
        kinds = [e['type'] for e in sse_events(raw)]
        self.assertEqual(kinds[0], 'message_start')
        self.assertEqual(kinds[-1], 'message_stop')
        self.assertIn('ok', ''.join(e['delta']['text'] for e in sse_events(raw) if e.get('delta', {}).get('type') == 'text_delta'))
        record = self.peer.requests[0]
        body = self.upstream_body()
        self.assertTrue(record['has_authorization'])
        self.assertFalse(record['has_api_key'])
        self.assertEqual(record['accept_encoding'], 'identity')
        self.assertIn('oauth-2025-04-20', record['beta'])
        self.assertTrue(record['user_agent'].startswith('claude-cli/'))
        self.assertEqual(body['model'], 'claude-fable-5-1')
        self.assertEqual(body['max_tokens'], 128000)
        self.assertEqual(body['thinking'], {'type': 'adaptive', 'display': 'summarized'})
        self.assertEqual(body['output_config'], {'effort': 'high'})
        self.assertNotIn('temperature', body)
        self.assertNotIn('cache_control', body)
        self.assertEqual(body['tools'], [{'name': 'mcp__zed__edit_file', 'description': 'Edit a file',
                                          'input_schema': {'type': 'object', 'properties': {'path': {'type': 'string'}}}, 'eager_input_streaming': True}])
        system_texts = [b['text'] for b in body['system']]
        self.assertEqual(system_texts[-1], 'You are the editor assistant.')
        self.assertNotIn('Claude Code', ' '.join(system_texts))
        first = body['messages'][0]
        self.assertEqual(first['role'], 'user')
        self.assertIn({'type': 'text', 'text': 'Hello there'}, [relay.plain(b) for b in first['content']])

    def test_tool_call_reaches_zed_without_the_prefix(self):
        self.peer.tool_name = 'mcp__zed__edit_file'
        status, _, raw = self.post('/v1/messages', zed_request())
        self.assertEqual(status, 200)
        events = sse_events(raw)
        starts = [e for e in events if e['type'] == 'content_block_start' and e['content_block']['type'] == 'tool_use']
        self.assertEqual(len(starts), 1)
        self.assertEqual(starts[0]['content_block']['name'], 'edit_file')
        self.assertEqual(starts[0]['content_block']['id'], 'toolu_fake')
        self.assertEqual([e['delta']['partial_json'] for e in events if e.get('delta', {}).get('type') == 'input_json_delta'], ['{"path": "a.rs"}'])
        self.assertEqual([e['delta']['stop_reason'] for e in events if e['type'] == 'message_delta'], ['tool_use'])
        self.upstream_body()

    def test_history_replays_verbatim_with_restoration_and_pinned_breakpoint(self):
        history = [
            {'role': 'user', 'content': [{'type': 'text', 'text': 'hi'}]},
            {'role': 'assistant', 'content': [{'type': 'thinking', 'thinking': 'short thought', 'signature': 'ZmFrZXNpZw=='},
                                              {'type': 'text', 'text': 'hello'}]},
            {'role': 'user', 'content': [{'type': 'text', 'text': 'edit it'}]},
            {'role': 'assistant', 'content': [{'type': 'thinking', 'thinking': '', 'signature': 'c2lnMg=='},
                                              {'type': 'tool_use', 'id': 'toolu_01', 'name': 'edit_file', 'input': {'path': 'a.rs'}}]},
            {'role': 'user', 'content': [{'type': 'tool_result', 'tool_use_id': 'toolu_01', 'content': 'done'}]},
        ]
        status, _, raw = self.post('/v1/messages', zed_request(messages=history))
        self.assertEqual(status, 200, raw[:300])
        body = self.upstream_body()
        messages = body['messages']
        assistants = [m for m in messages if m['role'] == 'assistant']
        self.assertEqual(assistants[0]['content'][0], {'type': 'thinking', 'thinking': 'short thought', 'signature': 'ZmFrZXNpZw=='})
        self.assertEqual(assistants[1]['content'][1]['name'], 'mcp__zed__edit_file')
        self.assertEqual(assistants[1]['content'][1]['input'], {'path': 'a.rs'})
        last_assistant = max(i for i, m in enumerate(messages) if m['role'] == 'assistant')
        newest = messages[last_assistant + 1]
        self.assertEqual(newest['role'], 'user')
        self.assertEqual(relay.plain(newest['content'][0]), {'type': 'tool_result', 'tool_use_id': 'toolu_01', 'content': 'done'})
        marks = [(i, j) for i, m in enumerate(messages) if isinstance(m.get('content'), list)
                 for j, b in enumerate(m['content']) if isinstance(b, dict) and 'cache_control' in b]
        self.assertEqual(marks, [(last_assistant + 1, len(newest['content']) - 1)])

    def test_upstream_error_is_forwarded(self):
        self.peer.error = (400, {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'prompt is too long'}})
        status, headers, raw = self.post('/v1/messages', zed_request())
        self.assertEqual(status, 400)
        self.assertEqual(json.loads(raw)['error']['message'], 'prompt is too long')
        self.assertEqual(headers.get('request-id'), 'req_fake_error')
        self.assertEqual(len(self.peer.requests), 1)

    def test_unsupported_fields_fail_before_any_process(self):
        status, _, raw = self.post('/v1/messages', zed_request(speed='fast'))
        self.assertEqual(status, 400)
        self.assertIn('speed', json.loads(raw)['error']['message'])
        self.assertEqual(self.peer.requests, [])
        status, _, raw = self.post('/v1/messages', zed_request(messages=[{'role': 'user', 'content': 'a'}, {'role': 'assistant', 'content': 'b'}]))
        self.assertEqual(status, 400)
        self.assertIn('prefill', json.loads(raw)['error']['message'])

    def test_non_streaming_answer_is_assembled(self):
        self.peer.tool_name = 'mcp__zed__edit_file'
        self.peer.thinking = True
        status, headers, raw = self.post('/v1/messages', zed_request(stream=False))
        self.assertEqual(status, 200, raw[:300])
        message = json.loads(raw)
        self.assertEqual(message['stop_reason'], 'tool_use')
        self.assertEqual(message['content'][0], {'type': 'thinking', 'thinking': 'pondering', 'signature': 'c2ln'})
        self.assertEqual(message['content'][1], {'type': 'tool_use', 'id': 'toolu_fake', 'name': 'edit_file', 'input': {'path': 'a.rs'}})
        self.assertEqual(message['usage']['output_tokens'], 7)

    def test_models_listing(self):
        status, raw = self.get('/v1/models')
        self.assertEqual(status, 200)
        listing = {m['id']: m for m in json.loads(raw)['data']}
        self.assertEqual(listing['claude-fable-5-1']['max_input_tokens'], 1_000_000)
        self.assertTrue(listing['claude-fable-5-1']['capabilities']['thinking']['types']['adaptive']['supported'])
        self.assertEqual(listing['claude-haiku-4-5-20251001']['max_input_tokens'], 200_000)

    def test_tool_name_the_model_dropped_the_prefix_from_replays_as_named(self):
        self.peer.tool_name = 'edit_file'
        status, _, raw = self.post('/v1/messages', zed_request())
        self.assertEqual(status, 200)
        self.peer.requests = []
        self.peer.tool_name = None
        history = [
            {'role': 'user', 'content': 'go'},
            {'role': 'assistant', 'content': [{'type': 'tool_use', 'id': 'toolu_fake', 'name': 'edit_file', 'input': {'path': 'a.rs'}},
                                              {'type': 'tool_use', 'id': 'toolu_other', 'name': 'edit_file', 'input': {'path': 'b.rs'}}]},
            {'role': 'user', 'content': [{'type': 'tool_result', 'tool_use_id': 'toolu_fake', 'content': 'x'},
                                         {'type': 'tool_result', 'tool_use_id': 'toolu_other', 'content': 'y'}]},
        ]
        status, _, raw = self.post('/v1/messages', zed_request(messages=history))
        self.assertEqual(status, 200, raw[:300])
        calls = next(m for m in self.upstream_body()['messages'] if m['role'] == 'assistant')['content']
        self.assertEqual([c['name'] for c in calls if c['type'] == 'tool_use'], ['edit_file', 'mcp__zed__edit_file'])

    def test_browser_origin_is_refused(self):
        status, _, _ = self.post('/v1/messages', zed_request(), headers={'Origin': 'https://example.com'})
        self.assertEqual(status, 403)
        self.assertEqual(self.peer.requests, [])

    def test_count_tokens_estimate(self):
        status, _, raw = self.post('/v1/messages/count_tokens', zed_request())
        self.assertEqual(status, 200)
        self.assertGreater(json.loads(raw)['input_tokens'], 10)

    def test_empty_system_prompt(self):
        status, _, raw = self.post('/v1/messages', zed_request(system=''))
        self.assertEqual(status, 200, raw[:300])
        texts = ' '.join(b['text'] for b in self.upstream_body()['system'])
        self.assertNotIn('Claude Code', texts)


if __name__ == '__main__':
    unittest.main()
