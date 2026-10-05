"""Minimal MCP stdio server: advertises the tools in a manifest file, refuses every call."""
import json, sys
from pathlib import Path

def main():
    tools = json.loads(Path(sys.argv[1]).read_text())
    for line in sys.stdin:
        msg = json.loads(line)
        method, rid = msg.get('method'), msg.get('id')
        if method == 'initialize':
            result = {'protocolVersion': '2024-11-05', 'capabilities': {'tools': {}}, 'serverInfo': {'name': 'zed-inert', 'version': '1'}}
        elif method == 'tools/list':
            result = {'tools': tools}
        elif method == 'tools/call':
            result = {'isError': True, 'content': [{'type': 'text', 'text': 'Tools run in the editor, not here.'}]}
        else:
            result = {}
        if rid is not None:
            print(json.dumps({'jsonrpc': '2.0', 'id': rid, 'result': result}), flush=True)

if __name__ == '__main__':
    main()
