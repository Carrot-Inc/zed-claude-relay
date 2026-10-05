# zed-claude-relay

Use a Claude Pro/Max subscription with Zed's built-in agent, through the stock Claude Code CLI.

Zed's Anthropic-compatible provider is pointed at a loopback server. Every request Zed makes is
handed to a fresh `claude -p` process that authenticates with the normal Claude Code login, builds
the upstream request itself and bills the turn to the subscription, exactly as `claude -p` or the
Agent SDK would. The relay never reads, copies or refreshes credentials; the only thing it touches
is the CLI's own HTTP request on its way to `api.anthropic.com`.

The mechanism is a port of the Hermes Agent plugin
[claude-subscription-directsdk](https://github.com/NousResearch/hermes-plugin-claude-subscription-directsdk)
(MIT, Nous Research and contributors), with Zed's native Messages-API client in place of Hermes'
chat-completions translation.

## What Anthropic says

From [Use the Claude Agent SDK with your Claude plan](https://support.claude.com/en/articles/15036540-use-the-claude-agent-sdk-with-your-claude-plan)
(update of June 15, 2026): "Claude Agent SDK, `claude -p`, and third-party app usage still draw from
your subscription's usage limits." The planned separate monthly credit for that usage was paused.
The relay drives `claude -p`; whether that fits the account's terms is the account holder's call.

## How a request travels

1. Zed sends a Messages request (system prompt, history, tool schemas, model, thinking, effort).
2. The relay turns it into the CLI's inputs: the system prompt becomes `--system-prompt-file`, the
   history is replayed over stream-json (earlier user turns acknowledged with zero-turn results,
   assistant turns replayed as complete messages so thinking blocks and signatures survive), the
   tools become an inert MCP server inventory plus a `CLAUDE_CODE_EXTRA_BODY` override, so the wire
   schemas are Zed's, under the names `mcp__zed__<tool>`.
3. The CLI runs with `--tools ''`, `--max-turns 1`, `--permission-mode dontAsk`, no session
   persistence, no compaction, no retries, and `ANTHROPIC_BASE_URL` set to a per-request admission
   proxy on loopback with a random path.
4. The admission proxy forwards exactly one `POST /v1/messages` upstream with the CLI's own headers
   (OAuth bearer, beta list, user agent), streams the answer back to the CLI and to Zed, and answers
   any further attempt with a local 400. Zed gets the upstream bytes unchanged, except that
   `tool_use` names lose the `mcp__zed__` prefix; upstream errors arrive with their status and
   rate-limit headers.
5. When the model calls a tool the CLI denies its own (inert) call and stops at the turn limit;
   Zed runs the tool and sends the next request.

Two things the CLI adds to every request cannot be removed: its own two system blocks (a billing
header line and "You are a Claude agent, built on Anthropic's Claude Agent SDK.") ahead of Zed's
system prompt, and its per-request context (environment, model, date, account email, a prompt nudge)
as mid-conversation `system` messages on models that support them (Fable, Opus 5.x, Sonnet 5.5) or as
`<system-reminder>` text blocks on older ones.

### Cache hygiene

The CLI appends the account reminder inside the newest tool result and puts its cache breakpoint on
a trailing per-request system message that never recurs; Zed replays the turn without either, so
each tool round would rewrite the whole history. Two transformations from the Hermes plugin keep the
prefix stable: the queried tool-result turn is restored to Zed's bytes (only trailing bare text the
CLI appended is removed, and only when the frame maps unambiguously), and marks after the stable span
are folded into one on the last block the next request replays. Measured on Fable 5.1 through this
relay: a three-request tool round read 15 379 and 15 729 of about 15 800 prompt tokens from cache on
requests 2 and 3.

## Requirements

- macOS or Linux, Python 3.9+ (standard library only).
- Claude Code 2.1.280 or newer, installed and logged in (`claude auth login`). The relay finds
  `claude` on PATH or in the usual install prefixes; `ZED_CLAUDE_RELAY_CLAUDE` overrides.
- Zed 1.22 or newer (the `anthropic_compatible` provider).

## Install

```sh
git clone <this repo> ~/programming/zed-claude-relay
cd ~/programming/zed-claude-relay
launchd/install.sh          # macOS: a LaunchAgent that keeps the relay on 127.0.0.1:7865
```

Or run it in a terminal: `python3 relay.py` (flags: `--port`, `--idle-timeout`, `--telemetry`,
`--verbose`). Logs are one line per request (model, status, token usage, request id, timing), never
headers or content. The LaunchAgent writes them to `~/Library/Logs/zed-claude-relay.log`.

If `ANTHROPIC_API_KEY`, `ANTHROPIC_AUTH_TOKEN` or a Bedrock/Vertex/Foundry switch is in the relay's
environment it is dropped for the CLI with a warning, so the subscription login is always what is
used. `CLAUDE_CONFIG_DIR` is honoured.

## Zed settings

```jsonc
"language_models": {
  "anthropic_compatible": {
    "Claude Subscription": {
      "api_url": "http://127.0.0.1:7865",
      "available_models": [
        {
          "name": "claude-fable-5-1",
          "display_name": "Fable 5.1 (subscription)",
          "max_tokens": 1000000,
          "max_output_tokens": 128000,
          "mode": { "type": "adaptive" },
          "capabilities": { "tools": true, "images": true, "prompt_caching": false }
        }
        // same shape for claude-opus-5-5, claude-sonnet-5-5; Haiku 4.5 is
        // claude-haiku-4-5-20251001 with max_tokens 200000, max_output_tokens 64000
        // and "mode": { "type": "thinking", "budget_tokens": 4096 }
      ]
    }
  }
}
```

Then open Agent settings, find "Claude Subscription" and enter any text as the API key (the relay
only checks that one is present; Zed stores it in the keychain). Setting the environment variable
`CLAUDE_SUBSCRIPTION_API_KEY` before Zed starts does the same. Pick a model from the provider in the
agent panel, or make it the default:

```jsonc
"agent": { "default_model": { "provider": "Claude Subscription", "model": "claude-fable-5-1", "enable_thinking": true, "effort": "high" } }
```

Keep `prompt_caching` off: the CLI places the cache breakpoints, and Zed's own marks would push the
request over the four-breakpoint limit.

## What is passed, dropped or refused

| Zed sends | The relay does |
|---|---|
| `model` | pinned ids get the CLI's 1M-context spelling (`claude-fable-5-1[1m]`); others pass through |
| `system`, `messages`, `tools`, `max_tokens`, `stop_sequences`, `tool_choice` | passed through (tool names prefixed on the wire, unprefixed on the way back) |
| `thinking` | `type`, `budget_tokens` and `display` pass; `block_binding` is dropped (needs a beta the CLI does not send) |
| `output_config.effort` | becomes `--effort` and the body's `output_config` |
| `temperature`, `top_p`, `top_k`, `cache_control` | dropped: subscription routes reject sampling controls, and caching is the CLI's |
| `speed`, `context_management`, `metadata`, anything else | refused with a 400 naming the field |
| `thinking: {type: disabled}` | also sends `context_management: {edits: []}`, the CLI's clear-thinking edit is invalid with thinking off |

Tool-use ids are remembered with the name the model produced, so a call the model made without the
prefix replays as the model named it.

`GET /v1/models` returns a pinned catalogue in the shape Zed's native Anthropic provider reads, and
`POST /v1/messages/count_tokens` returns a character-based estimate; both exist so the native
provider can also be pointed at the relay through `language_models.anthropic.api_url`.

## Limits

- One `claude` process per request: a few hundred milliseconds of start-up before the first byte.
- Fast mode, server-side compaction and forced tool choice on models that reject it are not
  available; Zed's compatible provider does not offer the first two.
- A thread that started on the API-key provider replays its tool calls with the prefix added, which
  Fable 5.1 may treat as an edited history on accounts created after 2026-08-31.
- Usage draws from the subscription at the `claude -p` rate; the subscription's extra-usage setting
  decides whether anything is billed beyond the plan.
- No HTTP proxy support for the upstream connection.

## Tests

`python3 -m unittest tests.test_relay -v` runs the real CLI against a fake loopback Messages API: it
checks the translation, the prefix handling both ways, history replay with restoration and the
pinned breakpoint, error forwarding, non-streaming answers and the listing. Nothing leaves the
machine, but the CLI must be installed and logged in.
