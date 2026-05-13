# AgentCore Pi Extension

Pi package for connecting one live Pi session to [CloudNova](https://cloudnova.kjulin.dev) as an external agent. Once installed, you can drive a Pi session from CloudNova Web (or any CloudNova client) and have Pi's assistant messages, tool calls, and run results mirror back into the CloudNova thread.

## Install

```bash
pi install git:github.com/kjulin/agentcore-pi
```

Pin a specific release with `@v0.1.0` if you want a fixed version. Pi clones the repo and runs `npm install` automatically (it fetches `firebase`).

After install, restart Pi or run `/reload` in the active session to pick up the extension.

## First-time setup

1. In CloudNova Web, go to **Settings → Tokens** and create a workspace token.
2. In Pi:

   ```text
   /cloudnova login <workspace-token>
   /cloudnova connect <agent-id>
   ```

   `login` stores the token at `~/.pi/agent/cloudnova.json`.

   `connect` binds the current Pi session to a CloudNova external agent. If the agent ID doesn't exist yet, it's created as `external: { type: "pi" }`. If the ID exists but isn't external, `connect` refuses to avoid hijacking a managed agent.

3. In CloudNova Web, open the agent and send a message. Pi will receive it formatted as:

   ```text
   [CloudNova message]
   From: <user> via Web

   <message>
   ```

   Let Pi reply. The thread shows Pi's `agent` response and a final `result` event. Tool calls show up as `tool_use` / `tool_result`.

## Commands

```text
/cloudnova help
/cloudnova status              show login + listener state
/cloudnova login <token>       log in with a workspace token
/cloudnova agents              list workspace agents
/cloudnova connect <agent-id>  bind this Pi session to an agent
/cloudnova disconnect          unbind this Pi session
/cloudnova logout              remove stored token
```

## Behavior

- One Pi session connects to one CloudNova agent at a time.
- A connected session has one active CloudNova thread at a time.
- The first CloudNova input for a thread makes that thread active.
- If another CloudNova thread sends input while Pi is idle, the extension compacts (soft-clear) the previous thread and switches.
- If another input arrives while Pi is busy or already switching, the extension posts a CloudNova `error` event instead of queueing.
- Local user input in the connected Pi session is mirrored to the active CloudNova thread as a silent `input` event.
- Listener reconnects with exponential backoff on Firestore errors and is recreated on Pi reload/resume from the saved session binding.
- Reconnect is live-only: each new listener starts from "now" and does not replay missed inputs.

## Limitations

- No missed-message replay yet — messages sent while the listener is offline may be missed.
- Thread switches use Pi's compaction as a soft clear, not a true `/new`; recent previous-thread context may remain inside the compaction window.
- No durable CloudNova-thread → Pi-session mapping; older CloudNova threads don't restore their previous Pi session.
- No headless worker mode.
- No remote approval flow for Pi tool permission prompts.
- File upload/download is not complete.
- CloudNova agent config (system prompt, skills, secrets) is not injected into Pi yet.
