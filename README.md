# Pi CloudNova Extension

Pi package for connecting one live Pi session to CloudNova as an external agent.

## Install

```bash
pi install git:github.com/kjulin/agentcore-pi
```

Pin a specific release with `@v0.1.0`. After install, restart Pi or run `/reload` in the active session.

## Local development install

For monorepo contributors:

```bash
npm run build --workspace=@kjulin/pi-cloudnova
pi install ./packages/pi-cloudnova -l
```

Then restart Pi or run `/reload` in the active Pi session. Pi only discovers newly installed project packages on startup/reload.

The package manifest loads `dist/index.js`, so rebuild after source changes.

## Releasing

`packages/pi-cloudnova/scripts/release.sh` builds a self-contained bundle and stages a commit + tag in a local clone of `kjulin/agentcore-pi` (under `packages/pi-cloudnova/release/`, gitignored). It does not push. After running it, follow the printed `git push` instruction.

## Commands

```text
/cloudnova help
/cloudnova status
/cloudnova login <workspace-token>
/cloudnova agents
/cloudnova connect <agent-id>
/cloudnova disconnect
/cloudnova logout
```

`login` stores the CloudNova workspace token at `~/.pi/agent/cloudnova.json`.

`connect` binds the current Pi session to an external CloudNova agent and starts listening for new CloudNova input events. If the agent ID does not exist, it creates a new `external: { type: "pi" }` agent with that ID. If the agent exists but is internal, connect rejects to avoid accidentally taking over a managed runtime agent.

## Current behavior

- One Pi session can be connected to one CloudNova agent at a time.
- One connected Pi session has one active CloudNova thread at a time.
- If the first CloudNova input arrives for a thread, that thread becomes active.
- If another CloudNova thread sends input while Pi is idle, the extension runs an aggressive compaction as a soft clear, binds to the new thread, and sends the input.
- If another input arrives while Pi is busy or already switching threads, the extension posts a CloudNova `error` event instead of queueing it.
- Pi assistant messages are mirrored as CloudNova `agent` events.
- Pi tool lifecycle events are mirrored as CloudNova `tool_use` / `tool_result` events.
- Pi turn completion is mirrored as a CloudNova `result` event.
- Local user input in the connected Pi session is mirrored back to the active CloudNova thread as a silent `input` event.
- The Firestore input listener is in-memory and is recreated from the saved session binding on Pi reload/resume.
- Listener errors trigger exponential-backoff reconnect attempts.
- Reconnect is live-only: each new listener starts from "now" and does not replay missed inputs.
- `/cloudnova status` and the Pi status bar show listener state.

## Limitations

- No missed-message replay yet; messages sent while the listener is offline may be missed.
- Thread switches use Pi compaction as a soft clear, not a true `/new`; recent previous-thread messages may remain if Pi keeps them in the compaction window.
- No durable CloudNova thread -> Pi session mapping yet; older CloudNova threads do not restore their previous Pi session.
- No headless worker mode yet.
- No remote approval flow for Pi tool permission prompts.
- File upload/download support is not complete.
- CloudNova agent config such as system prompt, skills, and secrets is not injected into Pi yet.

## Manual smoke test

1. Build and install the package:

   ```bash
   npm run build --workspace=@kjulin/pi-cloudnova
   pi install ./packages/pi-cloudnova -l
   ```

2. Restart Pi or run `/reload`.
3. In CloudNova Web, create a workspace token from Settings -> Tokens.
4. In Pi, run:

   ```text
   /cloudnova login <workspace-token>
   /cloudnova status
   /cloudnova connect pi-engineer
   ```

5. Open CloudNova Web, select the `pi-engineer` agent, and send a message.
6. Verify that Pi receives a message formatted like:

   ```text
   [CloudNova message]
   From: Klaus Julin via Web

   Hello
   ```

7. Let Pi answer. Verify in CloudNova Web that the thread receives an `agent` response and a completion `result`.
8. Optionally run a Pi tool and verify that CloudNova shows `tool_use` / `tool_result` events.
9. Run `/cloudnova disconnect` when done.

## Next planned work

1. Web badge/icon for external agents by `external.type`.
2. CloudNova-managed Pi agent config: system prompt, skills, and secrets.
3. Optional missed-input replay once idempotency/run leasing exists.
