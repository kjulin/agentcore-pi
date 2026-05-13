// src/index.ts
import os2 from "node:os";

// ../sdk/dist/agent-core.js
import { deleteApp } from "firebase/app";
import { collection as collection2, doc as doc2, getDoc as getDoc2, getDocs as getDocs2, setDoc as setDoc2 } from "firebase/firestore";

// ../sdk/dist/firebase.js
import { initializeApp } from "firebase/app";
import { getAuth } from "firebase/auth";
import { UserImpl } from "@firebase/auth/internal";
import { getFirestore } from "firebase/firestore";
async function initCloudNovaFirebase(token) {
  const tokenRes = await fetch(`https://securetoken.googleapis.com/v1/token?key=${token.apiKey}`, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: new URLSearchParams({
      grant_type: "refresh_token",
      refresh_token: token.refreshToken
    })
  });
  if (!tokenRes.ok) {
    throw new Error(`Failed to exchange CloudNova refresh token: ${await tokenRes.text()}`);
  }
  const tokenData = await tokenRes.json();
  const app = initializeApp({
    apiKey: token.apiKey,
    projectId: token.projectId,
    authDomain: `${token.projectId}.firebaseapp.com`,
    storageBucket: `${token.projectId}.firebasestorage.app`
  }, `cloudnova-sdk-${token.workspaceId}-${Math.random().toString(36).slice(2)}`);
  const auth = getAuth(app);
  const user = await UserImpl._fromIdTokenResponse(auth, {
    idToken: tokenData.id_token,
    refreshToken: tokenData.refresh_token,
    expiresIn: tokenData.expires_in,
    localId: tokenData.user_id
  });
  await auth.updateCurrentUser(user);
  const workspaceRef = `workspaces/${token.workspaceId}`;
  return {
    app,
    firestore: getFirestore(app),
    workspaceId: token.workspaceId,
    workspaceRef,
    userId: tokenData.user_id
  };
}

// ../sdk/dist/threads.js
import { addDoc, collection, deleteDoc, doc, getDoc, getDocs, limit as firestoreLimit, onSnapshot, orderBy, query, setDoc, updateDoc, where } from "firebase/firestore";
var Threads = class {
  firestore;
  workspaceRef;
  agentId;
  threadsPath;
  eventsPath;
  constructor(firestore, workspaceRef, agentId) {
    this.firestore = firestore;
    this.workspaceRef = workspaceRef;
    this.agentId = agentId;
    this.threadsPath = `${workspaceRef}/threads`;
    this.eventsPath = `${workspaceRef}/thread-events`;
  }
  async list() {
    const q = query(collection(this.firestore, this.threadsPath), where("agentId", "==", this.agentId));
    const snap = await getDocs(q);
    const threads = snap.docs.map((d) => docToThread(d.id, d.data()));
    threads.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
    return threads;
  }
  async get(threadId) {
    const snap = await getDoc(doc(this.firestore, this.threadsPath, threadId));
    if (!snap.exists())
      throw new Error(`Thread not found: ${threadId}`);
    const thread = docToThread(threadId, snap.data());
    if (thread.agentId !== this.agentId) {
      throw new Error(`Thread ${threadId} belongs to agent ${thread.agentId}, not ${this.agentId}.`);
    }
    return thread;
  }
  async create(opts) {
    const id = randomId();
    const now = (/* @__PURE__ */ new Date()).toISOString();
    const thread = {
      id,
      agentId: this.agentId,
      createdAt: now,
      updatedAt: now,
      ...opts?.title !== void 0 ? { title: opts.title } : {},
      metadata: { source: "sdk", ...opts?.metadata }
    };
    const { id: _id, ...data } = thread;
    await setDoc(doc(this.firestore, this.threadsPath, id), data);
    return thread;
  }
  async delete(threadId) {
    await this.get(threadId);
    const q = query(collection(this.firestore, this.eventsPath), where("threadId", "==", threadId));
    const snap = await getDocs(q);
    await Promise.all(snap.docs.map((d) => deleteDoc(d.ref)));
    await deleteDoc(doc(this.firestore, this.threadsPath, threadId));
  }
  async getEvents(threadId) {
    await this.get(threadId);
    const q = query(collection(this.firestore, this.eventsPath), where("threadId", "==", threadId), orderBy("timestamp"));
    const snap = await getDocs(q);
    return snap.docs.map((d) => docToEvent(d.data()));
  }
  async post(threadId, event) {
    await this.get(threadId);
    const timestamped = { ...event, timestamp: (/* @__PURE__ */ new Date()).toISOString() };
    const docData = {
      ...eventToDoc(timestamped),
      threadId,
      agentId: this.agentId
    };
    await addDoc(collection(this.firestore, this.eventsPath), docData);
    await updateDoc(doc(this.firestore, this.threadsPath, threadId), { updatedAt: timestamped.timestamp });
  }
  onInput(listener, options = {}) {
    const startTime = (/* @__PURE__ */ new Date()).toISOString();
    const agentCache = /* @__PURE__ */ new Map();
    const q = query(collection(this.firestore, this.eventsPath), where("timestamp", ">", startTime), orderBy("timestamp"));
    const unsub = onSnapshot(q, (snap) => {
      for (const change of snap.docChanges()) {
        if (change.type !== "added")
          continue;
        const data = change.doc.data();
        const threadId = data.threadId;
        if (!threadId)
          continue;
        const event = docToEvent(data);
        if (event.type !== "input")
          continue;
        void this.threadBelongsToAgent(threadId, agentCache).then((belongs) => {
          if (belongs)
            void listener(threadId, event);
        });
      }
    }, (error) => {
      void options.onError?.(error instanceof Error ? error : new Error(String(error)));
    });
    return unsub;
  }
  onThreadEvent(threadId, listener) {
    const startTime = (/* @__PURE__ */ new Date()).toISOString();
    const q = query(collection(this.firestore, this.eventsPath), where("threadId", "==", threadId), where("timestamp", ">", startTime), orderBy("timestamp"));
    const unsub = onSnapshot(q, (snap) => {
      for (const change of snap.docChanges()) {
        if (change.type === "added")
          void listener(docToEvent(change.doc.data()));
      }
    });
    return unsub;
  }
  async findByMetadata(key, value) {
    const q = query(collection(this.firestore, this.threadsPath), where("agentId", "==", this.agentId), where(`metadata.${key}`, "==", value), firestoreLimit(1));
    const snap = await getDocs(q);
    if (snap.empty)
      return null;
    const d = snap.docs[0];
    return docToThread(d.id, d.data());
  }
  async threadBelongsToAgent(threadId, cache) {
    const cached = cache.get(threadId);
    if (cached !== void 0)
      return cached;
    try {
      const thread = await this.get(threadId);
      const belongs = thread.agentId === this.agentId;
      cache.set(threadId, belongs);
      return belongs;
    } catch {
      cache.set(threadId, false);
      return false;
    }
  }
};
function randomId() {
  const bytes = new Uint8Array(8);
  globalThis.crypto.getRandomValues(bytes);
  return [...bytes].map((b) => b.toString(16).padStart(2, "0")).join("");
}
function docToThread(id, data) {
  const thread = {
    id,
    agentId: requireString(data, "agentId"),
    createdAt: requireString(data, "createdAt"),
    updatedAt: requireString(data, "updatedAt"),
    ...typeof data.title === "string" ? { title: data.title } : {},
    ...Array.isArray(data.topics) ? { topics: data.topics.filter((v) => typeof v === "string") } : {},
    ...isStringRecord(data.metadata) ? { metadata: data.metadata } : {},
    ...typeof data.sessionId === "string" ? { sessionId: data.sessionId } : {},
    ...typeof data.credits === "number" ? { credits: data.credits } : {},
    ...typeof data.createdBy === "string" ? { createdBy: data.createdBy } : {}
  };
  return thread;
}
function docToEvent(data) {
  const { threadId: _threadId, sdkEvent, ...rest } = data;
  const event = { ...rest };
  if (typeof sdkEvent === "string") {
    try {
      event.sdkEvent = JSON.parse(sdkEvent);
    } catch {
      event.sdkEvent = sdkEvent;
    }
  }
  return event;
}
function eventToDoc(event) {
  const { sdkEvent, ...rest } = event;
  if (sdkEvent !== void 0)
    return { ...rest, sdkEvent: JSON.stringify(sdkEvent) };
  return rest;
}
function requireString(data, key) {
  const value = data[key];
  if (typeof value !== "string")
    throw new Error(`Invalid thread document: missing ${key}.`);
  return value;
}
function isStringRecord(value) {
  if (!value || typeof value !== "object" || Array.isArray(value))
    return false;
  return Object.values(value).every((v) => typeof v === "string");
}

// ../sdk/dist/token.js
function parseWorkspaceToken(raw) {
  let parsed;
  try {
    parsed = JSON.parse(decodeBase64(raw));
  } catch {
    throw new Error("CloudNova token is not valid base64-encoded JSON.");
  }
  if (!parsed || typeof parsed !== "object") {
    throw new Error("CloudNova token is invalid.");
  }
  const obj = parsed;
  const workspaceId = requireString2(obj, "workspaceId");
  const projectId = requireString2(obj, "projectId");
  const apiKey = requireString2(obj, "apiKey");
  const refreshToken = requireString2(obj, "refreshToken");
  const mode = obj["mode"];
  if (mode !== void 0 && mode !== "read" && mode !== "write") {
    throw new Error('CloudNova token field "mode" must be "read" or "write" when present.');
  }
  return {
    workspaceId,
    projectId,
    apiKey,
    refreshToken,
    ...mode ? { mode } : {}
  };
}
function requireString2(obj, key) {
  const value = obj[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`CloudNova token is missing required field "${key}".`);
  }
  return value;
}
function decodeBase64(raw) {
  if (typeof Buffer !== "undefined") {
    return Buffer.from(raw, "base64").toString("utf-8");
  }
  const binary = atob(raw);
  const bytes = Uint8Array.from(binary, (ch) => ch.charCodeAt(0));
  return new TextDecoder().decode(bytes);
}

// ../sdk/dist/agent-core.js
var AgentCore = class _AgentCore {
  connection;
  token;
  workspaceId;
  agentsPath;
  constructor(connection, token) {
    this.connection = connection;
    this.token = token;
    this.workspaceId = connection.workspaceId;
    this.agentsPath = `${connection.workspaceRef}/agents`;
  }
  static async connect(opts) {
    const token = parseWorkspaceToken(opts.token);
    const connection = await initCloudNovaFirebase(token);
    return new _AgentCore(connection, token);
  }
  agent(agentId) {
    return new AgentHandle(this.connection.firestore, this.connection.workspaceRef, agentId);
  }
  async listAgents() {
    const snap = await getDocs2(collection2(this.connection.firestore, this.agentsPath));
    const agents = snap.docs.map((d) => docToAgent(d.id, d.data()));
    agents.sort((a, b) => a.name.localeCompare(b.name));
    return agents;
  }
  async getAgent(agentId) {
    const snap = await getDoc2(doc2(this.connection.firestore, this.agentsPath, agentId));
    if (!snap.exists())
      throw new Error(`Agent not found: ${agentId}`);
    return docToAgent(agentId, snap.data());
  }
  async connectAgent(opts) {
    const externalType = opts.type ?? "sdk";
    const existingSnap = await getDoc2(doc2(this.connection.firestore, this.agentsPath, opts.id));
    if (existingSnap.exists()) {
      const existing = docToAgent(opts.id, existingSnap.data());
      if (!existing.external) {
        throw new Error(`Agent ${opts.id} exists but is not external.`);
      }
      await setDoc2(doc2(this.connection.firestore, this.agentsPath, opts.id), {
        name: opts.name,
        ...opts.description !== void 0 ? { description: opts.description } : {},
        external: { type: externalType }
      }, { merge: true });
    } else {
      const agent = {
        name: opts.name,
        ...opts.description !== void 0 ? { description: opts.description } : {},
        directories: [],
        tools: [],
        skills: [],
        secrets: [],
        integrations: [],
        createdBy: this.connection.userId,
        external: { type: externalType }
      };
      await setDoc2(doc2(this.connection.firestore, this.agentsPath, opts.id), agent);
    }
    return this.agent(opts.id);
  }
  async disconnect() {
    await deleteApp(this.connection.app);
  }
};
var AgentHandle = class {
  id;
  threads;
  constructor(firestore, workspaceRef, id) {
    this.id = id;
    this.threads = new Threads(firestore, workspaceRef, id);
  }
};
function docToAgent(id, data) {
  return {
    id,
    name: typeof data.name === "string" ? data.name : id,
    ...typeof data.description === "string" ? { description: data.description } : {},
    ...typeof data.purpose === "string" ? { purpose: data.purpose } : {},
    ...typeof data.systemPrompt === "string" ? { systemPrompt: data.systemPrompt } : {},
    directories: Array.isArray(data.directories) ? data.directories : [],
    tools: stringArray(data.tools),
    skills: stringArray(data.skills),
    secrets: stringArray(data.secrets),
    integrations: stringArray(data.integrations),
    ...typeof data.model === "string" ? { model: data.model } : {},
    ...typeof data.createdBy === "string" ? { createdBy: data.createdBy } : {},
    ...isExternalConfig(data.external) ? { external: data.external } : {}
  };
}
function stringArray(value) {
  return Array.isArray(value) ? value.filter((v) => typeof v === "string") : [];
}
function isExternalConfig(value) {
  return !!value && typeof value === "object" && !Array.isArray(value) && typeof value.type === "string";
}

// src/config.ts
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
var CONFIG_PATH = path.join(os.homedir(), ".pi", "agent", "cloudnova.json");
function getConfigPath() {
  return CONFIG_PATH;
}
async function loadConfig() {
  try {
    const raw = await fs.readFile(CONFIG_PATH, "utf-8");
    const data = JSON.parse(raw);
    if (typeof data.token !== "string") return void 0;
    const token = parseWorkspaceToken(data.token);
    return {
      token: data.token,
      workspaceId: token.workspaceId,
      projectId: token.projectId
    };
  } catch (err) {
    if (err.code === "ENOENT") return void 0;
    throw err;
  }
}
async function saveConfig(tokenRaw) {
  const token = parseWorkspaceToken(tokenRaw);
  const config = {
    token: tokenRaw,
    workspaceId: token.workspaceId,
    projectId: token.projectId
  };
  await fs.mkdir(path.dirname(CONFIG_PATH), { recursive: true });
  await fs.writeFile(CONFIG_PATH, JSON.stringify(config, null, 2) + "\n", { mode: 384 });
  return config;
}
async function deleteConfig() {
  try {
    await fs.unlink(CONFIG_PATH);
  } catch (err) {
    if (err.code !== "ENOENT") throw err;
  }
}

// src/index.ts
var BINDING_ENTRY = "cloudnova-binding";
var MESSAGE_TYPE = "cloudnova-status";
var THREAD_SWITCH_COMPACTION_PROMPT = [
  "CloudNova is switching to a different thread.",
  "Aggressively discard the previous CloudNova thread. Do not preserve prior user requests, plans, preferences, or conversational details unless they are durable workspace facts needed for safe coding.",
  "Preserve only files that were read or modified, unresolved local repo state, and concrete tool side effects that could affect future work.",
  "The summary should make clear that the previous CloudNova conversation was intentionally closed and should not be continued."
].join("\n\n");
var INITIAL_RECONNECT_DELAY_MS = 1e3;
var MAX_RECONNECT_DELAY_MS = 3e4;
var globalState = globalThis;
var state = globalState.__cloudNovaPiState__ ??= {
  activeBinding: void 0,
  activeCore: void 0,
  activeAgent: void 0,
  inputUnsub: void 0,
  activeRun: void 0,
  pendingSwitch: void 0,
  listenerStatus: "offline",
  listenerGeneration: 0,
  reconnectAttempts: 0,
  reconnectTimer: void 0,
  lastListenerError: void 0
};
function cloudNovaExtension(pi) {
  pi.registerMessageRenderer(MESSAGE_TYPE, (message, _options, theme) => ({
    render(width) {
      const details = message.details;
      const color = details?.level === "error" ? "error" : "dim";
      return message.content.split("\n").map((line) => theme.fg(color, truncate(line, width)));
    },
    invalidate() {
    }
  }));
  pi.registerCommand("cloudnova", {
    description: "CloudNova help and subcommands",
    getArgumentCompletions: (prefix) => {
      const subcommands = [
        { value: "help", label: "help", description: "Show CloudNova command help" },
        { value: "status", label: "status", description: "Show login/session status" },
        { value: "login ", label: "login", description: "Log in with a workspace token" },
        { value: "agents", label: "agents", description: "List workspace agents" },
        { value: "connect ", label: "connect", description: "Connect this Pi session as an agent" },
        { value: "disconnect", label: "disconnect", description: "Disconnect this Pi session" },
        { value: "logout", label: "logout", description: "Remove stored token" }
      ];
      const filtered = subcommands.filter((item) => item.label.startsWith(prefix));
      return filtered.length > 0 ? filtered : null;
    },
    handler: async (args, ctx) => {
      const [subcommand, ...rest] = splitArgs(args);
      if (!subcommand || subcommand === "help") {
        showHelp(pi, ctx);
        return;
      }
      await runSubcommand(pi, ctx, subcommand, rest.join(" "));
    }
  });
  pi.on("session_start", async (_event, ctx) => {
    state.activeBinding = restoreBinding(ctx);
    if (state.activeBinding) await reconnectListener(createPiSession(pi, ctx), state.activeBinding);
    await updateStatus(ctx);
  });
  pi.on("session_shutdown", async (_event, ctx) => {
    await stopListener();
    ctx.ui.setStatus("cloudnova", void 0);
  });
  pi.on("input", async (event, ctx) => {
    await mirrorLocalInput(ctx, event);
  });
  pi.on("message_update", async (event) => {
    if (!state.activeRun) return;
    const delta = extractTextDelta(event);
    if (delta) state.activeRun.assistantText += delta;
  });
  pi.on("message_end", async (event, ctx) => {
    await mirrorAssistantMessage(ctx, event);
  });
  pi.on("tool_execution_start", async (event, ctx) => {
    await mirrorToolUse(ctx, event);
  });
  pi.on("tool_execution_end", async (event, ctx) => {
    await mirrorToolResult(ctx, event);
  });
  pi.on("agent_end", async (event, ctx) => {
    if (!state.activeRun || !state.activeAgent || !state.activeBinding) return;
    const run = state.activeRun;
    state.activeRun = void 0;
    const text = run.postedAssistantMessages === 0 ? activeRunText(run, event) : "";
    try {
      if (text.trim()) await state.activeAgent.threads.post(run.threadId, { type: "agent", text: text.trim() });
      await state.activeAgent.threads.post(run.threadId, {
        type: "result",
        text: "",
        sessionId: ctx.sessionManager.getSessionFile?.() ?? state.activeBinding.runnerId
      });
    } catch (err) {
      ctx.ui.notify(`CloudNova reply failed: ${formatError(err)}`, "error");
    }
  });
}
async function runSubcommand(pi, ctx, subcommand, args) {
  if (subcommand === "status") return showStatus(pi, ctx);
  if (subcommand === "login") return login(pi, ctx, args);
  if (subcommand === "logout") return logout(pi, ctx);
  if (subcommand === "agents") return listAgents(pi, ctx);
  if (subcommand === "connect") return connectAgent(pi, ctx, args);
  if (subcommand === "disconnect") return disconnectAgent(pi, ctx);
  show(pi, ctx, `Unknown CloudNova command: ${subcommand}

Use /cloudnova help.`, "error");
}
function showHelp(pi, ctx) {
  show(pi, ctx, [
    "CloudNova commands:",
    "",
    "/cloudnova status \u2014 show login/session status",
    "/cloudnova login <token> \u2014 log in with a workspace token",
    "/cloudnova agents \u2014 list workspace agents",
    "/cloudnova connect <agent-id> \u2014 connect this Pi session as an external agent",
    "/cloudnova disconnect \u2014 disconnect this Pi session from its agent",
    "/cloudnova logout \u2014 remove stored token"
  ].join("\n"), "info");
}
async function showStatus(pi, ctx) {
  state.activeBinding = restoreBinding(ctx) ?? state.activeBinding;
  const config = await loadConfig();
  if (!config) {
    show(pi, ctx, `CloudNova: not logged in
Config: ${getConfigPath()}`, "info");
    return;
  }
  const lines = [
    "CloudNova: logged in",
    `Workspace: ${config.workspaceId}`
  ];
  if (state.activeBinding && state.activeBinding.workspaceId === config.workspaceId) {
    lines.push(`Connected agent: ${state.activeBinding.agentId}`);
    lines.push(`Listener: ${listenerStatusLabel()}`);
    if (state.activeBinding.activeThreadId) lines.push(`Active thread: ${state.activeBinding.activeThreadId}`);
    lines.push(`Runner: ${state.activeBinding.runnerId}`);
    if (state.lastListenerError) lines.push(`Last listener error: ${state.lastListenerError}`);
  } else {
    lines.push("Connected agent: none");
  }
  show(pi, ctx, lines.join("\n"), "info");
}
async function login(pi, ctx, token) {
  const trimmed = token.trim();
  if (!trimmed) {
    show(pi, ctx, "Usage: /cloudnova login <token>", "error");
    return;
  }
  try {
    const config = await saveConfig(trimmed);
    const core = await AgentCore.connect({ token: trimmed });
    await core.disconnect();
    await updateStatus(ctx);
    show(pi, ctx, `CloudNova: logged in
Workspace: ${config.workspaceId}`, "info");
  } catch (err) {
    show(pi, ctx, `CloudNova login failed: ${formatError(err)}`, "error");
  }
}
async function logout(pi, ctx) {
  await stopListener();
  await deleteConfig();
  state.activeBinding = void 0;
  await updateStatus(ctx);
  show(pi, ctx, "CloudNova: logged out", "info");
}
async function listAgents(pi, ctx) {
  const config = await loadConfig();
  if (!config) {
    show(pi, ctx, "CloudNova: not logged in. Run /cloudnova login <token>.", "error");
    return;
  }
  try {
    const core = await AgentCore.connect({ token: config.token });
    const agents = await core.listAgents();
    await core.disconnect();
    const visible = agents.filter((agent) => agent.createdBy !== "system");
    if (visible.length === 0) {
      show(pi, ctx, "CloudNova: no agents found", "info");
      return;
    }
    const lines = visible.map((agent) => {
      const external = agent.external ? ` external:${agent.external.type}` : "";
      return `${agent.id} \u2014 ${agent.name}${external}`;
    });
    show(pi, ctx, `CloudNova agents:
${lines.join("\n")}`, "info");
  } catch (err) {
    show(pi, ctx, `CloudNova agents failed: ${formatError(err)}`, "error");
  }
}
async function connectAgent(pi, ctx, agentIdArg) {
  const agentId = agentIdArg.trim();
  if (!agentId) {
    show(pi, ctx, "Usage: /cloudnova connect <agent-id>", "error");
    return;
  }
  const config = await loadConfig();
  if (!config) {
    show(pi, ctx, "CloudNova: not logged in. Run /cloudnova login <token>.", "error");
    return;
  }
  try {
    const core = await AgentCore.connect({ token: config.token });
    let created = false;
    try {
      const agent = await core.getAgent(agentId);
      if (!agent.external) {
        await core.disconnect();
        show(pi, ctx, `Agent ${agentId} exists but is not external. Create a new agent ID for Pi or convert it later from an admin tool.`, "error");
        return;
      }
    } catch (err) {
      if (!isNotFoundError(err)) throw err;
      await core.connectAgent({
        id: agentId,
        name: humanizeAgentId(agentId),
        description: "Pi external agent",
        type: "pi"
      });
      created = true;
    }
    await core.disconnect();
    const binding = {
      workspaceId: config.workspaceId,
      agentId,
      runnerId: createRunnerId(agentId),
      connectedAt: (/* @__PURE__ */ new Date()).toISOString()
    };
    state.activeBinding = binding;
    pi.appendEntry(BINDING_ENTRY, binding);
    await reconnectListener(createPiSession(pi, ctx), binding);
    await updateStatus(ctx);
    show(pi, ctx, `CloudNova: ${created ? "created and connected" : "connected"}
Workspace: ${binding.workspaceId}
Agent: ${binding.agentId}
Runner: ${binding.runnerId}`, "info");
  } catch (err) {
    show(pi, ctx, `CloudNova connect failed: ${formatError(err)}`, "error");
  }
}
async function disconnectAgent(pi, ctx) {
  await stopListener();
  const previous = state.activeBinding ?? restoreBinding(ctx);
  if (previous) {
    pi.appendEntry(BINDING_ENTRY, { ...previous, disconnectedAt: (/* @__PURE__ */ new Date()).toISOString() });
  }
  state.activeBinding = void 0;
  await updateStatus(ctx);
  show(pi, ctx, "CloudNova: disconnected", "info");
}
function createPiSession(pi, ctx) {
  return {
    ctx,
    appendEntry: (customType, data) => pi.appendEntry(customType, data),
    sendUserMessage: (content) => pi.sendUserMessage(content),
    compact: (customInstructions) => compactSession(ctx, customInstructions)
  };
}
function safeNotify(ctx, message, level) {
  try {
    ctx.ui.notify(message, level);
  } catch {
  }
}
async function compactSession(ctx, customInstructions) {
  const compact = ctx.compact;
  if (!compact) throw new Error("Pi compact API is not available in this Pi version.");
  return new Promise((resolve, reject) => {
    compact({
      customInstructions,
      onComplete: () => resolve({ compacted: true }),
      onError: (error) => {
        const reason = error.message || String(error);
        if (isSkippableCompactionError(reason)) {
          resolve({ compacted: false, skippedReason: reason });
        } else {
          reject(error);
        }
      }
    });
  });
}
function isSkippableCompactionError(message) {
  return /nothing to compact|already compacted/i.test(message);
}
async function reconnectListener(session, binding, opts = {}) {
  clearReconnectTimer();
  state.listenerGeneration += 1;
  const generation = state.listenerGeneration;
  const previousCore = state.activeCore;
  state.inputUnsub?.();
  state.inputUnsub = void 0;
  state.activeBinding = binding;
  setListenerStatus(opts.isReconnect ? "reconnecting" : "connecting", session.ctx);
  const config = await loadConfig();
  if (!config || config.workspaceId !== binding.workspaceId) {
    state.activeAgent = void 0;
    state.activeCore = void 0;
    await previousCore?.disconnect().catch(() => {
    });
    state.lastListenerError = config ? "Stored token is for a different workspace." : "Not logged in.";
    setListenerStatus("offline", session.ctx);
    return;
  }
  try {
    const core = await AgentCore.connect({ token: config.token });
    if (generation !== state.listenerGeneration) {
      await core.disconnect().catch(() => {
      });
      return;
    }
    const agent = core.agent(binding.agentId);
    state.activeCore = core;
    state.activeAgent = agent;
    state.inputUnsub = agent.threads.onInput((threadId, event) => {
      void handleCloudNovaInput(session, threadId, event).catch((err) => {
        safeNotify(session.ctx, `CloudNova input failed: ${formatError(err)}`, "error");
      });
    }, {
      onError: (err) => {
        void handleListenerError(session, binding, generation, err);
      }
    });
    state.reconnectAttempts = 0;
    state.lastListenerError = void 0;
    await previousCore?.disconnect().catch(() => {
    });
    setListenerStatus("online", session.ctx);
  } catch (err) {
    if (generation !== state.listenerGeneration) return;
    state.lastListenerError = formatError(err);
    setListenerStatus("reconnecting", session.ctx);
    scheduleReconnect(session, binding);
  }
}
async function handleListenerError(session, binding, generation, err) {
  if (generation !== state.listenerGeneration) return;
  state.lastListenerError = formatError(err);
  state.inputUnsub?.();
  state.inputUnsub = void 0;
  setListenerStatus("reconnecting", session.ctx);
  scheduleReconnect(session, binding);
}
function scheduleReconnect(session, binding) {
  if (!state.activeBinding || state.activeBinding.workspaceId !== binding.workspaceId || state.activeBinding.agentId !== binding.agentId) return;
  clearReconnectTimer();
  state.reconnectAttempts += 1;
  const delay = reconnectDelayMs(state.reconnectAttempts);
  state.reconnectTimer = setTimeout(() => {
    state.reconnectTimer = void 0;
    void reconnectListener(session, binding, { isReconnect: true }).catch((err) => {
      state.lastListenerError = formatError(err);
      setListenerStatus("reconnecting", session.ctx);
      scheduleReconnect(session, binding);
    });
  }, delay);
  void updateStatus(session.ctx);
}
async function stopListener() {
  clearReconnectTimer();
  state.listenerGeneration += 1;
  state.inputUnsub?.();
  state.inputUnsub = void 0;
  state.activeAgent = void 0;
  state.activeRun = void 0;
  state.reconnectAttempts = 0;
  state.lastListenerError = void 0;
  if (state.activeCore) {
    await state.activeCore.disconnect().catch(() => {
    });
    state.activeCore = void 0;
  }
  state.listenerStatus = "offline";
}
function clearReconnectTimer() {
  if (!state.reconnectTimer) return;
  clearTimeout(state.reconnectTimer);
  state.reconnectTimer = void 0;
}
function reconnectDelayMs(attempt) {
  return Math.min(MAX_RECONNECT_DELAY_MS, INITIAL_RECONNECT_DELAY_MS * 2 ** Math.max(0, attempt - 1));
}
function setListenerStatus(status, ctx) {
  state.listenerStatus = status;
  void updateStatus(ctx);
}
function listenerStatusLabel() {
  if (state.listenerStatus === "reconnecting" && state.reconnectAttempts > 0) {
    return `reconnecting (attempt ${state.reconnectAttempts})`;
  }
  return state.listenerStatus;
}
async function handleCloudNovaInput(session, threadId, event) {
  const { ctx } = session;
  if (!state.activeBinding || !state.activeAgent || event.silent) return;
  if (state.activeRun) {
    await state.activeAgent.threads.post(threadId, {
      type: "error",
      text: state.activeRun.threadId === threadId ? "Pi runner is still handling the previous message. Try again when it finishes." : "Pi runner is busy with another CloudNova thread. Try again later."
    });
    return;
  }
  if (state.activeBinding.activeThreadId && state.activeBinding.activeThreadId !== threadId) {
    if (!ctx.isIdle()) {
      await state.activeAgent.threads.post(threadId, {
        type: "error",
        text: "Pi runner is busy with another CloudNova thread. Try again later."
      });
      return;
    }
    await switchToThread(session, threadId, event);
    return;
  }
  if (!ctx.isIdle()) {
    await state.activeAgent.threads.post(threadId, { type: "error", text: "Pi runner is busy. Try again later." });
    return;
  }
  if (!state.activeBinding.activeThreadId) {
    state.activeBinding = { ...state.activeBinding, activeThreadId: threadId };
    session.appendEntry(BINDING_ENTRY, state.activeBinding);
    await updateStatus(ctx);
  }
  state.activeRun = createActiveRun(threadId);
  await session.sendUserMessage(formatCloudInput(event));
}
async function mirrorLocalInput(ctx, event) {
  if (!state.activeBinding?.activeThreadId || !state.activeAgent || state.activeRun || state.pendingSwitch) return;
  const input = event;
  if (input.source === "extension" || typeof input.text !== "string" || !input.text.trim()) return;
  const threadId = state.activeBinding.activeThreadId;
  state.activeRun = createActiveRun(threadId);
  try {
    await state.activeAgent.threads.post(threadId, {
      type: "input",
      text: input.text.trim(),
      from: "Pi",
      silent: true
    });
  } catch (err) {
    state.activeRun = void 0;
    ctx.ui.notify(`CloudNova input mirror failed: ${formatError(err)}`, "error");
  }
}
async function mirrorAssistantMessage(ctx, event) {
  if (!state.activeRun || !state.activeAgent) return;
  const message = event.message;
  const text = extractAssistantText(message);
  if (!text.trim()) return;
  try {
    await state.activeAgent.threads.post(state.activeRun.threadId, { type: "agent", text: text.trim() });
    state.activeRun.postedAssistantMessages += 1;
  } catch (err) {
    ctx.ui.notify(`CloudNova assistant mirror failed: ${formatError(err)}`, "error");
  }
}
async function mirrorToolUse(ctx, event) {
  if (!state.activeRun || !state.activeAgent) return;
  const tool = event;
  if (typeof tool.toolCallId !== "string" || typeof tool.toolName !== "string") return;
  const content = tool.args === void 0 ? {} : isRecord(tool.args) ? sanitizeForFirestore(tool.args) : { args: sanitizeForFirestore(tool.args) };
  try {
    await state.activeAgent.threads.post(state.activeRun.threadId, {
      type: "tool_use",
      toolUseId: tool.toolCallId,
      name: tool.toolName,
      content,
      summary: summarizeToolUse(tool.toolName, content)
    });
  } catch (err) {
    ctx.ui.notify(`CloudNova tool mirror failed: ${formatError(err)}`, "error");
  }
}
async function mirrorToolResult(ctx, event) {
  if (!state.activeRun || !state.activeAgent) return;
  const tool = event;
  if (typeof tool.toolCallId !== "string") return;
  try {
    await state.activeAgent.threads.post(state.activeRun.threadId, {
      type: "tool_result",
      toolUseId: tool.toolCallId,
      content: sanitizeForFirestore(tool.result ?? null),
      summary: summarizeToolResult(tool.result, tool.isError === true)
    });
  } catch (err) {
    ctx.ui.notify(`CloudNova tool result mirror failed: ${formatError(err)}`, "error");
  }
}
async function switchToThread(session, threadId, event) {
  const { ctx } = session;
  if (!state.activeBinding || !state.activeAgent) return;
  if (state.pendingSwitch) {
    await state.activeAgent.threads.post(threadId, {
      type: "error",
      text: "Pi runner is already switching CloudNova threads. Try again shortly."
    });
    return;
  }
  const binding = { ...state.activeBinding, activeThreadId: threadId, connectedAt: (/* @__PURE__ */ new Date()).toISOString() };
  state.pendingSwitch = { threadId };
  try {
    const compactResult = await session.compact(THREAD_SWITCH_COMPACTION_PROMPT);
    const action = compactResult.compacted ? "soft-cleared" : `soft-clear skipped (${compactResult.skippedReason ?? "not available"})`;
    safeNotify(ctx, `CloudNova switched thread: ${action}.`, "info");
    state.activeBinding = binding;
    session.appendEntry(BINDING_ENTRY, binding);
    await updateStatus(ctx);
    state.activeRun = createActiveRun(threadId);
    await session.sendUserMessage(formatCloudInput(event, { threadSwitch: true }));
  } catch (err) {
    state.activeRun = void 0;
    safeNotify(ctx, `CloudNova thread switch failed: ${formatError(err)}`, "error");
    await state.activeAgent?.threads.post(threadId, { type: "error", text: `Pi runner could not switch CloudNova threads: ${formatError(err)}` }).catch(() => {
    });
  } finally {
    state.pendingSwitch = void 0;
  }
}
function createActiveRun(threadId) {
  return { threadId, assistantText: "", postedAssistantMessages: 0 };
}
function formatCloudInput(event, opts = {}) {
  const parts = ["[CloudNova message]"];
  if (event.from) parts.push(`From: ${event.from}`);
  if (opts.threadSwitch) {
    parts.push("", "[CloudNova thread switch] Treat prior CloudNova conversation in this Pi session as closed. Use only durable workspace state that remains relevant.");
  }
  parts.push("", event.text);
  return parts.join("\n");
}
function extractTextDelta(event) {
  const maybe = event;
  return maybe.assistantMessageEvent?.type === "text_delta" && typeof maybe.assistantMessageEvent.delta === "string" ? maybe.assistantMessageEvent.delta : void 0;
}
function activeRunText(run, event) {
  if (run.assistantText.trim()) return run.assistantText;
  const fallback = extractAssistantTextFromAgentEnd(event);
  return fallback ?? "";
}
function extractAssistantText(message) {
  const msg = message;
  if (msg.role !== "assistant") return "";
  return extractTextContent(msg.content);
}
function extractAssistantTextFromAgentEnd(event) {
  const messages = event.messages;
  if (!Array.isArray(messages)) return void 0;
  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i];
    if (msg.role !== "assistant") continue;
    const text = extractTextContent(msg.content);
    if (text) return text;
  }
  return void 0;
}
function restoreBinding(ctx) {
  let latest;
  for (const entry of ctx.sessionManager.getBranch()) {
    if (entry.type !== "custom" || entry.customType !== BINDING_ENTRY) continue;
    const data = entry.data;
    if (isBinding(data)) latest = data.disconnectedAt ? void 0 : data;
  }
  return latest;
}
async function updateStatus(ctx) {
  const config = await loadConfig();
  if (!config) {
    ctx.ui.setStatus("cloudnova", void 0);
    return;
  }
  const binding = state.activeBinding && state.activeBinding.workspaceId === config.workspaceId ? ` / ${state.activeBinding.agentId} / ${state.listenerStatus}` : "";
  ctx.ui.setStatus("cloudnova", ctx.ui.theme.fg("dim", `CloudNova: ${config.workspaceId}${binding}`));
}
function createRunnerId(agentId) {
  const host = os2.hostname().replace(/[^a-zA-Z0-9.-]/g, "-") || "local";
  const suffix = Math.random().toString(36).slice(2, 8);
  return `pi:${host}:${agentId}:${suffix}`;
}
function isBinding(value) {
  if (!value || typeof value !== "object") return false;
  const data = value;
  return typeof data.workspaceId === "string" && typeof data.agentId === "string" && typeof data.runnerId === "string" && typeof data.connectedAt === "string";
}
function splitArgs(args) {
  return (args ?? "").trim().split(/\s+/).filter(Boolean);
}
function extractTextContent(content) {
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return "";
  return content.map((block) => {
    if (!isRecord(block)) return "";
    if (typeof block.text === "string") return block.text;
    if (typeof block.content === "string") return block.content;
    return "";
  }).join("");
}
function summarizeToolUse(toolName, args) {
  const input = oneLine(JSON.stringify(args));
  return input ? `${toolName} ${input}` : toolName;
}
function summarizeToolResult(result, isError) {
  const text = oneLine(typeof result === "string" ? result : JSON.stringify(result));
  if (!text) return isError ? "error" : "done";
  return isError ? `error: ${text}` : text;
}
function oneLine(value) {
  return (value ?? "").replace(/\s+/g, " ").trim().slice(0, 240);
}
function sanitizeForFirestore(value) {
  if (value === void 0) return null;
  if (Array.isArray(value)) return value.map(sanitizeForFirestore);
  if (!isRecord(value)) return value;
  return Object.fromEntries(Object.entries(value).map(([key, item]) => [key, sanitizeForFirestore(item)]));
}
function isRecord(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}
function show(pi, ctx, content, level) {
  if (level === "error") ctx.ui.notify(content, level);
  pi.sendMessage({
    customType: MESSAGE_TYPE,
    content,
    display: true,
    details: { level, timestamp: Date.now() }
  });
}
function truncate(text, width) {
  if (width <= 0 || text.length <= width) return text;
  return text.slice(0, Math.max(0, width - 1)) + "\u2026";
}
function isNotFoundError(err) {
  return err instanceof Error && /not found/i.test(err.message);
}
function humanizeAgentId(agentId) {
  return agentId.split("-").filter(Boolean).map((part) => part.slice(0, 1).toUpperCase() + part.slice(1)).join(" ") || agentId;
}
function formatError(err) {
  return err instanceof Error ? err.message : String(err);
}
export {
  cloudNovaExtension as default
};
