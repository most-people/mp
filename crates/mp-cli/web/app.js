const state = {
  channels: [],
  active: null,
  messages: [],
  messageIds: new Set(),
  peers: new Set(),
  typingPeers: new Set(),
  socket: null,
  reconnectTimer: null,
  typingTimer: null,
  typingSent: false,
  toastTimer: null,
};

const elements = {
  socketState: document.querySelector("#socket-state"),
  channelList: document.querySelector("#channel-list"),
  channelTotal: document.querySelector("#channel-total"),
  activeName: document.querySelector("#active-name"),
  activeId: document.querySelector("#active-id"),
  presence: document.querySelector("#presence"),
  peerCount: document.querySelector("#peer-count"),
  messages: document.querySelector("#messages"),
  emptyState: document.querySelector("#empty-state"),
  typingLine: document.querySelector("#typing-line"),
  composer: document.querySelector("#composer"),
  messageInput: document.querySelector("#message-input"),
  sendMessage: document.querySelector("#send-message"),
  createDialog: document.querySelector("#create-dialog"),
  createForm: document.querySelector("#create-form"),
  channelName: document.querySelector("#channel-name"),
  createSubmit: document.querySelector("#create-submit"),
  joinDialog: document.querySelector("#join-dialog"),
  joinForm: document.querySelector("#join-form"),
  channelInvite: document.querySelector("#channel-invite"),
  joinSubmit: document.querySelector("#join-submit"),
  inviteDialog: document.querySelector("#invite-dialog"),
  inviteOutput: document.querySelector("#invite-output"),
  copyInvite: document.querySelector("#copy-invite"),
  copyInviteDialog: document.querySelector("#copy-invite-dialog"),
  toast: document.querySelector("#toast"),
};

async function api(path, options = {}) {
  const request = { ...options };
  request.headers = { Accept: "application/json", ...(options.headers || {}) };
  if (request.body !== undefined) {
    request.headers["Content-Type"] = "application/json";
  }
  const response = await fetch(path, request);
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : null;
  if (!response.ok) {
    throw new Error(payload?.error || `HTTP ${response.status}`);
  }
  return payload;
}

function shortKey(value, length = 10) {
  if (!value) return "";
  return `${value.slice(0, length)}…${value.slice(-4)}`;
}

function formatTime(timestamp) {
  return new Intl.DateTimeFormat("zh-CN", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(timestamp));
}

function setBusy(button, busy, idleLabel) {
  button.disabled = busy;
  button.textContent = busy ? "处理中" : idleLabel;
}

function showToast(message, isError = false) {
  clearTimeout(state.toastTimer);
  elements.toast.textContent = message;
  elements.toast.classList.toggle("error", isError);
  elements.toast.classList.add("visible");
  state.toastTimer = setTimeout(() => elements.toast.classList.remove("visible"), 3200);
}

function renderChannels() {
  elements.channelList.replaceChildren();
  elements.channelTotal.textContent = `${state.channels.length} channels`;
  if (state.channels.length === 0) {
    const empty = document.createElement("div");
    empty.className = "sidebar-empty";
    empty.textContent = "暂无本地频道";
    elements.channelList.append(empty);
    return;
  }

  for (const channel of state.channels) {
    const button = document.createElement("button");
    button.type = "button";
    button.className = "channel-item";
    if (state.active?.channelId === channel.id) button.classList.add("active");
    button.dataset.channelId = channel.id;

    const copy = document.createElement("span");
    copy.className = "channel-item-copy";
    const name = document.createElement("span");
    name.className = "channel-item-name";
    name.textContent = channel.name;
    const id = document.createElement("span");
    id.className = "channel-item-id";
    id.textContent = shortKey(channel.id, 8);
    copy.append(name, id);

    const count = document.createElement("span");
    count.className = "channel-item-count";
    count.textContent = String(channel.messageCount);
    button.append(copy, count);
    button.addEventListener("click", () => openChannel(channel.id));
    elements.channelList.append(button);
  }
}

function renderHeader() {
  const active = state.active;
  if (!active) {
    elements.activeName.textContent = "选择频道";
    elements.activeId.textContent = "未连接";
    elements.peerCount.textContent = "0 peers";
    elements.presence.classList.remove("online");
    elements.copyInvite.disabled = true;
    elements.messageInput.disabled = true;
    elements.sendMessage.disabled = true;
    return;
  }

  elements.activeName.textContent = active.name;
  elements.activeId.textContent = active.channelId;
  elements.copyInvite.disabled = false;
  elements.messageInput.disabled = false;
  elements.sendMessage.disabled = false;
  renderPresence();
}

function renderPresence() {
  const count = state.peers.size;
  elements.peerCount.textContent = `${count} ${count === 1 ? "peer" : "peers"}`;
  elements.presence.classList.toggle("online", count > 0);
}

function createMessageElement(message) {
  const local = message.writer === state.active?.publicKey;
  const row = document.createElement("article");
  row.className = `message${local ? " local" : ""}`;
  row.dataset.messageId = message.messageId;

  const avatar = document.createElement("div");
  avatar.className = "message-avatar";
  avatar.textContent = local ? "me" : message.writer.slice(0, 2);

  const body = document.createElement("div");
  body.className = "message-body";
  const meta = document.createElement("div");
  meta.className = "message-meta";
  const writer = document.createElement("span");
  writer.className = "message-writer";
  writer.textContent = local ? "你" : shortKey(message.writer, 12);
  writer.title = message.writer;
  const time = document.createElement("time");
  time.className = "message-time";
  time.dateTime = new Date(message.timestampMs).toISOString();
  time.textContent = formatTime(message.timestampMs);
  const sequence = document.createElement("span");
  sequence.className = "message-sequence";
  sequence.textContent = `#${message.sequence}`;
  meta.append(writer, time, sequence);

  const text = document.createElement("p");
  text.className = "message-text";
  text.textContent = message.text;
  body.append(meta, text);
  row.append(avatar, body);
  return row;
}

function renderMessages() {
  elements.messages.replaceChildren();
  const empty = document.createElement("div");
  empty.className = "empty-state";
  empty.id = "empty-state";
  const mark = document.createElement("div");
  mark.className = "empty-mark";
  mark.setAttribute("aria-hidden", "true");
  mark.textContent = "#";
  const label = document.createElement("strong");
  label.textContent = state.active ? "暂无本地消息" : "没有打开的频道";
  empty.append(mark, label);
  elements.emptyState = empty;

  if (state.messages.length === 0) {
    elements.messages.append(empty);
    return;
  }
  for (const message of state.messages) {
    elements.messages.append(createMessageElement(message));
  }
  elements.messages.scrollTop = elements.messages.scrollHeight;
}

function appendMessage(message) {
  if (state.messageIds.has(message.messageId)) return;
  state.messageIds.add(message.messageId);
  state.messages.push(message);
  if (state.messages.length === 1) elements.messages.replaceChildren();
  elements.messages.append(createMessageElement(message));
  elements.messages.scrollTop = elements.messages.scrollHeight;
  const channel = state.channels.find((item) => item.id === state.active?.channelId);
  if (channel) channel.messageCount += 1;
  renderChannels();
}

function renderTyping() {
  const peers = [...state.typingPeers];
  elements.typingLine.textContent = peers.length
    ? `${peers.map((peer) => shortKey(peer, 8)).join("、")} 正在输入`
    : "";
}

async function loadStatus() {
  const payload = await api("/api/status");
  state.channels = payload.channels;
  state.active = payload.active;
  state.peers = new Set(payload.active?.peers || []);
  renderChannels();
  renderHeader();
  if (state.active) await loadMessages(state.active.channelId);
  else {
    state.messages = [];
    state.messageIds = new Set();
    renderMessages();
  }
}

async function loadMessages(channelId) {
  const messages = await api(`/api/channels/${encodeURIComponent(channelId)}/messages`);
  if (state.active?.channelId !== channelId) return;
  state.messages = messages;
  state.messageIds = new Set(messages.map((message) => message.messageId));
  renderMessages();
}

async function openChannel(channelId) {
  if (state.active?.channelId === channelId) return;
  try {
    const payload = await api(`/api/channels/${encodeURIComponent(channelId)}/open`, {
      method: "POST",
    });
    state.active = payload.active;
    state.peers = new Set(payload.active.peers || []);
    state.typingPeers.clear();
    renderTyping();
    await loadStatus();
    elements.messageInput.focus();
  } catch (error) {
    showToast(error.message, true);
  }
}

async function copyActiveInvite() {
  if (!state.active) return;
  try {
    const payload = await api(
      `/api/channels/${encodeURIComponent(state.active.channelId)}/invite`,
    );
    await copyText(payload.invite);
  } catch (error) {
    showToast(error.message, true);
  }
}

async function copyText(value) {
  try {
    await navigator.clipboard.writeText(value);
  } catch {
    const textarea = document.createElement("textarea");
    textarea.value = value;
    textarea.className = "clipboard-fallback";
    document.body.append(textarea);
    textarea.select();
    document.execCommand("copy");
    textarea.remove();
  }
  showToast("邀请已复制");
}

function connectEvents() {
  clearTimeout(state.reconnectTimer);
  if (state.socket) state.socket.close();
  const protocol = location.protocol === "https:" ? "wss:" : "ws:";
  const socket = new WebSocket(`${protocol}//${location.host}/api/events`);
  state.socket = socket;

  socket.addEventListener("open", () => {
    elements.socketState.classList.add("connected");
  });
  socket.addEventListener("close", () => {
    elements.socketState.classList.remove("connected");
    if (state.socket === socket) {
      state.reconnectTimer = setTimeout(connectEvents, 1500);
    }
  });
  socket.addEventListener("message", (message) => {
    try {
      handleSocketPayload(JSON.parse(message.data));
    } catch {
      showToast("事件数据无效", true);
    }
  });
}

function handleSocketPayload(payload) {
  if (payload.type === "snapshot") {
    if (payload.active) {
      state.active = payload.active;
      state.peers = new Set(payload.active.peers || []);
      renderHeader();
      renderChannels();
    }
    return;
  }
  if (payload.type !== "channel_event" || payload.channelId !== state.active?.channelId) return;
  const event = payload.event;
  if (event.type === "presence") {
    if (event.online) state.peers.add(event.peer);
    else {
      state.peers.delete(event.peer);
      state.typingPeers.delete(event.peer);
      renderTyping();
    }
    renderPresence();
    return;
  }
  if (event.type === "typing") {
    if (event.active) state.typingPeers.add(event.peer);
    else state.typingPeers.delete(event.peer);
    renderTyping();
    return;
  }
  if (event.type === "text") {
    appendMessage({
      messageId: event.message_id,
      writer: event.writer,
      sequence: event.sequence,
      timestampMs: event.timestamp_ms,
      text: event.text,
    });
  }
}

async function setTyping(active) {
  if (!state.active || state.typingSent === active) return;
  state.typingSent = active;
  try {
    await api("/api/typing", {
      method: "POST",
      body: JSON.stringify({ active }),
    });
  } catch {
    state.typingSent = false;
  }
}

document.querySelector("#create-channel").addEventListener("click", () => {
  elements.createForm.reset();
  elements.createDialog.showModal();
  elements.channelName.focus();
});

document.querySelector("#join-channel").addEventListener("click", () => {
  elements.joinForm.reset();
  elements.joinDialog.showModal();
  elements.channelInvite.focus();
});

for (const button of document.querySelectorAll("[data-close]")) {
  button.addEventListener("click", () => document.querySelector(`#${button.dataset.close}`).close());
}

elements.createForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  setBusy(elements.createSubmit, true, "创建");
  try {
    const payload = await api("/api/channels", {
      method: "POST",
      body: JSON.stringify({ name: elements.channelName.value }),
    });
    elements.createDialog.close();
    state.active = payload.active;
    state.peers = new Set(payload.active.peers || []);
    elements.inviteOutput.value = payload.invite;
    elements.inviteDialog.showModal();
    await loadStatus();
  } catch (error) {
    showToast(error.message, true);
  } finally {
    setBusy(elements.createSubmit, false, "创建");
  }
});

elements.joinForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  setBusy(elements.joinSubmit, true, "加入");
  try {
    const payload = await api("/api/channels/join", {
      method: "POST",
      body: JSON.stringify({ invite: elements.channelInvite.value.trim() }),
    });
    elements.joinDialog.close();
    state.active = payload.active;
    state.peers = new Set(payload.active.peers || []);
    await loadStatus();
    elements.messageInput.focus();
  } catch (error) {
    showToast(error.message, true);
  } finally {
    setBusy(elements.joinSubmit, false, "加入");
  }
});

elements.copyInvite.addEventListener("click", copyActiveInvite);
elements.copyInviteDialog.addEventListener("click", () => copyText(elements.inviteOutput.value));

elements.composer.addEventListener("submit", async (event) => {
  event.preventDefault();
  const text = elements.messageInput.value;
  if (!text.trim()) return;
  elements.sendMessage.disabled = true;
  elements.messageInput.value = "";
  clearTimeout(state.typingTimer);
  setTyping(false);
  try {
    await api("/api/messages", {
      method: "POST",
      body: JSON.stringify({ text }),
    });
  } catch (error) {
    elements.messageInput.value = text;
    showToast(error.message, true);
  } finally {
    elements.sendMessage.disabled = !state.active;
    elements.messageInput.focus();
  }
});

elements.messageInput.addEventListener("keydown", (event) => {
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    elements.composer.requestSubmit();
  }
});

elements.messageInput.addEventListener("input", () => {
  clearTimeout(state.typingTimer);
  if (elements.messageInput.value) {
    setTyping(true);
    state.typingTimer = setTimeout(() => setTyping(false), 900);
  } else {
    setTyping(false);
  }
});

window.addEventListener("beforeunload", () => {
  if (state.typingSent) {
    navigator.sendBeacon(
      "/api/typing",
      new Blob([JSON.stringify({ active: false })], { type: "application/json" }),
    );
  }
});

connectEvents();
loadStatus().catch((error) => showToast(error.message, true));
