const form = document.getElementById("submit-form");
const submitBtn = document.getElementById("submit-btn");
const previewBtn = document.getElementById("preview-btn");
const responseBox = document.getElementById("response-box");
const responseMeta = document.getElementById("response-meta");
const responseRaw = document.getElementById("response-raw");
const streamStatus = document.getElementById("stream-status");
const statusBox = document.getElementById("status-box");

const video = document.getElementById("video");
const alertsMeta = document.getElementById("alerts-meta");
const alertsList = document.getElementById("alerts-list");

const queryForm = document.getElementById("query-form");
const queryBtn = document.getElementById("query-btn");
const queryTextInput = document.getElementById("query-text");
const queryCameraInput = document.getElementById("query-camera-id");
const queryMeta = document.getElementById("query-meta");
const queryResults = document.getElementById("query-results");

const rtspInput = document.getElementById("rtsp-url");
const hlsInput = document.getElementById("hls-url");
const cameraInput = document.getElementById("camera-id");
const currentSource = document.getElementById("current-source");
const activityLog = document.getElementById("activity-log");
const themeToggle = document.getElementById("theme-toggle");
const dashboardLayout = document.getElementById("dashboard-layout");
const sidebarToggle = document.getElementById("sidebar-toggle");
const sidebarToggleArrow = document.querySelector("#sidebar-toggle .toggle-arrow");
const cameraList = document.getElementById("camera-list");
const addCameraForm = document.getElementById("add-camera-form");
const newCameraIdInput = document.getElementById("new-camera-id");
const newCameraNameInput = document.getElementById("new-camera-name");
const newCameraRtspInput = document.getElementById("new-camera-rtsp");
const newCameraHlsInput = document.getElementById("new-camera-hls");

const tabButtons = Array.from(document.querySelectorAll(".tab-btn"));
const screens = Array.from(document.querySelectorAll(".screen"));

let hlsInstance = null;
let alertsPollHandle = null;
const seenAlertKeys = new Set();
const THEME_KEY = "vigilens-theme";
const CUSTOM_PRESETS_KEY = "vigilens-custom-camera-presets";

function setSidebarCollapsed(collapsed) {
  if (!dashboardLayout || !sidebarToggle) return;
  dashboardLayout.classList.toggle("sidebar-collapsed", collapsed);
  sidebarToggle.setAttribute("aria-expanded", String(!collapsed));
  sidebarToggle.setAttribute("aria-label", collapsed ? "Open camera sidebar" : "Collapse camera sidebar");
  if (sidebarToggleArrow) {
    sidebarToggleArrow.textContent = collapsed ? "›" : "‹";
  }
}

function applyCameraPreset(button) {
  if (!button) return;

  document.querySelectorAll(".camera-preset").forEach((item) => item.classList.remove("is-active"));
  button.classList.add("is-active");

  const cameraId = button.dataset.cameraId || "";
  const streamName = button.dataset.streamName || "";
  const rtspUrl = button.dataset.rtspUrl || "";
  const hlsUrl = button.dataset.hlsUrl || "";

  if (cameraId) {
    cameraInput.value = cameraId;
    queryCameraInput.value = cameraId;
    currentSource.textContent = cameraId;
  }
  if (streamName) {
    document.getElementById("stream-name").value = streamName;
  }
  if (rtspUrl) {
    rtspInput.value = rtspUrl;
  }
  if (hlsUrl) {
    hlsInput.value = hlsUrl;
  }

  addActivity(`Preset selected: ${cameraId || "camera"}.`);
}

function normalizeStreamName(name, fallbackCameraId) {
  const source = (name || fallbackCameraId || "camera").trim().toLowerCase();
  const sanitized = source.replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "");
  return sanitized || "camera";
}

function createPresetListItem(preset, isCustom = false) {
  if (!cameraList) return null;

  const li = document.createElement("li");
  const button = document.createElement("button");
  button.type = "button";
  button.className = "camera-preset";
  button.dataset.cameraId = preset.cameraId;
  button.dataset.streamName = preset.streamName;
  button.dataset.rtspUrl = preset.rtspUrl;
  button.dataset.hlsUrl = preset.hlsUrl;
  if (isCustom) {
    button.dataset.custom = "true";
  }

  const idSpan = document.createElement("span");
  idSpan.className = "preset-id";
  idSpan.textContent = preset.cameraId;

  const nameSpan = document.createElement("span");
  nameSpan.className = "preset-name";
  nameSpan.textContent = preset.name;

  button.appendChild(idSpan);
  button.appendChild(nameSpan);
  li.appendChild(button);
  return li;
}

function getCustomPresetPayload() {
  return Array.from(document.querySelectorAll('.camera-preset[data-custom="true"]')).map((button) => ({
    cameraId: button.dataset.cameraId || "",
    name: button.querySelector(".preset-name")?.textContent || button.dataset.cameraId || "Custom Camera",
    streamName: button.dataset.streamName || "",
    rtspUrl: button.dataset.rtspUrl || "",
    hlsUrl: button.dataset.hlsUrl || "",
  }));
}

function persistCustomPresets() {
  const payload = getCustomPresetPayload();
  localStorage.setItem(CUSTOM_PRESETS_KEY, JSON.stringify(payload));
}

function loadCustomPresets() {
  if (!cameraList) return;

  const raw = localStorage.getItem(CUSTOM_PRESETS_KEY);
  if (!raw) return;

  let presets;
  try {
    presets = JSON.parse(raw);
  } catch {
    return;
  }

  if (!Array.isArray(presets)) return;

  presets.forEach((preset) => {
    if (!preset?.cameraId || !preset?.rtspUrl) return;
    const exists = Array.from(document.querySelectorAll(".camera-preset")).some(
      (button) => (button.dataset.cameraId || "").toLowerCase() === String(preset.cameraId).toLowerCase()
    );
    if (exists) return;

    const streamName = preset.streamName || normalizeStreamName(preset.name, preset.cameraId);
    const hlsUrl = preset.hlsUrl || rtspToHls(preset.rtspUrl);
    const item = createPresetListItem(
      {
        cameraId: String(preset.cameraId),
        name: String(preset.name || preset.cameraId),
        streamName,
        rtspUrl: String(preset.rtspUrl),
        hlsUrl,
      },
      true
    );

    if (item) {
      cameraList.appendChild(item);
    }
  });
}

function applyTheme(theme) {
  document.body.dataset.theme = theme;
  if (themeToggle) {
    themeToggle.setAttribute(
      "aria-label",
      theme === "dark" ? "Switch to light mode" : "Switch to dark mode"
    );
    themeToggle.setAttribute(
      "title",
      theme === "dark" ? "Switch to light mode" : "Switch to dark mode"
    );
  }
}

function initTheme() {
  const savedTheme = localStorage.getItem(THEME_KEY);
  if (savedTheme === "dark" || savedTheme === "light") {
    applyTheme(savedTheme);
    return;
  }

  const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  applyTheme(prefersDark ? "dark" : "light");
}

function switchScreen(screenName) {
  tabButtons.forEach((btn) => {
    btn.classList.toggle("is-active", btn.dataset.screenTarget === screenName);
  });

  screens.forEach((screen) => {
    screen.classList.toggle("is-active", screen.dataset.screen === screenName);
  });
}

function addActivity(message) {
  if (!activityLog) return;
  const li = document.createElement("li");
  li.className = "activity-item";
  li.textContent = `${new Date().toLocaleTimeString()} - ${message}`;
  activityLog.prepend(li);

  while (activityLog.children.length > 40) {
    activityLog.removeChild(activityLog.lastChild);
  }
}

function rtspToHls(rtspUrl) {
  try {
    const parsed = new URL(rtspUrl);
    if (!parsed.pathname || parsed.pathname === "/") return hlsInput.value;
    return `http://localhost:8888${parsed.pathname}/index.m3u8`;
  } catch {
    return hlsInput.value;
  }
}

function setStatus(text, state = "ok") {
  streamStatus.textContent = text;
  if (statusBox) {
    statusBox.dataset.state = state;
  }
}

function renderResult({ message, meta, raw }) {
  responseBox.textContent = message;
  responseMeta.textContent = meta;
  responseRaw.textContent = typeof raw === "string" ? raw : JSON.stringify(raw, null, 2);
}

function toLocalTime(timestampValue) {
  const date = new Date(timestampValue);
  return Number.isNaN(date.getTime()) ? String(timestampValue || "") : date.toLocaleString();
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function getSeverityLevel(item) {
  const source = String(item?.source || "").toLowerCase();
  const summary = String(item?.summary || "").toLowerCase();
  const text = `${source} ${summary}`;

  if (/(fire|weapon|gun|explosion|critical|emergency|bleeding|attack)/.test(text)) {
    return "high";
  }

  if (/(fall|intrusion|breach|violence|unsafe|fight|trespass|panic)/.test(text)) {
    return "medium";
  }

  return "low";
}

function renderItems(target, items, emptyText) {
  const applySeverity = target?.id === "alerts-list";

  if (!items || items.length === 0) {
    target.innerHTML = `<li class="item-empty">${escapeHtml(emptyText)}</li>`;
    return;
  }

  target.innerHTML = items
    .map((item) => {
      const safeSummary = escapeHtml(item.summary || "No summary");
      const safeSource = escapeHtml(item.source || "event");
      const safeCamera = escapeHtml(item.camera_id || "unknown");
      const safeTime = escapeHtml(toLocalTime(item.timestamp));
      const safeClipUrl = escapeHtml(item.clip_url || "");
      const sourceTag = safeSource.toUpperCase();
      const severity = applySeverity ? getSeverityLevel(item) : "";
      const severityClass = severity ? ` severity-${severity}` : "";
      const severityBadge = severity
        ? `<span class="severity-badge">${escapeHtml(severity)}</span>`
        : "";

      return `
        <li class="item-card${severityClass}">
          <div class="item-title-row">
            <p class="item-title">[${sourceTag}] ${safeSummary}</p>
            ${severityBadge}
          </div>
          <p class="item-sub">${safeTime} | camera: ${safeCamera}</p>
          ${safeClipUrl ? `<a class="item-link" href="${safeClipUrl}" target="_blank" rel="noopener noreferrer">Open Clip</a>` : ""}
        </li>
      `;
    })
    .join("");
}

async function postJson(url, payload) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const text = await response.text();
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    body = text;
  }

  if (!response.ok) {
    throw new Error(typeof body === "string" ? body : JSON.stringify(body));
  }

  return body;
}

function getActiveCameraId() {
  const value = queryCameraInput.value.trim() || cameraInput.value.trim();
  return value || null;
}

async function refreshAlerts() {
  try {
    const payload = {
      query: "alert",
      camera_id: getActiveCameraId() || undefined,
    };

    const body = await postJson("/api/query", payload);
    const results = Array.isArray(body?.results) ? body.results : [];

    results.forEach((item) => {
      const key = `${item.timestamp}|${item.camera_id}|${item.summary}|${item.clip_url}`;
      if (!seenAlertKeys.has(key)) {
        seenAlertKeys.add(key);
      }
    });

    renderItems(alertsList, results.slice(0, 10), "No alerts yet.");
    alertsMeta.textContent = `Refresh ${new Date().toLocaleTimeString()}`;
  } catch (error) {
    alertsMeta.textContent = "Refresh issue";
    console.error("alert polling error", error);
  }
}

function startAlertsPolling() {
  if (alertsPollHandle) {
    clearInterval(alertsPollHandle);
  }
  refreshAlerts();
  alertsPollHandle = setInterval(refreshAlerts, 5000);
}

async function runQuery() {
  const query = queryTextInput.value.trim();
  if (!query) {
    queryMeta.textContent = "Type a question first.";
    return;
  }

  queryBtn.disabled = true;
  queryBtn.textContent = "Searching...";
  queryMeta.textContent = "Searching activity...";
  setStatus("Searching", "busy");

  try {
    const body = await postJson("/api/query", {
      query,
      camera_id: queryCameraInput.value.trim() || undefined,
    });

    const route = body?.route || "unknown";
    const results = Array.isArray(body?.results) ? body.results : [];
    queryMeta.textContent = `Found ${results.length} result(s) via ${route}.`;
    renderItems(queryResults, results, "No results for this query.");

    setStatus("Ready", "ok");
    addActivity(`Search completed. ${results.length} result(s) found.`);
  } catch (error) {
    queryMeta.textContent = "Search failed. Please try again.";
    renderItems(queryResults, [], "Search failed. Please check connection and retry.");
    setStatus("Search Error", "error");
    addActivity("Search failed due to a connection issue.");
    console.error("query error", error);
  } finally {
    queryBtn.disabled = false;
    queryBtn.textContent = "Run Query";
  }
}

function loadPreview(url) {
  if (hlsInstance) {
    hlsInstance.destroy();
    hlsInstance = null;
  }

  if (window.Hls && Hls.isSupported()) {
    hlsInstance = new Hls({
      maxBufferLength: 20,
      lowLatencyMode: false,
      liveSyncDurationCount: 4,
      liveMaxLatencyDurationCount: 12,
      backBufferLength: 30,
    });
    hlsInstance.loadSource(url);
    hlsInstance.attachMedia(video);

    hlsInstance.on(Hls.Events.MANIFEST_PARSED, () => {
      setStatus("Streaming", "ok");
    });

    hlsInstance.on(Hls.Events.ERROR, (_event, data) => {
      const { type, details, fatal } = data;

      if (details === "bufferStalledError") {
        setStatus("Buffering", "busy");

        if (video.buffered && video.buffered.length > 0) {
          const liveEdge = video.buffered.end(video.buffered.length - 1);
          const target = Math.max(liveEdge - 0.6, 0);
          if (Number.isFinite(target) && video.currentTime < target - 0.2) {
            video.currentTime = target;
          }
        }
        return;
      }

      if (!fatal) {
        setStatus("Recovering", "busy");
        return;
      }

      if (type === Hls.ErrorTypes.NETWORK_ERROR) {
        setStatus("Network Recovery", "busy");
        hlsInstance?.startLoad();
        return;
      }

      if (type === Hls.ErrorTypes.MEDIA_ERROR) {
        setStatus("Media Recovery", "busy");
        hlsInstance?.recoverMediaError();
        return;
      }

      console.error("HLS preview fatal error", data);
      setStatus("Playback Error", "error");
    });

    setStatus("Preview Ready", "ok");
    addActivity("Preview stream connected.");
    return;
  }

  video.src = url;
  setStatus("Preview Ready", "ok");
  addActivity("Preview loaded.");
}

function buildPayload() {
  const tenantId = document.getElementById("tenant-id").value.trim();
  const cameraId = cameraInput.value.trim();
  const streamName = document.getElementById("stream-name").value.trim();
  const query = document.getElementById("trigger-query").value.trim();

  const payload = {
    tenant_id: tenantId,
    camera_id: cameraId || undefined,
    name: streamName || undefined,
    rtsp_url: rtspInput.value.trim(),
    trigger_queries: [
      {
        query,
        threshold: Number(document.getElementById("threshold").value),
        alert_payload_description: `Vigilens alert for query: ${query}`,
      },
    ],
    chunk_seconds: Number(document.getElementById("chunk-seconds").value),
    fps: Number(document.getElementById("fps").value),
  };

  const webhookUrl = document.getElementById("webhook-url").value.trim();
  const discordWebhookUrl = document.getElementById("discord-webhook-url").value.trim();
  const webhookUrls = [webhookUrl, discordWebhookUrl].filter(Boolean);
  if (webhookUrls.length > 0) {
    payload.webhook_urls = webhookUrls;
  }

  Object.keys(payload).forEach((key) => {
    if (payload[key] === undefined || payload[key] === "") {
      delete payload[key];
    }
  });

  return payload;
}

rtspInput.addEventListener("blur", () => {
  hlsInput.value = rtspToHls(rtspInput.value.trim());
});

previewBtn.addEventListener("click", () => {
  const url = hlsInput.value.trim();
  if (!url) return;

  loadPreview(url);
  switchScreen("monitor");
  addActivity("Moved to camera view.");
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  const payload = buildPayload();

  submitBtn.disabled = true;
  submitBtn.textContent = "Saving...";
  renderResult({
    message: "Saving camera settings...",
    meta: "Please wait",
    raw: payload,
  });
  setStatus("Saving", "busy");
  addActivity("Camera setup submission started.");

  try {
    const response = await fetch("/api/streams/submit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    const text = await response.text();
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      body = text;
    }

    if (!response.ok) {
      renderResult({
        message: "Could not save settings.",
        meta: `HTTP ${response.status}`,
        raw: body,
      });
      setStatus("Save Failed", "error");
      addActivity(`Save failed with HTTP ${response.status}.`);
      return;
    }

    const streamId = body?.stream_id || body?.id || body?.stream?.stream_id;
    const details = streamId ? `Stream ID: ${streamId}` : "Request accepted.";

    renderResult({
      message: "Camera started successfully.",
      meta: details,
      raw: body,
    });

    queryCameraInput.value = payload.camera_id || queryCameraInput.value;
    hlsInput.value = rtspToHls(payload.rtsp_url);
    currentSource.textContent = payload.camera_id || "Unknown";

    loadPreview(hlsInput.value.trim());
    startAlertsPolling();
    setStatus("Running", "ok");
    addActivity(`Camera is now running. ${details}`);
    switchScreen("monitor");
  } catch (error) {
    renderResult({
      message: "Unable to start camera.",
      meta: "Network or server issue",
      raw: String(error),
    });
    setStatus("Start Error", "error");
    addActivity("Start failed because the server could not be reached.");
  } finally {
    submitBtn.disabled = false;
    submitBtn.textContent = "Save And Start";
  }
});

queryForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  await runQuery();
});

tabButtons.forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.dataset.screenTarget;
    if (!target) return;
    switchScreen(target);
  });
});

if (themeToggle) {
  themeToggle.addEventListener("click", () => {
    const currentTheme = document.body.dataset.theme === "dark" ? "dark" : "light";
    const nextTheme = currentTheme === "dark" ? "light" : "dark";
    applyTheme(nextTheme);
    localStorage.setItem(THEME_KEY, nextTheme);
    addActivity(`Theme changed to ${nextTheme} mode.`);
  });
}

cameraInput.addEventListener("input", () => {
  const value = cameraInput.value.trim();
  currentSource.textContent = value || "Unknown";
});

if (sidebarToggle) {
  sidebarToggle.addEventListener("click", () => {
    if (!dashboardLayout) return;
    const collapsed = dashboardLayout.classList.contains("sidebar-collapsed");
    setSidebarCollapsed(!collapsed);
  });
}

if (cameraList) {
  cameraList.addEventListener("click", (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    const button = target.closest(".camera-preset");
    if (!button || !cameraList.contains(button)) return;
    applyCameraPreset(button);
  });
}

if (addCameraForm) {
  addCameraForm.addEventListener("submit", (event) => {
    event.preventDefault();
    if (!cameraList || !newCameraIdInput || !newCameraNameInput || !newCameraRtspInput || !newCameraHlsInput) {
      return;
    }

    const cameraId = newCameraIdInput.value.trim();
    const name = newCameraNameInput.value.trim();
    const rtspUrl = newCameraRtspInput.value.trim();
    const hlsUrl = newCameraHlsInput.value.trim() || rtspToHls(rtspUrl);
    const streamName = normalizeStreamName(name, cameraId);

    if (!cameraId || !name || !rtspUrl) {
      addActivity("Could not add camera. Please fill all required fields.");
      return;
    }

    const duplicate = Array.from(document.querySelectorAll(".camera-preset")).some(
      (button) => (button.dataset.cameraId || "").toLowerCase() === cameraId.toLowerCase()
    );

    if (duplicate) {
      addActivity(`Camera ${cameraId} already exists.`);
      return;
    }

    const item = createPresetListItem(
      {
        cameraId,
        name,
        streamName,
        rtspUrl,
        hlsUrl,
      },
      true
    );

    if (!item) return;

    cameraList.appendChild(item);
    applyCameraPreset(item.querySelector(".camera-preset"));
    persistCustomPresets();
    addCameraForm.reset();
    addActivity(`Added new camera preset: ${cameraId}.`);
  });
}

loadCustomPresets();
loadPreview(hlsInput.value.trim());
startAlertsPolling();
initTheme();
setSidebarCollapsed(false);
setStatus("Ready", "ready");
addActivity("Welcome. Your dashboard is ready.");
