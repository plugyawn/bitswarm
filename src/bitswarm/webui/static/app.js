const state = {
  selectedTransferId: null,
  log: [],
};

const $ = (selector) => document.querySelector(selector);

function log(message) {
  const line = `${new Date().toLocaleTimeString()}  ${message}`;
  state.log.unshift(line);
  state.log = state.log.slice(0, 80);
  $("#log").textContent = state.log.join("\n");
}

function formatBytes(value) {
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let size = value / 1024;
  for (const unit of units) {
    if (size < 1024) return `${size.toFixed(size < 10 ? 1 : 0)} ${unit}`;
    size /= 1024;
  }
  return `${size.toFixed(1)} PB`;
}

function formatRate(value) {
  return `${formatBytes(Math.max(0, value))}/s`;
}

function percent(value) {
  return `${Math.round(value * 100)}%`;
}

function escapeText(value) {
  return `${value ?? ""}`.replace(/[&<>"']/g, (char) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  }[char]));
}

function progress(value) {
  return `
    <div class="progress">
      <div class="bar"><div class="fill" style="width: ${Math.max(0, Math.min(100, value * 100))}%"></div></div>
      <span>${percent(value)}</span>
    </div>
  `;
}

async function api(path, options = {}) {
  const response = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    let detail = `${response.status} ${response.statusText}`;
    try {
      const payload = await response.json();
      detail = payload.detail ?? detail;
    } catch {
      // Keep HTTP detail.
    }
    throw new Error(detail);
  }
  return response.json();
}

function readForm(form) {
  return Object.fromEntries(new FormData(form).entries());
}

function cleanPayload(payload) {
  return Object.fromEntries(Object.entries(payload).filter(([, value]) => value !== ""));
}

async function submitDownload(event) {
  event.preventDefault();
  const raw = readForm(event.currentTarget);
  const peers = raw.peers
    ? raw.peers.split(",").map((item) => item.trim()).filter(Boolean)
    : [];
  const payload = cleanPayload({
    manifest_path: raw.manifest_path,
    output_path: raw.output_path,
    peers,
    tracker_url: raw.tracker_url,
    token: raw.token,
    auto_start: true,
  });
  try {
    const transfer = await api("/api/ui/transfers/download", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    state.selectedTransferId = transfer.transfer_id;
    log(`download started: ${transfer.name}`);
    event.currentTarget.reset();
    await refresh();
  } catch (error) {
    log(`download failed: ${error.message}`);
  }
}

async function submitSeed(event) {
  event.preventDefault();
  const raw = readForm(event.currentTarget);
  const payload = cleanPayload({
    root_path: raw.root_path,
    manifest_path: raw.manifest_path,
    name: raw.name,
  });
  if (raw.piece_size) payload.piece_size = Number(raw.piece_size);
  try {
    const seed = await api("/api/ui/seeds", {
      method: "POST",
      body: JSON.stringify(payload),
    });
    log(`seeding ${seed.name}; peer origin ${window.location.origin}`);
    event.currentTarget.reset();
    await refresh();
  } catch (error) {
    log(`seed failed: ${error.message}`);
  }
}

async function cancelTransfer(id) {
  try {
    await api(`/api/ui/transfers/${id}/cancel`, { method: "POST", body: "{}" });
    log(`cancelled ${id}`);
    await refresh();
  } catch (error) {
    log(`cancel failed: ${error.message}`);
  }
}

function renderTransfers(transfers) {
  $("#transfer-count").textContent = `${transfers.length} item${transfers.length === 1 ? "" : "s"}`;
  $("#transfer-rows").innerHTML = transfers.map((transfer) => `
    <tr class="${transfer.transfer_id === state.selectedTransferId ? "selected" : ""}" data-transfer-id="${transfer.transfer_id}">
      <td>
        <strong>${escapeText(transfer.name)}</strong><br />
        <small>${escapeText(transfer.manifest_id)}</small>
      </td>
      <td><span class="status ${transfer.status}">${transfer.status}</span></td>
      <td>${progress(transfer.progress)}</td>
      <td>${transfer.completed_pieces}/${transfer.total_pieces}</td>
      <td>${transfer.peer_count}</td>
      <td>${formatRate(transfer.down_bps)}</td>
      <td>${transfer.status === "downloading" ? `<button data-cancel="${transfer.transfer_id}">Stop</button>` : ""}</td>
    </tr>
  `).join("");

  document.querySelectorAll("[data-transfer-id]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedTransferId = row.dataset.transferId;
      renderDetails(transfers.find((item) => item.transfer_id === state.selectedTransferId));
      renderTransfers(transfers);
    });
  });
  document.querySelectorAll("[data-cancel]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      cancelTransfer(button.dataset.cancel);
    });
  });
}

function renderSeeds(seeds) {
  $("#seed-count").textContent = `${seeds.length} item${seeds.length === 1 ? "" : "s"}`;
  $("#seed-rows").innerHTML = seeds.map((seed) => `
    <tr>
      <td>
        <strong>${escapeText(seed.name)}</strong><br />
        <small>${escapeText(seed.root_path)}</small>
      </td>
      <td><span class="status ${seed.status}">${seed.status}</span></td>
      <td>${seed.total_pieces}</td>
      <td>${formatBytes(seed.total_bytes)}</td>
      <td><small>${escapeText(seed.manifest_id)}</small></td>
    </tr>
  `).join("");
}

function renderDetails(transfer) {
  if (!transfer) {
    $("#selected-id").textContent = "none";
    $("#detail-empty").classList.remove("hidden");
    $("#detail-view").classList.add("hidden");
    return;
  }
  $("#selected-id").textContent = transfer.transfer_id;
  $("#detail-empty").classList.add("hidden");
  $("#detail-view").classList.remove("hidden");
  $("#detail-output").textContent = transfer.output_path;
  $("#detail-manifest").textContent = transfer.manifest_id;
  $("#file-list").innerHTML = transfer.files.map((file) => `
    <div class="file-row">
      <header>
        <strong>${escapeText(file.path)}</strong>
        <span>${formatBytes(file.size)} ${percent(file.progress)}</span>
      </header>
      ${progress(file.progress)}
    </div>
  `).join("");
  $("#piece-grid").innerHTML = transfer.pieces.map((piece) => `
    <div class="piece ${piece.status}" title="${escapeText(piece.piece_id)} ${formatBytes(piece.size)}"></div>
  `).join("");
}

function renderStats(snapshot) {
  const active = snapshot.transfers.filter((item) => item.status === "downloading").length;
  const seeding = snapshot.seeds.filter((item) => item.status === "seeding").length;
  const rate = snapshot.transfers.reduce((total, item) => total + item.down_bps, 0);
  $("#stat-active").textContent = active;
  $("#stat-seeding").textContent = seeding;
  $("#stat-rate").textContent = formatRate(rate);
}

async function refresh() {
  try {
    const snapshot = await api("/api/ui/state");
    renderStats(snapshot);
    renderTransfers(snapshot.transfers);
    renderSeeds(snapshot.seeds);
    const selected = snapshot.transfers.find((item) => item.transfer_id === state.selectedTransferId);
    if (!selected && snapshot.transfers.length > 0 && state.selectedTransferId === null) {
      state.selectedTransferId = snapshot.transfers[0].transfer_id;
    }
    renderDetails(snapshot.transfers.find((item) => item.transfer_id === state.selectedTransferId));
  } catch (error) {
    log(`refresh failed: ${error.message}`);
  }
}

function wireNavigation() {
  document.querySelectorAll(".nav-item").forEach((button) => {
    button.addEventListener("click", () => {
      document.querySelectorAll(".nav-item").forEach((item) => item.classList.remove("active"));
      button.classList.add("active");
      const panel = button.dataset.panel;
      document.querySelector(`[data-panel-id="${panel}"]`)?.scrollIntoView({ behavior: "smooth", block: "start" });
    });
  });
}

function init() {
  $("#local-origin").textContent = window.location.origin;
  $("#download-form").addEventListener("submit", submitDownload);
  $("#seed-form").addEventListener("submit", submitSeed);
  $("#clear-log").addEventListener("click", () => {
    state.log = [];
    $("#log").textContent = "";
  });
  wireNavigation();
  refresh();
  setInterval(refresh, 1000);
}

init();
