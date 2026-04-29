(function () {
  "use strict";

  var blockedPrefixes = ["#!/settings", "#!/status", "#!/debug"];

  function normalizeRoute() {
    var hash = window.location.hash || "";
    if (!hash || blockedPrefixes.some(function (prefix) { return hash.indexOf(prefix) === 0; })) {
      window.location.hash = "#!/downloading";
    }
  }

  function hideStockSettings() {
    document.body.classList.add("bitswarm-bridge");
    var selectors = [
      'a[href^="#!/settings"]',
      'a[href="#!/status"]',
      'a[href="#!/debug"]'
    ];
    selectors.forEach(function (selector) {
      Array.prototype.forEach.call(document.querySelectorAll(selector), function (node) {
        var li = node.closest("li");
        if (li) {
          li.style.display = "none";
        }
      });
    });
  }

  function percent(current, total) {
    if (!total || total <= 0) {
      return 0;
    }
    return Math.max(0, Math.min(100, Math.round((current / total) * 100)));
  }

  function escapeText(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function ensureTelemetryPanel() {
    var content = document.querySelector(".content-body");
    if (!content) {
      return null;
    }
    var panel = document.getElementById("bitswarm-telemetry-panel");
    if (panel) {
      return panel;
    }
    panel = document.createElement("section");
    panel.id = "bitswarm-telemetry-panel";
    panel.innerHTML = [
      '<div class="bs-telemetry-head">',
      '  <div>',
      '    <div class="bs-eyebrow">Bitswarm sidecar</div>',
      '    <h2 id="bs-telemetry-title">Bitswarm</h2>',
      '    <p id="bs-telemetry-subtitle">Waiting for workload telemetry.</p>',
      '  </div>',
      '  <div class="bs-status">',
      '    <span id="bs-telemetry-status">idle</span>',
      '    <strong id="bs-telemetry-phase">idle</strong>',
      '  </div>',
      '</div>',
      '<div id="bs-telemetry-metrics" class="bs-metrics"></div>',
      '<div id="bs-telemetry-progress" class="bs-progress-list"></div>',
      '<div class="bs-grid">',
      '  <div><h3>Members</h3><div id="bs-telemetry-members" class="bs-list"></div></div>',
      '  <div><h3>Streams</h3><div id="bs-telemetry-streams" class="bs-list"></div></div>',
      '  <div><h3>Events</h3><div id="bs-telemetry-events" class="bs-list"></div></div>',
      '</div>'
    ].join("");
    content.insertBefore(panel, content.firstChild);
    return panel;
  }

  function renderMetrics(metrics) {
    var root = document.getElementById("bs-telemetry-metrics");
    if (!root) {
      return;
    }
    root.innerHTML = (metrics || []).map(function (metric) {
      return '<div class="bs-metric"><span>' + escapeText(metric.label) + '</span><strong>' +
        escapeText(metric.value) + '</strong><small>' + escapeText(metric.detail || "") + '</small></div>';
    }).join("");
  }

  function renderProgress(progressRows) {
    var root = document.getElementById("bs-telemetry-progress");
    if (!root) {
      return;
    }
    root.innerHTML = (progressRows || []).map(function (row) {
      var pct = percent(Number(row.current || 0), Number(row.total || 0));
      return '<div class="bs-progress-row">' +
        '<div class="bs-progress-top"><strong>' + escapeText(row.label) + '</strong><span>' +
        escapeText(row.state) + ' · ' + escapeText(row.current) + '/' + escapeText(row.total) +
        ' ' + escapeText(row.unit || "") + ' · ' + pct + '%</span></div>' +
        '<div class="bs-bar"><div style="width:' + pct + '%"></div></div>' +
        '<p>' + escapeText(row.detail || row.rate || "") + '</p>' +
        '</div>';
    }).join("");
  }

  function renderMembers(rows) {
    var root = document.getElementById("bs-telemetry-members");
    if (!root) {
      return;
    }
    root.innerHTML = (rows || []).slice(0, 8).map(function (row) {
      var pct = row.total ? percent(Number(row.current || 0), Number(row.total || 0)) : null;
      return '<div class="bs-list-row"><strong>' + escapeText(row.label) + '</strong><span>' +
        escapeText(row.role || "") + ' · ' + escapeText(row.state) +
        (pct == null ? '' : ' · ' + pct + '%') + '</span><small>' +
        escapeText(row.detail || "") + '</small></div>';
    }).join("");
  }

  function renderStreams(rows) {
    var root = document.getElementById("bs-telemetry-streams");
    if (!root) {
      return;
    }
    root.innerHTML = (rows || []).slice(0, 6).map(function (row) {
      var pct = row.total ? percent(Number(row.current || 0), Number(row.total || 0)) : null;
      return '<div class="bs-list-row bs-stream"><strong>' + escapeText(row.label) + '</strong><span>' +
        escapeText(row.kind || "stream") + ' · ' + escapeText(row.state) +
        (pct == null ? '' : ' · ' + pct + '%') + ' · ' + escapeText(row.score || "") +
        '</span><small>' + escapeText(row.detail || "") + '</small>' +
        (row.prompt ? '<pre>Q: ' + escapeText(row.prompt) + '</pre>' : '') +
        (row.output ? '<pre>A: ' + escapeText(row.output) + '</pre>' : '') + '</div>';
    }).join("");
  }

  function renderEvents(rows) {
    var root = document.getElementById("bs-telemetry-events");
    if (!root) {
      return;
    }
    root.innerHTML = (rows || []).slice(-8).reverse().map(function (row) {
      var date = row.ts_ms ? new Date(row.ts_ms).toLocaleTimeString() : "";
      return '<div class="bs-event bs-' + escapeText(row.level || "info") + '"><span>' +
        escapeText(date) + '</span><strong>' + escapeText(row.level || "info") + '</strong><p>' +
        escapeText(row.message) + '</p></div>';
    }).join("");
  }

  function renderTelemetry(data) {
    var panel = ensureTelemetryPanel();
    if (!panel) {
      return;
    }
    panel.classList.toggle("is-disabled", !data.enabled);
    document.getElementById("bs-telemetry-title").textContent = data.title || "Bitswarm";
    document.getElementById("bs-telemetry-subtitle").textContent = data.subtitle || "";
    document.getElementById("bs-telemetry-status").textContent = data.status || "idle";
    document.getElementById("bs-telemetry-phase").textContent = data.phase || "idle";
    renderMetrics(data.metrics);
    renderProgress(data.progress);
    renderMembers(data.members);
    renderStreams(data.streams);
    renderEvents(data.events);
  }

  function pollTelemetry() {
    fetch("/api/bitswarm/ui/telemetry", { cache: "no-store" })
      .then(function (response) { return response.ok ? response.json() : null; })
      .then(function (data) {
        if (data) {
          renderTelemetry(data);
        }
      })
      .catch(function () {});
  }

  normalizeRoute();
  window.addEventListener("hashchange", normalizeRoute);
  document.addEventListener("DOMContentLoaded", function () {
    hideStockSettings();
    pollTelemetry();
    window.setInterval(hideStockSettings, 1000);
    window.setInterval(pollTelemetry, 1000);
  });
}());
