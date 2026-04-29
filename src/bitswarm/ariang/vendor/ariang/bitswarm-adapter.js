(function () {
  "use strict";

  var blockedPrefixes = ["#!/settings", "#!/status", "#!/debug"];
  var catalog = null;
  var runs = [];
  var refreshTimer = null;
  var expandedRuns = {};

  function normalizeRoute() {
    var hash = window.location.hash || "";
    if (!hash || blockedPrefixes.some(function (prefix) { return hash.indexOf(prefix) === 0; })) {
      window.location.hash = "#!/downloading";
    }
  }

  function getStoredActor() {
    var params = new URLSearchParams(window.location.search || "");
    var actor = (params.get("actor") || params.get("operator") || "").trim().toUpperCase();
    if (actor) {
      window.localStorage.setItem("bitswarm.operator", actor);
      return actor;
    }
    return (window.localStorage.getItem("bitswarm.operator") || "A").trim().toUpperCase();
  }

  function setStoredActor(actor) {
    window.localStorage.setItem("bitswarm.operator", actor);
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

  function ensureRunButtons() {
    var toolbar = document.querySelector(".main-header .navbar-toolbar .nav.navbar-nav");
    if (!toolbar || document.getElementById("bitswarm-runs-button")) {
      return;
    }
    var startItem = document.createElement("li");
    startItem.innerHTML = '<a id="bitswarm-start-run-button" class="toolbar pointer-cursor" title="Start Run">' +
      '<i class="fa fa-plus-circle"></i> <span>Start Run</span></a>';
    var runsItem = document.createElement("li");
    runsItem.innerHTML = '<a id="bitswarm-runs-button" class="toolbar pointer-cursor" title="Runs">' +
      '<i class="fa fa-list"></i> <span>Runs</span></a>';
    toolbar.insertBefore(runsItem, toolbar.firstChild);
    toolbar.insertBefore(startItem, toolbar.firstChild);
    document.getElementById("bitswarm-start-run-button").addEventListener("click", function (event) {
      event.preventDefault();
      showRunModal("start");
    });
    document.getElementById("bitswarm-runs-button").addEventListener("click", function (event) {
      event.preventDefault();
      showRunModal("runs");
    });
  }

  function ensureRunModal() {
    if (document.getElementById("bitswarm-run-modal")) {
      return;
    }
    var modal = document.createElement("div");
    modal.id = "bitswarm-run-modal";
    modal.className = "modal fade";
    modal.tabIndex = -1;
    modal.setAttribute("role", "dialog");
    modal.innerHTML = [
      '<div class="modal-dialog modal-lg" role="document">',
      '  <div class="modal-content">',
      '    <div class="modal-header">',
      '      <button type="button" class="close" data-dismiss="modal" aria-label="Close">',
      '        <span aria-hidden="true">&times;</span>',
      '      </button>',
      '      <h4 class="modal-title">Bitswarm Runs</h4>',
      '    </div>',
      '    <div class="modal-body">',
      '      <ul class="nav nav-tabs" role="tablist">',
      '        <li id="bitswarm-tab-start" role="presentation" class="active">',
      '          <a href="#bitswarm-run-start" aria-controls="bitswarm-run-start" role="tab" data-toggle="tab">Start Run</a>',
      '        </li>',
      '        <li id="bitswarm-tab-runs" role="presentation">',
      '          <a href="#bitswarm-run-list" aria-controls="bitswarm-run-list" role="tab" data-toggle="tab">Active Runs</a>',
      '        </li>',
      '      </ul>',
      '      <div class="tab-content bitswarm-run-tabs">',
      '        <div role="tabpanel" class="tab-pane active" id="bitswarm-run-start">',
      '          <form id="bitswarm-run-form">',
      '            <div class="row">',
      '              <div class="form-group col-sm-8">',
      '                <label for="bitswarm-run-name">Run Name</label>',
      '                <input id="bitswarm-run-name" class="form-control" value="RandOpt Testnet">',
      '              </div>',
      '              <div class="form-group col-sm-4">',
      '                <label for="bitswarm-actor">Operator</label>',
      '                <select id="bitswarm-actor" class="form-control"></select>',
      '              </div>',
      '            </div>',
      '            <div class="row">',
      '              <div class="form-group col-sm-6">',
      '                <label for="bitswarm-recipe">Recipe</label>',
      '                <select id="bitswarm-recipe" class="form-control"></select>',
      '              </div>',
      '              <div class="form-group col-sm-3">',
      '                <label for="bitswarm-profile">Profile</label>',
      '                <select id="bitswarm-profile" class="form-control"></select>',
      '              </div>',
      '              <div class="form-group col-sm-3">',
      '                <label for="bitswarm-visibility">Visibility</label>',
      '                <select id="bitswarm-visibility" class="form-control">',
      '                  <option value="public">Public</option>',
      '                  <option value="unlisted">Unlisted</option>',
      '                </select>',
      '              </div>',
      '            </div>',
      '            <div id="bitswarm-recipe-detail" class="well well-sm"></div>',
      '            <div class="row">',
      '              <div class="form-group col-sm-4">',
      '                <label for="bitswarm-population">Population</label>',
      '                <input id="bitswarm-population" class="form-control" type="number" min="1">',
      '              </div>',
      '              <div class="form-group col-sm-4">',
      '                <label for="bitswarm-max-workers">Max Workers</label>',
      '                <input id="bitswarm-max-workers" class="form-control" type="number" min="1" max="14">',
      '              </div>',
      '              <div class="form-group col-sm-4">',
      '                <label for="bitswarm-shortlist">Shortlist Ratio</label>',
      '                <input id="bitswarm-shortlist" class="form-control" type="number" min="0.001" max="1" step="0.001">',
      '              </div>',
      '            </div>',
      '            <div class="text-danger bitswarm-run-error" id="bitswarm-run-error"></div>',
      '            <button class="btn btn-primary" type="submit">Start Run</button>',
      '            <button class="btn btn-default" type="button" id="bitswarm-refresh-runs">Refresh Runs</button>',
      '          </form>',
      '        </div>',
      '        <div role="tabpanel" class="tab-pane" id="bitswarm-run-list">',
      '          <div class="table-responsive">',
      '            <table class="table table-striped table-hover">',
      '              <thead>',
      '                <tr>',
      '                  <th>Run</th><th>Host</th><th>Recipe</th><th>Profile</th><th>Status</th><th>Members</th><th>Seeds</th><th>Rollouts</th><th></th>',
      '                </tr>',
      '              </thead>',
      '              <tbody id="bitswarm-runs-body"></tbody>',
      '            </table>',
      '          </div>',
      '        </div>',
      '      </div>',
      '    </div>',
      '  </div>',
      '</div>'
    ].join("");
    document.body.appendChild(modal);
    document.getElementById("bitswarm-run-form").addEventListener("submit", createRun);
    document.getElementById("bitswarm-refresh-runs").addEventListener("click", function () {
      refreshRuns();
      activateTab("runs");
    });
    document.getElementById("bitswarm-actor").addEventListener("change", function (event) {
      setStoredActor(event.target.value);
      renderRuns();
    });
    document.getElementById("bitswarm-profile").addEventListener("change", syncProfileDefaults);
    document.getElementById("bitswarm-recipe").addEventListener("change", renderRecipeDetail);
    document.getElementById("bitswarm-runs-body").addEventListener("click", function (event) {
      var toggle = event.target.closest("[data-toggle-seeds]");
      if (toggle) {
        expandedRuns[toggle.getAttribute("data-toggle-seeds")] =
          !expandedRuns[toggle.getAttribute("data-toggle-seeds")];
        renderRuns();
        return;
      }
      var button = event.target.closest("[data-join-run]");
      if (button) {
        joinRun(button.getAttribute("data-join-run"));
      }
    });
  }

  function activateTab(which) {
    var link = document.querySelector(which === "runs" ?
      'a[href="#bitswarm-run-list"]' : 'a[href="#bitswarm-run-start"]');
    if (window.jQuery && link) {
      window.jQuery(link).tab("show");
    }
  }

  function showRunModal(tab) {
    ensureRunModal();
    loadCatalog().then(function () {
      refreshRuns();
      activateTab(tab === "runs" ? "runs" : "start");
      if (window.jQuery) {
        window.jQuery("#bitswarm-run-modal").modal("show");
      }
    });
  }

  function loadCatalog() {
    if (catalog) {
      return Promise.resolve(catalog);
    }
    return fetch("/api/bitswarm/ui/catalog", { cache: "no-store" })
      .then(checkJson)
      .then(function (payload) {
        catalog = payload;
        populateCatalog();
        return catalog;
      })
      .catch(showError);
  }

  function populateCatalog() {
    fillSelect("bitswarm-actor", catalog.operators, function (value) {
      return { value: value, label: value };
    }, getStoredActor());
    fillSelect("bitswarm-recipe", catalog.recipes, function (recipe) {
      return { value: recipe.id, label: recipe.label };
    }, "qwen05-arithmetic");
    fillSelect("bitswarm-profile", catalog.profiles, function (profile) {
      return { value: profile.id, label: profile.label };
    }, "smoke");
    syncProfileDefaults();
    renderRecipeDetail();
  }

  function fillSelect(id, rows, mapper, selected) {
    var select = document.getElementById(id);
    select.innerHTML = "";
    rows.forEach(function (row) {
      var mapped = mapper(row);
      var option = document.createElement("option");
      option.value = mapped.value;
      option.textContent = mapped.label;
      if (mapped.value === selected) {
        option.selected = true;
      }
      select.appendChild(option);
    });
  }

  function selectedProfile() {
    var id = document.getElementById("bitswarm-profile").value;
    return (catalog.profiles || []).filter(function (profile) { return profile.id === id; })[0];
  }

  function selectedRecipe() {
    var id = document.getElementById("bitswarm-recipe").value;
    return (catalog.recipes || []).filter(function (recipe) { return recipe.id === id; })[0];
  }

  function syncProfileDefaults() {
    var profile = selectedProfile();
    if (!profile) {
      return;
    }
    document.getElementById("bitswarm-population").value = profile.population;
    document.getElementById("bitswarm-max-workers").value = profile.max_workers;
    document.getElementById("bitswarm-shortlist").value = profile.shortlist_ratio;
  }

  function renderRecipeDetail() {
    var recipe = selectedRecipe();
    var profile = selectedProfile();
    var detail = document.getElementById("bitswarm-recipe-detail");
    if (!recipe || !profile) {
      detail.textContent = "";
      return;
    }
    detail.textContent = recipe.description + " Model: " + recipe.model +
      " | Evaluator: " + recipe.evaluator + " | Profile: " + profile.description;
  }

  function createRun(event) {
    event.preventDefault();
    clearError();
    var actor = document.getElementById("bitswarm-actor").value;
    setStoredActor(actor);
    var payload = {
      actor: actor,
      name: document.getElementById("bitswarm-run-name").value || "RandOpt Testnet",
      recipe_id: document.getElementById("bitswarm-recipe").value,
      profile_id: document.getElementById("bitswarm-profile").value,
      visibility: document.getElementById("bitswarm-visibility").value,
      settings: {
        population: Number(document.getElementById("bitswarm-population").value),
        max_workers: Number(document.getElementById("bitswarm-max-workers").value),
        shortlist_ratio: Number(document.getElementById("bitswarm-shortlist").value)
      }
    };
    fetch("/api/bitswarm/ui/runs", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(payload)
    })
      .then(checkJson)
      .then(function () {
        return refreshRuns();
      })
      .then(function () {
        activateTab("runs");
      })
      .catch(showError);
  }

  function refreshRuns() {
    return fetch("/api/bitswarm/ui/runs", { cache: "no-store" })
      .then(checkJson)
      .then(function (payload) {
        runs = payload.runs || [];
        renderRuns();
      })
      .catch(showError);
  }

  function renderRuns() {
    var body = document.getElementById("bitswarm-runs-body");
    if (!body) {
      return;
    }
    var actor = getStoredActor();
    if (!runs.length) {
      body.innerHTML = '<tr><td colspan="9" class="text-muted">No active runs.</td></tr>';
      return;
    }
    body.innerHTML = runs.map(function (run) {
      var joined = (run.members || []).some(function (member) { return member.actor === actor; });
      var memberText = (run.members || []).map(function (member) {
        return member.actor + ":" + member.role;
      }).join(", ");
      var seedSummary = summarizeSeeds(run);
      var rolloutSummary = summarizeRollouts(run);
      var expanded = !!expandedRuns[run.run_id];
      var action = joined ? '<span class="label label-success">Joined</span>' :
        '<button class="btn btn-xs btn-primary" data-join-run="' + escapeAttr(run.run_id) + '">Join</button>';
      var row = '<tr>' +
        '<td><strong>' + escapeHtml(run.name) + '</strong><br><small>' + escapeHtml(run.run_id) + '</small></td>' +
        '<td>' + escapeHtml(run.host_actor) + '</td>' +
        '<td>' + escapeHtml(run.recipe_label) + '</td>' +
        '<td>' + escapeHtml(run.profile_label) + '</td>' +
        '<td>' + escapeHtml(run.status) + '</td>' +
        '<td><span title="' + escapeAttr(memberText) + '">' + escapeHtml(String((run.members || []).length)) +
        '</span></td>' +
        '<td>' + escapeHtml(seedSummary) + '</td>' +
        '<td>' + escapeHtml(rolloutSummary) + '</td>' +
        '<td class="text-right">' +
        '<button class="btn btn-xs btn-default" data-toggle-seeds="' + escapeAttr(run.run_id) + '">' +
        (expanded ? 'Hide Seeds' : 'Seeds') + '</button> ' + action + '</td>' +
        '</tr>';
      if (expanded) {
        row += '<tr class="bitswarm-seed-detail-row"><td colspan="9">' + renderSeedDetails(run) + '</td></tr>';
      }
      return row;
    }).join("");
  }

  function summarizeSeeds(run) {
    var counts = { pending: 0, leased: 0, completed: 0 };
    (run.seeds || []).forEach(function (seed) {
      counts[seed.state] = (counts[seed.state] || 0) + 1;
    });
    return "P " + counts.pending + " / L " + counts.leased + " / C " + counts.completed;
  }

  function summarizeRollouts(run) {
    var pending = 0;
    var completed = 0;
    (run.seeds || []).forEach(function (seed) {
      (seed.rollouts || []).forEach(function (rollout) {
        if (rollout.status === "completed" || rollout.status === "failed") {
          completed += 1;
        } else {
          pending += 1;
        }
      });
    });
    return "P " + pending + " / C " + completed;
  }

  function renderSeedDetails(run) {
    var seeds = (run.seeds || []).slice().sort(function (left, right) {
      if (left.issued_at_ms !== right.issued_at_ms) {
        return left.issued_at_ms - right.issued_at_ms;
      }
      return left.seed_id.localeCompare(right.seed_id);
    });
    if (!seeds.length) {
      return '<div class="text-muted">No seeds issued.</div>';
    }
    return '<div class="panel-group bitswarm-seed-panel">' + seeds.map(function (seed) {
      var pending = (seed.rollouts || []).filter(function (row) {
        return row.status !== "completed" && row.status !== "failed";
      }).length;
      var completed = (seed.rollouts || []).length - pending;
      return '<div class="panel panel-default">' +
        '<div class="panel-heading">' +
        '<a data-toggle="collapse" href="#bitswarm-seed-' + escapeAttr(run.run_id + '-' + seed.seed_id) + '">' +
        '<strong>' + escapeHtml(seed.seed_id) + '</strong> ' +
        '<span class="text-muted">' + escapeHtml(seed.sigma_id) + ' · issued ' +
        escapeHtml(formatTimestamp(seed.issued_at_ms)) + ' · ' + escapeHtml(seed.state) +
        ' · pending ' + pending + ' · completed ' + completed + '</span>' +
        '</a>' +
        '</div>' +
        '<div id="bitswarm-seed-' + escapeAttr(run.run_id + '-' + seed.seed_id) +
        '" class="panel-collapse collapse">' +
        '<div class="panel-body">' + renderRolloutTable(seed) + '</div>' +
        '</div>' +
        '</div>';
    }).join("") + '</div>';
  }

  function renderRolloutTable(seed) {
    var rollouts = (seed.rollouts || []).slice().sort(function (left, right) {
      if (left.issued_at_ms !== right.issued_at_ms) {
        return left.issued_at_ms - right.issued_at_ms;
      }
      return left.rollout_id.localeCompare(right.rollout_id);
    });
    if (!rollouts.length) {
      return '<div class="text-muted">No rollouts reported yet for this seed.</div>';
    }
    return '<div class="table-responsive"><table class="table table-condensed table-bordered bitswarm-rollout-table">' +
      '<thead><tr><th>Machine</th><th>Item</th><th>Sign</th><th>Status</th><th>Issued</th><th>Completed</th>' +
      '<th>Score</th><th>Expected</th><th>Output</th></tr></thead><tbody>' +
      rollouts.map(function (rollout) {
        var cls = "";
        if (rollout.correct === true) {
          cls = "success";
        } else if (rollout.correct === false || rollout.status === "failed") {
          cls = "danger";
        } else if (rollout.status === "pending" || rollout.status === "running") {
          cls = "warning";
        }
        return '<tr class="' + cls + '">' +
          '<td>' + escapeHtml(rollout.machine) + '</td>' +
          '<td>' + escapeHtml(rollout.item_id) + '</td>' +
          '<td>' + escapeHtml(rollout.sign) + '</td>' +
          '<td>' + escapeHtml(rollout.status) + '</td>' +
          '<td>' + escapeHtml(formatTimestamp(rollout.issued_at_ms)) + '</td>' +
          '<td>' + escapeHtml(rollout.completed_at_ms ? formatTimestamp(rollout.completed_at_ms) : '-') + '</td>' +
          '<td>' + escapeHtml(rollout.score == null ? '-' : String(rollout.score)) + '</td>' +
          '<td>' + escapeHtml(rollout.expected || '') + '</td>' +
          '<td class="bitswarm-rollout-output">' + escapeHtml(rollout.output || '') + '</td>' +
          '</tr>';
      }).join("") + '</tbody></table></div>';
  }

  function formatTimestamp(value) {
    if (!value) {
      return "-";
    }
    var date = new Date(Number(value));
    if (Number.isNaN(date.getTime())) {
      return String(value);
    }
    return date.toLocaleTimeString();
  }

  function joinRun(runId) {
    clearError();
    fetch("/api/bitswarm/ui/runs/" + encodeURIComponent(runId) + "/join", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ actor: getStoredActor() })
    })
      .then(checkJson)
      .then(refreshRuns)
      .catch(showError);
  }

  function checkJson(response) {
    if (!response.ok) {
      return response.json().catch(function () {
        return { detail: response.statusText };
      }).then(function (payload) {
        throw new Error(payload.detail || response.statusText);
      });
    }
    return response.json();
  }

  function clearError() {
    var node = document.getElementById("bitswarm-run-error");
    if (node) {
      node.textContent = "";
    }
  }

  function showError(error) {
    var node = document.getElementById("bitswarm-run-error");
    if (node) {
      node.textContent = error && error.message ? error.message : String(error || "request failed");
    }
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function escapeAttr(value) {
    return escapeHtml(value).replace(/'/g, "&#39;");
  }

  normalizeRoute();
  window.addEventListener("hashchange", normalizeRoute);
  document.addEventListener("DOMContentLoaded", function () {
    hideStockSettings();
    ensureRunButtons();
    ensureRunModal();
    loadCatalog().then(refreshRuns);
    refreshTimer = window.setInterval(function () {
      hideStockSettings();
      ensureRunButtons();
      refreshRuns();
    }, 5000);
    window.addEventListener("beforeunload", function () {
      if (refreshTimer) {
        window.clearInterval(refreshTimer);
      }
    });
  });
}());
