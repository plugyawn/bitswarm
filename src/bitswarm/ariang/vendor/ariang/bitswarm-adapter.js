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

  normalizeRoute();
  window.addEventListener("hashchange", normalizeRoute);
  document.addEventListener("DOMContentLoaded", function () {
    hideStockSettings();
    window.setInterval(hideStockSettings, 1000);
  });
}());
