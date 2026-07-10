(function () {
  var BRAND_VERSION = "20260625-vm-brand";
  var lightLogo = "/assets/branding/logo-light.svg?v=" + BRAND_VERSION;
  var darkLogo = "/assets/branding/logo-dark.svg?v=" + BRAND_VERSION;
  var lightIcon = "/assets/branding/favicon.svg?v=" + BRAND_VERSION;
  var darkIcon = "/assets/branding/favicon-dark.svg?v=" + BRAND_VERSION;
  var appleTouch = "/assets/branding/apple-touch-icon.png?v=" + BRAND_VERSION;

  function isDarkTheme() {
    var body = document.body;
    var html = document.documentElement;
    var themeSource = [
      body && body.getAttribute("data-theme"),
      html && html.getAttribute("data-theme"),
      body && body.className,
      html && html.className
    ].filter(Boolean).join(" ").toLowerCase();
    if (themeSource.includes("dark")) {
      return true;
    }
    if (themeSource.includes("light")) {
      return false;
    }
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
  }

  function syncBrandAssets() {
    var dark = isDarkTheme();
    document.querySelectorAll("[data-brand-logo-img]").forEach(function (img) {
      img.src = dark ? darkLogo : lightLogo;
    });
    document.querySelectorAll('link[rel="icon"]').forEach(function (link) {
      link.href = dark ? darkIcon : lightIcon;
      link.type = "image/svg+xml";
    });
    document.querySelectorAll('link[rel="apple-touch-icon"]').forEach(function (link) {
      link.href = appleTouch;
    });
  }

  window.__syncBrandAssets = syncBrandAssets;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", syncBrandAssets, { once: true });
  } else {
    syncBrandAssets();
  }

  if (window.matchMedia) {
    var media = window.matchMedia("(prefers-color-scheme: dark)");
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", syncBrandAssets);
    } else if (typeof media.addListener === "function") {
      media.addListener(syncBrandAssets);
    }
  }
})();
