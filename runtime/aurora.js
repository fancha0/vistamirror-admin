/* Theme preference is local presentation state; it never changes server settings. */
(() => {
  'use strict';
  const key = 'vistamirror.ui.theme';
  const root = document.documentElement;
  const normalize = value => value === 'light' ? 'light' : 'aurora';
  let theme = 'aurora';
  try { theme = normalize(localStorage.getItem(key)); } catch (_) { /* Storage may be blocked. */ }
  root.dataset.vmTheme = theme;

  function render() {
    const dark = root.dataset.vmTheme === 'aurora';
    document.querySelectorAll('[data-theme-toggle]').forEach(button => {
      button.setAttribute('aria-pressed', String(dark));
      button.setAttribute('aria-label', dark ? '切换浅色主题' : '切换极光主题');
      button.title = dark ? '极光主题 · 点击切换浅色' : '浅色主题 · 点击切换极光';
      button.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true">' + (dark
        ? '<circle cx="12" cy="12" r="4"/><path d="M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.4 1.4m11.2 11.2L19 19M5 19l1.4-1.4M17.6 6.4 19 5"/>'
        : '<path d="M20.8 13.3A9 9 0 0 1 10.7 3.2 9 9 0 1 0 20.8 13.3Z"/>') + '</svg>';
    });
  }
  function apply(value, persist) {
    root.dataset.vmTheme = normalize(value);
    if (persist) try { localStorage.setItem(key, root.dataset.vmTheme); } catch (_) { /* Still usable for this page. */ }
    render();
  }
  function init() {
    render();
    document.addEventListener('click', event => {
      if (event.target.closest('[data-theme-toggle]')) apply(root.dataset.vmTheme === 'aurora' ? 'light' : 'aurora', true);
    });
  }
  window.addEventListener('storage', event => { if (event.key === key || event.key === null) apply(event.newValue, false); });
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', init, { once: true });
  else init();
})();
