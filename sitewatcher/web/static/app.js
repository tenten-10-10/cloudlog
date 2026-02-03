"use strict";

function normalizeText(s) {
  return (s || "").toString().toLowerCase().replace(/\s+/g, " ").trim();
}

function setupConfirm() {
  document.addEventListener("click", (e) => {
    const el = e.target.closest("[data-confirm]");
    if (!el) return;
    const msg = el.getAttribute("data-confirm") || "本当に実行しますか？";
    if (!confirm(msg)) e.preventDefault();
  });
}

function setupFilter() {
  const input = document.querySelector("[data-filter-input]");
  const typeSelect = document.querySelector("[data-filter-type]");
  const enabledSelect = document.querySelector("[data-filter-enabled]");
  const countEl = document.querySelector("[data-filter-count]");
  const items = Array.from(document.querySelectorAll("[data-filter-item]"));
  const emptyNoTargets = document.querySelector('[data-empty-state="no-targets"]');
  const emptyNoMatch = document.querySelector('[data-empty-state="no-match"]');
  if (!input) return;

  function apply() {
    const q = normalizeText(input.value);
    const type = typeSelect ? (typeSelect.value || "all") : "all";
    const enabled = enabledSelect ? (enabledSelect.value || "all") : "all";
    let visible = 0;

    for (const item of items) {
      const text = normalizeText(item.getAttribute("data-filter-text") || item.textContent || "");
      const itemType = (item.getAttribute("data-type") || "").toLowerCase();
      const itemEnabled = item.getAttribute("data-enabled") || "";

      const okText = q === "" || text.includes(q);
      const okType = type === "all" || itemType === type;
      const okEnabled = enabled === "all" || itemEnabled === enabled;
      const show = okText && okType && okEnabled;

      item.hidden = !show;
      if (show) visible += 1;
    }

    if (countEl) countEl.textContent = String(visible);
    if (items.length === 0) {
      if (emptyNoTargets) emptyNoTargets.hidden = false;
      if (emptyNoMatch) emptyNoMatch.hidden = true;
    } else if (visible === 0) {
      if (emptyNoTargets) emptyNoTargets.hidden = true;
      if (emptyNoMatch) emptyNoMatch.hidden = false;
    } else {
      if (emptyNoTargets) emptyNoTargets.hidden = true;
      if (emptyNoMatch) emptyNoMatch.hidden = true;
    }
  }

  input.addEventListener("input", apply);
  if (typeSelect) typeSelect.addEventListener("change", apply);
  if (enabledSelect) enabledSelect.addEventListener("change", apply);
  apply();
}

function setupTargetFormTypeToggle() {
  const typeSelect = document.querySelector('select[name="type"][data-target-type]');
  if (!typeSelect) return;
  const htmlOnly = Array.from(document.querySelectorAll("[data-only-type='html']"));
  const rssOnly = Array.from(document.querySelectorAll("[data-only-type='rss']"));

  function setDisabled(container, disabled) {
    const controls = container.querySelectorAll("input, select, textarea, button");
    for (const el of controls) {
      if (el.getAttribute("type") === "hidden") continue;
      el.disabled = !!disabled;
    }
  }

  function apply() {
    const t = (typeSelect.value || "html").toLowerCase();
    for (const el of htmlOnly) {
      const show = t === "html";
      el.hidden = !show;
      setDisabled(el, !show);
    }
    for (const el of rssOnly) {
      const show = t === "rss";
      el.hidden = !show;
      setDisabled(el, !show);
    }
  }

  typeSelect.addEventListener("change", apply);
  apply();
}

function setupNotifierToggle() {
  const toggles = Array.from(document.querySelectorAll("[data-notifier-toggle]"));
  if (toggles.length === 0) return;
  for (const input of toggles) {
    const name = input.getAttribute("data-notifier-toggle");
    if (!name) continue;
    const cfg = document.querySelector(`[data-notifier-config="${name}"]`);
    const apply = () => {
      if (!cfg) return;
      cfg.hidden = !input.checked;
    };
    input.addEventListener("change", apply);
    apply();
  }
}

function setupPasswordReveal() {
  document.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-reveal-for]");
    if (!btn) return;
    const id = btn.getAttribute("data-reveal-for");
    if (!id) return;
    const input = document.getElementById(id);
    if (!input) return;
    if (input.getAttribute("type") === "password") {
      input.setAttribute("type", "text");
      btn.textContent = "隠す";
    } else {
      input.setAttribute("type", "password");
      btn.textContent = "表示";
    }
  });
}

document.addEventListener("DOMContentLoaded", () => {
  setupConfirm();
  setupFilter();
  setupTargetFormTypeToggle();
  setupNotifierToggle();
  setupPasswordReveal();
});
