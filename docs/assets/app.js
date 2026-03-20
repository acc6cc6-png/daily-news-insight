const state = {
  digest: null,
  history: [],
  activeCategoryId: null,
  summaryCollapsed: false,
  usingLatest: true,
};

document.addEventListener("DOMContentLoaded", () => {
  void bootstrap();
});

async function bootstrap() {
  bindChromeEvents();

  try {
    const [digest, history] = await Promise.all([
      fetchJson("./data/latest/digest.json"),
      fetchJson("./data/history/index.json", []),
    ]);

    state.digest = digest;
    state.history = Array.isArray(history) ? history : [];
    state.activeCategoryId = digest.categories?.[0]?.id ?? null;

    renderTabs();
    renderHistory();
    renderCategory();
  } catch (error) {
    renderFatalState(error);
  }
}

function bindChromeEvents() {
  const historyOpen = document.querySelector("#history-open");
  const historyClose = document.querySelector("#history-close");
  const historyCloseButton = document.querySelector("#history-close-button");
  const summaryToggle = document.querySelector("#summary-toggle");

  historyOpen?.addEventListener("click", () => toggleHistory(true));
  historyClose?.addEventListener("click", () => toggleHistory(false));
  historyCloseButton?.addEventListener("click", () => toggleHistory(false));

  summaryToggle?.addEventListener("click", () => {
    state.summaryCollapsed = !state.summaryCollapsed;
    renderSummaryCollapse();
  });
}

async function fetchJson(url, fallback = null) {
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    if (fallback !== null) {
      return fallback;
    }
    throw error;
  }
}

function renderTabs() {
  const container = document.querySelector("#category-tabs");
  if (!container || !state.digest) {
    return;
  }

  container.innerHTML = state.digest.categories
    .map((category) => {
      const active = category.id === state.activeCategoryId;
      return `
        <button
          class="tab-button ${active ? "is-active" : ""}"
          type="button"
          data-category-id="${escapeHtml(category.id)}"
        >
          ${escapeHtml(category.name)}
        </button>
      `;
    })
    .join("");

  container.querySelectorAll("[data-category-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeCategoryId = button.getAttribute("data-category-id");
      renderTabs();
      renderCategory();
    });
  });
}

function renderCategory() {
  const category = getActiveCategory();
  if (!category || !state.digest) {
    return;
  }

  setText("#hero-kicker", state.digest.site.subtitle);
  setText("#hero-title", category.name);
  setText("#hero-lead", category.lead);
  setText("#edition-summary", `${state.digest.edition.dateLabel} ${state.digest.edition.timeLabel} · ${modeLabel(state.digest.edition.mode)}`);
  setText("#metric-generated", `${state.digest.edition.generatedAt} (${state.digest.edition.timezone})`);
  setText("#metric-mode", modeLabel(state.digest.edition.mode));
  setText("#metric-stories", `${category.stories.length} 条`);
  setText("#bullish-copy", category.bullish);
  setText("#bearish-copy", category.bearish);
  setText("#watch-copy", category.watch);
  setText("#lens-copy", category.lens);
  setText("#analyst-copy", category.economistTake);
  toggleLatestIndicator();
  renderSummaryCollapse();
  renderStories(category);
  renderComments(category);
  renderSources(category);
}

function renderStories(category) {
  const container = document.querySelector("#stories-list");
  if (!container) {
    return;
  }

  container.innerHTML = category.stories
    .map(
      (story) => `
        <article class="story-card">
          <div class="story-header">
            <span class="story-index">#${story.index}</span>
            <span class="story-signal ${signalClass(story.signal)}">${escapeHtml(story.signalLabel)}</span>
          </div>
          <h4>${escapeHtml(story.title)}</h4>
          <p>${escapeHtml(story.summary)}</p>
          <p class="signal-copy">${escapeHtml(story.reason)}</p>
          <div class="story-footer">
            <span class="story-source">${escapeHtml(story.source)}${story.publishedAt ? ` · ${escapeHtml(story.publishedAt)}` : ""}</span>
            <a class="story-link" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">Read Source</a>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderComments(category) {
  const container = document.querySelector("#comments-list");
  if (!container) {
    return;
  }

  container.innerHTML = category.personaComments
    .map(
      (comment) => `
        <article class="comment-card">
          <div class="comment-header">
            <span class="comment-name">${escapeHtml(comment.name)}</span>
            <span class="comment-role">${escapeHtml(comment.role)}</span>
            <span class="comment-emotion ${signalClass(normalizeEmotion(comment.emotion))}">
              ${escapeHtml(comment.emotion)}
            </span>
          </div>
          <h4>${escapeHtml(comment.role)}</h4>
          <p>${escapeHtml(comment.content)}</p>
        </article>
      `,
    )
    .join("");
}

function renderSources(category) {
  const container = document.querySelector("#source-list");
  if (!container || !state.digest) {
    return;
  }

  const registry = new Map(state.digest.sourceRegistry.map((item) => [item.id, item]));
  container.innerHTML = category.sourcesUsed
    .map((source) => {
      const full = registry.get(source.id) ?? source;
      return `
        <a class="source-pill" href="${escapeAttribute(full.home ?? "#")}" target="_blank" rel="noreferrer">
          <span>${escapeHtml(full.label)}</span>
          <span>${escapeHtml(sourceKindLabel(full.kind))}</span>
        </a>
      `;
    })
    .join("");
}

function renderHistory() {
  const container = document.querySelector("#history-list");
  if (!container) {
    return;
  }

  const items = [
    {
      id: "latest",
      label: "最新版本",
      path: "latest",
      mode: "latest",
    },
    ...state.history.map((item) => ({ ...item, mode: "history" })),
  ];

  container.innerHTML = items
    .map(
      (item) => `
        <div class="history-entry">
          <span class="history-badge">${item.mode === "latest" ? "Latest" : "Archive"}</span>
          <div class="history-entry-copy">${escapeHtml(item.label)}</div>
          <button class="history-entry-button" data-history-path="${escapeHtml(item.path)}" type="button">打开</button>
        </div>
      `,
    )
    .join("");

  container.querySelectorAll("[data-history-path]").forEach((button) => {
    button.addEventListener("click", async () => {
      const path = button.getAttribute("data-history-path");
      await loadEdition(path);
      toggleHistory(false);
    });
  });
}

async function loadEdition(path) {
  try {
    const digest =
      path === "latest"
        ? await fetchJson("./data/latest/digest.json")
        : await fetchJson(`./${path}`);

    state.digest = digest;
    state.usingLatest = path === "latest";

    if (!digest.categories.some((item) => item.id === state.activeCategoryId)) {
      state.activeCategoryId = digest.categories?.[0]?.id ?? null;
    }

    renderTabs();
    renderCategory();
  } catch (error) {
    renderFatalState(error);
  }
}

function renderSummaryCollapse() {
  const body = document.querySelector("#summary-body");
  const toggle = document.querySelector("#summary-toggle");
  if (!body || !toggle) {
    return;
  }

  body.classList.toggle("is-collapsed", state.summaryCollapsed);
  toggle.textContent = state.summaryCollapsed ? "展开" : "收起";
}

function toggleHistory(open) {
  const modal = document.querySelector("#history-modal");
  if (!modal) {
    return;
  }
  modal.classList.toggle("hidden", !open);
  modal.setAttribute("aria-hidden", String(!open));
}

function toggleLatestIndicator() {
  const indicator = document.querySelector("#latest-indicator");
  if (!indicator) {
    return;
  }
  indicator.classList.toggle("hidden", !state.usingLatest);
}

function getActiveCategory() {
  return state.digest?.categories?.find((item) => item.id === state.activeCategoryId) ?? null;
}

function modeLabel(mode) {
  if (mode === "ai") {
    return "AI 正式版";
  }
  if (mode === "mixed") {
    return "AI + 模板混合";
  }
  return "模板演示版";
}

function sourceKindLabel(kind) {
  return kind === "rss" ? "国际 RSS" : kind === "newsnow" ? "中文聚合" : "来源";
}

function signalClass(signal) {
  if (signal === "bullish") {
    return "is-bullish";
  }
  if (signal === "bearish") {
    return "is-bearish";
  }
  return "is-watch";
}

function normalizeEmotion(emotion) {
  if (emotion.includes("乐观")) {
    return "bullish";
  }
  if (emotion.includes("谨慎") || emotion.includes("担忧")) {
    return "bearish";
  }
  return "watch";
}

function setText(selector, value) {
  const node = document.querySelector(selector);
  if (node) {
    node.textContent = value ?? "";
  }
}

function renderFatalState(error) {
  console.error(error);
  setText("#hero-title", "页面加载失败");
  setText("#hero-lead", error?.message ?? "无法读取最新数据");
  setText("#bullish-copy", "请确认 docs/data/latest/digest.json 已生成。");
  setText("#bearish-copy", "如果是 GitHub Pages，请确认 Pages 的发布目录设置为 /docs。");
  setText("#watch-copy", "你也可以先本地运行 python scripts/daily_digest.py 生成演示数据。");
  setText("#analyst-copy", "当前页面只依赖静态 JSON，如果数据文件不存在，前端不会自行生成内容。");
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}
