const state = {
  digest: null,
  history: [],
  activeCategoryId: null,
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
    renderPage();
  } catch (error) {
    renderFatalState(error);
  }
}

function bindChromeEvents() {
  document.querySelector("#history-open")?.addEventListener("click", () => toggleHistory(true));
  document.querySelector("#history-close")?.addEventListener("click", () => toggleHistory(false));
  document.querySelector("#history-close-button")?.addEventListener("click", () => toggleHistory(false));
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

function renderPage() {
  if (!state.digest) {
    return;
  }
  const category = getActiveCategory();
  if (!category) {
    return;
  }

  renderHeader(category);
  renderPulse();
  renderOverview(category);
  renderSignals(category);
  renderFeeds(category);
  renderComments(category);
  renderSources(category);
  toggleLatestIndicator();
}

function renderHeader(category) {
  const edition = state.digest.edition;
  setText("#hero-kicker", state.digest.site.subtitle);
  setText("#hero-title", category.name);
  setText("#hero-lead", category.lead);
  setText("#generated-badge", `${edition.generatedAt} 更新`);
  setText("#window-banner", `统计窗口：${edition.windowLabel} ｜ 当前聚焦 ${category.name}`);
  setText("#sidebar-window", edition.windowLabel);
}

function renderPulse() {
  const pulse = state.digest?.marketPulse;
  const container = document.querySelector("#pulse-grid");
  if (!pulse || !container) {
    return;
  }

  setText("#pulse-window", pulse.windowLabel);
  setText("#pulse-headline", pulse.headline);

  container.innerHTML = pulse.highlights
    .map(
      (story) => `
        <article class="pulse-item">
          <div class="pulse-meta">
            <span class="pulse-category">${escapeHtml(story.categoryName)}</span>
            <span class="story-signal ${signalClass(story.signal)}">${escapeHtml(story.impactLabel)}</span>
          </div>
          <h4>${escapeHtml(story.title)}</h4>
          <p>${escapeHtml(story.impactReason)}</p>
          <div class="story-footer">
            <span class="story-source">${escapeHtml(story.source)}${story.publishedAt ? ` · ${escapeHtml(story.publishedAt)}` : ""}</span>
            <a class="story-link" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">原文</a>
          </div>
        </article>
      `,
    )
    .join("");
}

function renderOverview(category) {
  setText("#cycle-view", category.cycleView);
  setText("#strategy-take", category.strategyTake);
  setText("#metric-window", category.windowLabel);
  setText("#metric-generated", state.digest.edition.generatedAt);
  setText("#metric-mode", modeLabel(state.digest.edition.mode));
  setText("#story-total", `${category.stats.allStoryCount} 条动态`);
  setText("#priority-total", `${category.stats.priorityCount} 条重点`);
  setText("#lens-copy", category.lens);
  setText("#analyst-copy", category.economistTake);

  const linkageList = document.querySelector("#linkage-list");
  if (linkageList) {
    linkageList.innerHTML = category.linkageIdeas
      .map(
        (item, index) => `
          <article class="linkage-item">
            <span class="linkage-index">0${index + 1}</span>
            <p>${escapeHtml(item)}</p>
          </article>
        `,
      )
      .join("");
  }
}

function renderSignals(category) {
  setText("#bullish-copy", category.bullish);
  setText("#bearish-copy", category.bearish);
  setText("#watch-copy", category.watch);
}

function renderFeeds(category) {
  renderStoryList("#all-stories-list", category.allStories, "full");
  renderStoryList("#priority-stories-list", category.priorityStories, "priority");
}

function renderStoryList(selector, stories, layout) {
  const container = document.querySelector(selector);
  if (!container) {
    return;
  }

  container.innerHTML = stories
    .map((story) => {
      const badge = layout === "priority" ? `#${story.priorityRank}` : `#${story.index}`;
      return `
        <article class="story-card story-card--${layout}">
          <div class="story-header">
            <span class="story-index">${badge}</span>
            <span class="story-signal ${signalClass(story.signal)}">${escapeHtml(story.impactLabel || story.signalLabel)}</span>
          </div>
          <h4>${escapeHtml(story.title)}</h4>
          <p>${escapeHtml(story.summary)}</p>
          <p class="signal-copy">${escapeHtml(story.impactReason || story.reason)}</p>
          <div class="story-footer">
            <span class="story-source">${escapeHtml(story.source)}${story.publishedAt ? ` · ${escapeHtml(story.publishedAt)}` : ""}</span>
            <a class="story-link" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">原文</a>
          </div>
        </article>
      `;
    })
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
      renderPage();
    });
  });
}

function renderHistory() {
  const container = document.querySelector("#history-list");
  if (!container) {
    return;
  }

  const items = [
    { id: "latest", label: "最新快照", path: "latest", mode: "latest" },
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
    renderPage();
  } catch (error) {
    renderFatalState(error);
  }
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
    return "正式版";
  }
  if (mode === "mixed") {
    return "研究快照";
  }
  return "快照版";
}

function sourceKindLabel(kind) {
  return kind === "rss" ? "国际媒体" : kind === "newsnow" ? "中文聚合" : "来源";
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
  if (emotion.includes("积极") || emotion.includes("乐观")) {
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
  setText("#window-banner", "请确认 docs/data/latest/digest.json 已经生成。");
  setText("#cycle-view", "如果这是 GitHub Pages，请检查 Actions 是否完成，或稍后刷新。");
  setText("#strategy-take", "站点前端只读取静态 JSON；如果数据不存在，页面不会自行生成内容。");
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
