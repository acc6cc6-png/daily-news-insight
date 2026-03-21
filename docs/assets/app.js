const state = {
  digest: null,
  history: [],
  activeCategoryId: null,
  activeSectionId: "section-compass",
  usingLatest: true,
};

const BOARD_GROUPS = [
  {
    id: "macro",
    name: "宏观与主线",
    description: "重点新闻、国内外宏观与市场主线",
    categories: ["focus-news", "china-macro", "china-markets", "global-macro", "geopolitics"],
  },
  {
    id: "growth",
    name: "科技与成长",
    description: "AI、芯片和高成长叙事",
    categories: ["ai-models", "chips-devices", "crypto-web3"],
  },
  {
    id: "industry",
    name: "产业与商品",
    description: "能源、大宗和消费产业链",
    categories: ["energy-commodities", "industry-consumer"],
  },
  {
    id: "sentiment",
    name: "社会与情绪",
    description: "社会热点和大众情绪温度",
    categories: ["social-trends"],
  },
];

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

    renderSiteChrome();
    renderSidebarBoards();
    renderHistory();
    renderPage();
  } catch (error) {
    renderFatalState(error);
  }
}

function bindChromeEvents() {
  bindAccordionEvents();
  document.querySelector("#history-open")?.addEventListener("click", () => toggleHistory(true));
  document.querySelector("#history-close")?.addEventListener("click", () => toggleHistory(false));
  document.querySelector("#history-close-button")?.addEventListener("click", () => toggleHistory(false));

  document.querySelectorAll("[data-section-target]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.getAttribute("data-section-target");
      scrollToSection(id);
    });
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

function renderSiteChrome() {
  if (!state.digest) {
    return;
  }

  const site = state.digest.site ?? {};
  setText("#site-title", site.title || "小程AI的新闻日报");
  setText("#site-subtitle", site.subtitle || "跨市场重点新闻与投研摘要");
  setText("#contact-author", site.author || site.title || "乐毅");
  setText("#contact-wechat", site.contact_wechat || "cfu800");
  setText("#contact-email", site.contact_email || "3119268060@qq.com");
  setHref("#contact-email", `mailto:${site.contact_email || "3119268060@qq.com"}`);
  setHref("#repo-link", site.repo_url || "https://github.com/acc6cc6-png/daily-news-insight");
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
  renderTradeCompass();
  renderPulse();
  renderFeeds(category);
  renderOverview(category);
  renderSignals(category);
  renderComments(category);
  renderSources(category);
  renderSidebarBoards();
  renderSectionSummaries(category);
  toggleLatestIndicator();
  syncSectionButtons();
}

function renderHeader(category) {
  const edition = state.digest.edition;

  setText("#hero-kicker", state.digest.site.subtitle || "跨市场重点新闻与投研摘要");
  setText("#hero-title", category.name);
  setText("#hero-lead", category.lead);
  setText("#generated-badge", `${edition.generatedAt} 更新`);
  setText("#window-banner", `统计窗口：${edition.windowLabel} · 当前聚焦 ${category.name}`);
  setText("#sidebar-window", edition.windowLabel);
  setText("#metric-window", category.windowLabel);
  setText("#metric-generated", edition.generatedAt);
  setText("#metric-mode", modeLabel(edition.mode));
  setText("#metric-category", category.name);
  setText("#story-total", `${category.stats.allStoryCount} 条动态`);
  setText("#priority-total", `${category.stats.priorityCount} 条重点`);

  document.title = `${category.name} | ${state.digest.site.title || "小程AI的新闻日报"}`;
}

function renderTradeCompass() {
  const compass = state.digest?.tradeCompass;
  if (!compass) {
    return;
  }

  const bias = document.querySelector("#compass-bias");
  if (bias) {
    bias.className = `trade-bias ${signalClass(compass.biasSignal || "watch")}`;
    bias.textContent = compass.biasLabel || "等待确认";
  }

  setText("#compass-title", compass.title || "今日交易指北");
  setText("#compass-summary", compass.summary || "");
  setText("#compass-window", state.digest?.edition?.windowLabel || "");

  renderChipList("#compass-drivers", compass.drivers, "trade-driver-chip");
  renderChipList("#compass-watchlist", compass.watchlist, "watch-pill");
  renderTradeItemList("#compass-leaders", compass.leaders);
  renderTradeItemList("#compass-laggards", compass.laggards);
  renderTradeItemList("#compass-stable", compass.stable);

  const playbook = document.querySelector("#compass-playbook");
  if (playbook) {
    playbook.innerHTML = (compass.playbook || [])
      .map(
        (item, index) => `
          <article class="trade-rule">
            <span class="trade-rule-index">0${index + 1}</span>
            <p>${escapeHtml(item)}</p>
          </article>
        `,
      )
      .join("");
  }
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
  setText("#analyst-copy", category.economistTake);
  setText("#lens-copy", category.lens);

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
  renderStoryList("#priority-stories-list", category.priorityStories, "priority");
  renderStoryList("#all-stories-list", category.allStories, "full");
}

function renderStoryList(selector, stories, layout) {
  const container = document.querySelector(selector);
  if (!container) {
    return;
  }

  container.innerHTML = stories
    .map((story) => {
      const badge = layout === "priority" ? `#${story.priorityRank}` : `#${story.index}`;
      const origin =
        layout === "priority" && story.isSynthetic && story.sourceTitle
          ? `<p class="story-origin">关联原始标题：${escapeHtml(story.sourceTitle)}</p>`
          : "";

      return `
        <article class="story-card story-card--${layout}">
          <div class="story-header">
            <span class="story-index">${badge}</span>
            <span class="story-signal ${signalClass(story.signal)}">${escapeHtml(story.impactLabel || story.signalLabel)}</span>
          </div>
          <h4>${escapeHtml(story.title)}</h4>
          <p class="story-summary">${escapeHtml(story.summary)}</p>
          <p class="signal-copy">${escapeHtml(story.impactReason || story.reason)}</p>
          ${origin}
          <div class="story-footer">
            <span class="story-source">${escapeHtml(story.source)}${story.publishedAt ? ` · ${escapeHtml(story.publishedAt)}` : ""}</span>
            <a class="story-link" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">原文</a>
          </div>
        </article>
      `;
    })
    .join("");
}

function renderChipList(selector, items, className) {
  const container = document.querySelector(selector);
  if (!container) {
    return;
  }

  const values = Array.isArray(items) && items.length ? items : ["等待更多确认"];
  container.innerHTML = values
    .map((item) => `<span class="${className}">${escapeHtml(item)}</span>`)
    .join("");
}

function renderTradeItemList(selector, items) {
  const container = document.querySelector(selector);
  if (!container) {
    return;
  }

  const values = Array.isArray(items) && items.length ? items : [{ name: "等待更多确认", reason: "当前窗口还不足以给出更明确方向。" }];
  container.innerHTML = values
    .map(
      (item) => `
        <article class="trade-item">
          <span class="trade-item-name">${escapeHtml(item.name)}</span>
          <p class="trade-item-reason">${escapeHtml(item.reason)}</p>
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

function renderSectionSummaries(category) {
  const compass = state.digest?.tradeCompass;
  const pulse = state.digest?.marketPulse;
  const firstPriority = category.priorityStories?.[0];
  const firstComment = category.personaComments?.[0];
  const sourceKinds = new Set(
    (category.sourcesUsed || [])
      .map((source) => {
        const full = state.digest?.sourceRegistry?.find((item) => item.id === source.id) ?? source;
        return sourceKindLabel(full.kind);
      })
      .filter(Boolean),
  );

  setText("#summary-compass-bias", compass?.biasLabel || "等待确认");
  setText("#summary-compass-watch", `${compass?.watchlist?.length || 0} 个观察项`);
  setText("#summary-pulse-count", `${pulse?.highlights?.length || 0} 条主线`);
  setText("#summary-pulse-window", state.digest?.edition?.windowLabel || "-");
  setText("#summary-priority-count", `${category.priorityStories?.length || 0} 条重点`);
  setText("#summary-priority-top", firstPriority ? shortenText(firstPriority.title, 18) : "等待排序");
  setText("#summary-feed-count", `${category.allStories?.length || 0} 条动态`);
  setText("#summary-feed-window", category.windowLabel || "-");
  setText("#summary-overview-board", category.name || "-");
  setText("#summary-overview-lens", shortenText(category.lens || "-", 16));
  setText("#summary-comments-count", `${category.personaComments?.length || 0} 个角色`);
  setText("#summary-comments-role", firstComment ? firstComment.role : "等待观点");
  setText("#summary-sources-count", `${category.sourcesUsed?.length || 0} 个来源`);
  setText("#summary-sources-mix", Array.from(sourceKinds).join(" / ") || "等待来源");

  const compassBias = document.querySelector("#summary-compass-bias");
  if (compassBias) {
    compassBias.className = `accordion-chip ${signalClass(compass?.biasSignal || "watch")}`;
  }
}

function renderSidebarBoards() {
  const container = document.querySelector("#sidebar-category-list");
  if (!container || !state.digest) {
    return;
  }

  const activeCategory = getActiveCategory();
  const currentBoard = document.querySelector("#sidebar-current-board");
  if (currentBoard && activeCategory) {
    currentBoard.innerHTML = `
      <p class="board-current-label">当前板块</p>
      <strong>${escapeHtml(activeCategory.name)}</strong>
      <p>${escapeHtml(activeCategory.description)}</p>
      <span>${activeCategory.stats.priorityCount} 条重点 · ${activeCategory.stats.allStoryCount} 条动态</span>
    `;
  }

  container.innerHTML = BOARD_GROUPS.map((group) => {
    const categories = group.categories
      .map((id) => state.digest.categories.find((category) => category.id === id))
      .filter(Boolean);

    if (!categories.length) {
      return "";
    }

    const open = categories.some((category) => category.id === state.activeCategoryId);
    const buttons = categories
      .map((category) => {
        const active = category.id === state.activeCategoryId;
        return `
          <button
            class="board-button board-button--compact ${active ? "is-active" : ""}"
            type="button"
            data-category-id="${escapeHtml(category.id)}"
          >
            <span class="board-button-title">${escapeHtml(category.name)}</span>
            <span class="board-button-meta">${category.stats.priorityCount} 条重点 · ${category.stats.allStoryCount} 条动态</span>
          </button>
        `;
      })
      .join("");

    return `
      <details class="board-cluster" data-accordion-group="boards" ${open ? "open" : ""}>
        <summary class="board-cluster-summary">
          <div>
            <strong>${escapeHtml(group.name)}</strong>
            <p>${escapeHtml(group.description)}</p>
          </div>
          <span class="board-cluster-meta">${categories.length} 个板块</span>
        </summary>
        <div class="board-cluster-body">
          ${buttons}
        </div>
      </details>
    `;
  }).join("");

  container.querySelectorAll("[data-category-id]").forEach((button) => {
    button.addEventListener("click", () => {
      state.activeCategoryId = button.getAttribute("data-category-id");
      renderPage();
      scrollToSection("section-top");
    });
  });

  bindAccordionEvents(container);
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
      scrollToSection("section-top");
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

    renderSiteChrome();
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

function bindAccordionEvents(root = document) {
  root.querySelectorAll("details[data-accordion-group]").forEach((detail) => {
    if (detail.dataset.bound === "true") {
      return;
    }
    detail.dataset.bound = "true";
    detail.addEventListener("toggle", () => {
      const group = detail.getAttribute("data-accordion-group");
      if (detail.open) {
        document.querySelectorAll(`details[data-accordion-group="${group}"]`).forEach((peer) => {
          if (peer !== detail) {
            peer.open = false;
          }
        });
      }
      if (group === "main") {
        syncSectionButtons();
      }
    });
  });
}

function syncSectionButtons() {
  const openMain = document.querySelector('details[data-accordion-group="main"][open]');
  state.activeSectionId = openMain?.id ?? null;
  document.querySelectorAll("[data-section-target]").forEach((button) => {
    button.classList.toggle("is-active", button.getAttribute("data-section-target") === state.activeSectionId);
  });
}

function getActiveCategory() {
  return state.digest?.categories?.find((item) => item.id === state.activeCategoryId) ?? null;
}

function scrollToSection(id) {
  if (!id) {
    return;
  }
  const node = document.getElementById(id);
  if (!node) {
    return;
  }
  if (node.tagName === "DETAILS") {
    node.open = true;
  }
  node.scrollIntoView({ behavior: "smooth", block: "start" });
  syncSectionButtons();
}

function modeLabel(mode) {
  if (mode === "ai") {
    return "正式编写";
  }
  if (mode === "mixed") {
    return "混合模式";
  }
  return "规则快照";
}

function sourceKindLabel(kind) {
  if (kind === "rss") {
    return "国际媒体";
  }
  if (kind === "newsnow") {
    return "中文聚合";
  }
  return "来源";
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

function setHref(selector, value) {
  const node = document.querySelector(selector);
  if (node) {
    node.setAttribute("href", value ?? "#");
  }
}

function renderFatalState(error) {
  console.error(error);
  setText("#hero-title", "页面加载失败");
  setText("#hero-lead", error?.message ?? "无法读取最新数据");
  setText("#window-banner", "请确认 docs/data/latest/digest.json 已经生成。");
  setText("#cycle-view", "如果这是 GitHub Pages，请检查 Actions 是否完成，或稍后刷新。");
  setText("#strategy-take", "前端只读取静态 JSON；如果数据不存在，页面不会自动生成内容。");
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

function shortenText(value, limit) {
  const text = String(value ?? "");
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
}
