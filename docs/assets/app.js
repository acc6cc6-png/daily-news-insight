const state = {
  pageId: "home",
  digest: null,
  history: [],
  rawSnapshot: null,
  activeCategoryId: null,
};

const PAGE_GROUPS = [
  {
    label: "主页面",
    items: [
      { id: "home", title: "主页面", href: "./index.html", description: "今天先看什么、站内入口和当前窗口。" },
    ],
  },
  {
    label: "七个功能页",
    items: [
      { id: "trade", title: "今日交易指北", href: "./trade.html", description: "市场偏向、观察项、领涨承压与操作节奏。" },
      { id: "pulse", title: "跨市场主线", href: "./pulse.html", description: "把当天真正影响定价的主线单独拎出来。" },
      { id: "priority", title: "重点影响排序", href: "./priority.html", description: "只看重点，不在这里堆全天所有内容。" },
      { id: "feed", title: "全天全量动态", href: "./feed.html", description: "完整时间流，适合查漏补缺。" },
      { id: "boards", title: "板块策略", href: "./boards.html", description: "周期判断、策略建议和联想链路。" },
      { id: "comments", title: "多角色观察", href: "./comments.html", description: "不同角色怎么看同一批新闻。" },
      { id: "sources", title: "来源与数据出口", href: "./sources.html", description: "核对来源、查看最新 JSON 和数据出口。" },
    ],
  },
  {
    label: "工具页",
    items: [
      { id: "monitor", title: "原始监控", href: "./monitor.html", description: "检查原始抓取结果和源头质量。" },
      { id: "about", title: "关于本站", href: "./about.html", description: "了解更新机制、部署方式和作者信息。" },
    ],
  },
];

const CATEGORY_PAGE_IDS = new Set(["home", "priority", "feed", "boards", "comments", "sources"]);
const BOARD_FALLBACK_PAGE = "boards";
const STORAGE_KEY = "daily-news-active-category";

document.addEventListener("DOMContentLoaded", () => {
  void bootstrap();
});

async function bootstrap() {
  state.pageId = document.body.dataset.page || "home";

  try {
    const requests = [
      fetchJson("./data/latest/digest.json"),
      fetchJson("./data/history/index.json", []),
    ];

    if (state.pageId === "monitor") {
      requests.push(fetchJson("./data/raw/latest.json", null));
    }

    const [digest, history, rawSnapshot] = await Promise.all(requests);
    state.digest = digest;
    state.history = Array.isArray(history) ? history : [];
    state.rawSnapshot = rawSnapshot ?? null;
    state.activeCategoryId = resolveInitialCategoryId();
    persistCategory(state.activeCategoryId);

    renderSidebar();
    renderPage();
    bindGlobalEvents();
  } catch (error) {
    console.error(error);
    renderSidebar();
    renderFailure(error);
  }
}

function bindGlobalEvents() {
  document.addEventListener("click", (event) => {
    const categoryLink = event.target.closest("[data-category-id]");
    if (categoryLink) {
      persistCategory(categoryLink.getAttribute("data-category-id"));
    }
  });
}

async function fetchJson(url, fallback = undefined) {
  try {
    const response = await fetch(url, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch (error) {
    if (fallback !== undefined) {
      return fallback;
    }
    throw error;
  }
}

function resolveInitialCategoryId() {
  const categories = state.digest?.categories || [];
  const queryValue = new URLSearchParams(window.location.search).get("category");
  const storedValue = readStoredCategory();
  const fallback = categories[0]?.id ?? null;
  const candidate = [queryValue, storedValue, "focus-news", fallback].find(Boolean);
  return categories.some((item) => item.id === candidate) ? candidate : fallback;
}

function readStoredCategory() {
  try {
    return window.localStorage.getItem(STORAGE_KEY);
  } catch (_error) {
    return null;
  }
}

function persistCategory(categoryId) {
  if (!categoryId) {
    return;
  }
  try {
    window.localStorage.setItem(STORAGE_KEY, categoryId);
  } catch (_error) {
    // Ignore storage issues in private browsing or locked-down environments.
  }
}

function renderSidebar() {
  const site = state.digest?.site ?? {};
  const edition = state.digest?.edition ?? {};
  const activeCategory = getActiveCategory();
  const pageId = state.pageId;

  setText("#brand-title", site.title || "小程AI的新闻日报");
  setText("#brand-subtitle", site.subtitle || "跨市场重点新闻与投研摘要");
  setText("#sidebar-window", edition.windowLabel || "等待最新窗口");
  setText("#sidebar-generated", edition.generatedAt ? `最新更新时间 ${edition.generatedAt}` : "等待最新快照");
  setText("#contact-author", site.author || "乐毅");
  setText("#contact-wechat", site.contact_wechat || "cfu800");
  setText("#contact-email-value", site.contact_email || "3119268060@qq.com");
  setHref("#contact-email-link", `mailto:${site.contact_email || "3119268060@qq.com"}`);
  setHref("#repo-link", site.repo_url || "https://github.com/acc6cc6-png/daily-news-insight");

  const nav = document.querySelector("#sidebar-nav");
  if (nav) {
    nav.innerHTML = PAGE_GROUPS.map((group) => {
      const links = group.items
        .map((item) => {
          const active = item.id === pageId ? " is-active" : "";
          return `
            <a class="nav-link${active}" href="${escapeAttribute(item.href)}">
              <span class="nav-link-title">${escapeHtml(item.title)}</span>
              <span class="nav-link-copy">${escapeHtml(item.description)}</span>
            </a>
          `;
        })
        .join("");

      return `
        <section class="nav-group">
          <p class="nav-group-label">${escapeHtml(group.label)}</p>
          <div class="nav-group-items">${links}</div>
        </section>
      `;
    }).join("");
  }

  const categoryTargetPage = CATEGORY_PAGE_IDS.has(pageId) ? pageId : BOARD_FALLBACK_PAGE;
  setText("#sidebar-category-caption", CATEGORY_PAGE_IDS.has(pageId) ? "当前板块上下文" : "快速进入板块页");
  setText(
    "#sidebar-category-summary",
    activeCategory
      ? `${activeCategory.name} · ${activeCategory.stats.priorityCount} 条重点 / ${activeCategory.stats.allStoryCount} 条动态`
      : "等待板块数据",
  );

  const categoryList = document.querySelector("#sidebar-categories");
  if (categoryList) {
    categoryList.innerHTML = (state.digest?.categories || [])
      .map((category) => {
        const active = category.id === state.activeCategoryId ? " is-active" : "";
        return `
          <a
            class="category-link${active}"
            href="${escapeAttribute(buildPageHref(categoryTargetPage, category.id))}"
            data-category-id="${escapeAttribute(category.id)}"
          >
            <span class="category-link-title">${escapeHtml(category.name)}</span>
            <span class="category-link-copy">${category.stats.priorityCount} 条重点 · ${category.stats.allStoryCount} 条动态</span>
          </a>
        `;
      })
      .join("");
  }
}

function renderPage() {
  const root = document.querySelector("#page-root");
  if (!root || !state.digest) {
    return;
  }

  const renderers = {
    home: renderHomePage,
    trade: renderTradePage,
    pulse: renderPulsePage,
    priority: renderPriorityPage,
    feed: renderFeedPage,
    boards: renderBoardsPage,
    comments: renderCommentsPage,
    sources: renderSourcesPage,
    monitor: renderMonitorPage,
    about: renderAboutPage,
  };

  const renderer = renderers[state.pageId] || renderHomePage;
  root.innerHTML = renderer();

  const siteTitle = state.digest.site?.title || "小程AI的新闻日报";
  const pageTitle = pageName(state.pageId);
  document.title = `${pageTitle} | ${siteTitle}`;
}

function renderFailure(error) {
  const root = document.querySelector("#page-root");
  if (!root) {
    return;
  }

  root.innerHTML = `
    <section class="page-hero">
      <div class="hero-copy">
        <p class="eyebrow">Load Error</p>
        <h1>页面暂时没有加载成功</h1>
        <p class="hero-text">请稍后刷新，或者先检查 GitHub Actions 是否已经完成最新一次构建。</p>
      </div>
    </section>
    <section class="surface error-card">
      <p class="section-copy">${escapeHtml(error?.message || "无法读取最新数据。")}</p>
      <div class="action-row">
        <a class="action-link" href="./monitor.html">去原始监控页排查</a>
        <a class="action-link action-link--secondary" href="./about.html">查看部署说明</a>
      </div>
    </section>
  `;
}

function renderHomePage() {
  const activeCategory = getActiveCategory();
  const compass = state.digest.tradeCompass ?? {};
  const pulse = state.digest.marketPulse ?? {};
  const focusCategory = getCategoryById("focus-news") || activeCategory;

  const hero = renderHero({
    kicker: "Main Dashboard",
    title: "小程AI的新闻日报",
    description: "首页只保留今天最该先看的内容和七个功能入口，不再把所有模块堆在一张长页里。",
    metrics: [
      { label: "当前窗口", value: state.digest.edition?.windowLabel || "-" },
      { label: "最新更新", value: state.digest.edition?.generatedAt || "-" },
      { label: "市场偏向", value: compass.biasLabel || "等待判断" },
      { label: "历史快照", value: `${state.history.length} 份` },
    ],
  });

  return `
    <div class="page-stack page-stack--home">
    ${renderMobileShellBar({ includeCategories: true })}
    ${hero}

    <section class="dashboard-grid">
      <article class="surface surface--dark">
        <p class="eyebrow">Start Here</p>
        <h2 class="section-title">今天先看什么</h2>
        <p class="section-copy section-copy--light">${escapeHtml(compass.summary || "等待今日交易结论。")}</p>
        <div class="chip-row">
          ${(compass.drivers || []).slice(0, 4).map(renderChip).join("")}
        </div>
        <div class="action-row">
          <a class="action-link" href="./trade.html">进入今日交易指北</a>
        </div>
      </article>

      <article class="surface">
        <p class="eyebrow">Cross Market</p>
        <h2 class="section-title">当前主线快照</h2>
        <p class="section-copy">${escapeHtml(pulse.headline || "等待跨市场主线。")}</p>
        <div class="stack-list">
          ${(pulse.highlights || []).slice(0, 3).map((story) => renderCompactBullet(story.title, story.categoryName)).join("")}
        </div>
        <div class="action-row">
          <a class="action-link action-link--secondary" href="./pulse.html">查看跨市场主线页</a>
        </div>
      </article>

      <article class="surface">
        <p class="eyebrow">Selected Board</p>
        <h2 class="section-title">${escapeHtml(activeCategory?.name || "当前板块")}</h2>
        <p class="section-copy">${escapeHtml(activeCategory?.strategyTake || activeCategory?.lead || "等待板块内容。")}</p>
        <div class="stack-list">
          ${renderMetricLine("重点数量", `${activeCategory?.stats?.priorityCount || 0} 条`)}
          ${renderMetricLine("全天动态", `${activeCategory?.stats?.allStoryCount || 0} 条`)}
          ${renderMetricLine("第一条重点", shortenText(activeCategory?.priorityStories?.[0]?.title || "暂无", 42))}
        </div>
        <div class="action-row">
          <a class="action-link action-link--secondary" href="${escapeAttribute(buildPageHref("boards", activeCategory?.id))}">
            进入板块策略页
          </a>
        </div>
      </article>
    </section>

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Feature Pages</p>
          <h2 class="section-title">七个功能页面</h2>
        </div>
        <p class="section-hint">每个页面只负责一个功能，后面改版和查问题都会清楚很多。</p>
      </div>
      <div class="module-grid">
        ${renderModuleCards(activeCategory, focusCategory)}
      </div>
    </section>
    </div>
  `;
}

function renderTradePage() {
  const compass = state.digest.tradeCompass ?? {};

  return `
    <div class="page-stack page-stack--trade">
    ${renderMobileShellBar({ includeCategories: false })}
    ${renderHero({
      kicker: "Trading Guide",
      title: compass.title || "今日交易指北",
      description: compass.summary || "先看今天偏什么，再看观察项和节奏。",
      metrics: [
        { label: "市场偏向", value: compass.biasLabel || "等待判断" },
        { label: "观察项", value: `${(compass.watchlist || []).length} 个` },
        { label: "偏多候选", value: `${(compass.leaders || []).length} 个` },
        { label: "承压候选", value: `${(compass.laggards || []).length} 个` },
      ],
    })}

    <section class="dashboard-grid">
      <article class="surface surface--dark">
        <p class="eyebrow">Drivers</p>
        <h2 class="section-title">今天先盯的变量</h2>
        <div class="chip-row">
          ${(compass.drivers || []).map(renderChip).join("")}
        </div>
        <div class="subsurface">
          <p class="subsurface-label">开盘前观察项</p>
          <div class="chip-row chip-row--soft">
            ${(compass.watchlist || []).map(renderSoftChip).join("")}
          </div>
        </div>
      </article>

      <article class="surface">
        <p class="eyebrow">Playbook</p>
        <h2 class="section-title">操作节奏</h2>
        <div class="stack-list">
          ${(compass.playbook || []).map((item, index) => renderNumberedItem(index + 1, item)).join("")}
        </div>
      </article>
    </section>

    <section class="triple-grid">
      ${renderTradeColumn("偏多候选", "bullish", compass.leaders)}
      ${renderTradeColumn("承压候选", "bearish", compass.laggards)}
      ${renderTradeColumn("中性观察", "watch", compass.stable)}
    </section>
    </div>
  `;
}

function renderPulsePage() {
  const pulse = state.digest.marketPulse ?? {};
  const highlights = pulse.highlights || [];

  return `
    <div class="page-stack page-stack--pulse">
    ${renderMobileShellBar({ includeCategories: false })}
    ${renderHero({
      kicker: "Cross Market",
      title: "跨市场主线",
      description: pulse.headline || "把最影响今天定价的主线单独拎出来。",
      metrics: [
        { label: "主线条数", value: `${highlights.length} 条` },
        { label: "统计窗口", value: pulse.windowLabel || state.digest.edition?.windowLabel || "-" },
        { label: "主导类别", value: highlights[0]?.categoryName || "等待主线" },
        { label: "最新来源", value: highlights[0]?.source || "等待数据" },
      ],
    })}

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Highlights</p>
          <h2 class="section-title">当天最重要的跨市场主线</h2>
        </div>
        <p class="section-hint">这里不按时间排，只看今天真正会改写市场定价顺序的事情。</p>
      </div>
      <div class="card-grid card-grid--three">
        ${highlights.map(renderPulseCard).join("") || renderEmptyCard("暂时还没有跨市场主线。")}
      </div>
    </section>
    </div>
  `;
}

function renderPriorityPage() {
  const category = getActiveCategory();

  return `
    <div class="page-stack page-stack--priority">
    ${renderMobileShellBar({ includeCategories: true })}
    ${renderCategoryHero("重点影响排序", category, category?.lead || "重点区只保留按影响力排序的内容。")}
    ${renderCategoryTabs("priority", category?.id)}

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Impact Rank</p>
          <h2 class="section-title">${escapeHtml(category?.name || "板块")}重点排序</h2>
        </div>
        <p class="section-hint">不是按发布时间，而是按影响程度和当前窗口相关性排序。</p>
      </div>
      <div class="story-list">
        ${(category?.priorityStories || []).map((story) => renderStoryCard(story, "priority")).join("") || renderEmptyCard("当前板块还没有重点排序内容。")}
      </div>
    </section>
    </div>
  `;
}

function renderFeedPage() {
  const category = getActiveCategory();

  return `
    <div class="page-stack page-stack--feed">
    ${renderMobileShellBar({ includeCategories: true })}
    ${renderCategoryHero("全天全量动态", category, "完整时间流只放在这一页，首页不再堆满全部内容。")}
    ${renderCategoryTabs("feed", category?.id)}

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Full Feed</p>
          <h2 class="section-title">${escapeHtml(category?.name || "板块")}全天动态</h2>
        </div>
        <p class="section-hint">适合查漏补缺，不适合拿它当第一页。</p>
      </div>
      <div class="feed-list">
        ${(category?.allStories || []).map(renderFeedRow).join("") || renderEmptyCard("当前板块还没有全天动态。")}
      </div>
    </section>
    </div>
  `;
}

function renderBoardsPage() {
  const category = getActiveCategory();

  return `
    <div class="page-stack page-stack--boards">
    ${renderMobileShellBar({ includeCategories: true })}
    ${renderCategoryHero("板块策略", category, category?.cycleView || "把当前板块的周期判断和策略建议单独放在这一页。")}
    ${renderCategoryTabs("boards", category?.id)}

    <section class="dashboard-grid">
      <article class="surface">
        <p class="eyebrow">Cycle View</p>
        <h2 class="section-title">周期判断</h2>
        <p class="section-copy">${escapeHtml(category?.cycleView || "等待周期判断。")}</p>
      </article>
      <article class="surface">
        <p class="eyebrow">Strategy Take</p>
        <h2 class="section-title">策略建议</h2>
        <p class="section-copy">${escapeHtml(category?.strategyTake || "等待策略建议。")}</p>
      </article>
    </section>

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Linkage Ideas</p>
          <h2 class="section-title">联想链路</h2>
        </div>
        <p class="section-hint">不是机械套公式，而是把 headline 拆成更有交易价值的传导链。</p>
      </div>
      <div class="stack-list">
        ${(category?.linkageIdeas || []).map((item, index) => renderNumberedItem(index + 1, item)).join("") || renderEmptyCard("当前板块还没有联想链路。")}
      </div>
    </section>

    <section class="dashboard-grid">
      <article class="surface surface--dark">
        <p class="eyebrow">Economist Take</p>
        <h2 class="section-title">经济分析视角</h2>
        <p class="section-copy section-copy--light">${escapeHtml(category?.economistTake || "等待分析视角。")}</p>
      </article>

      <article class="surface">
        <p class="eyebrow">Signal View</p>
        <h2 class="section-title">顺风 / 逆风 / 观察</h2>
        <div class="signal-grid">
          ${renderSignalCard("顺风线索", "bullish", category?.bullish)}
          ${renderSignalCard("逆风线索", "bearish", category?.bearish)}
          ${renderSignalCard("继续观察", "watch", category?.watch)}
        </div>
      </article>
    </section>
    </div>
  `;
}

function renderCommentsPage() {
  const category = getActiveCategory();
  const comments = category?.personaComments || [];

  return `
    <div class="page-stack page-stack--comments">
    ${renderMobileShellBar({ includeCategories: true })}
    ${renderCategoryHero("多角色观察", category, "同一批新闻，用不同角色去看，避免只剩下一种表达口径。")}
    ${renderCategoryTabs("comments", category?.id)}

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Multi View</p>
          <h2 class="section-title">${escapeHtml(category?.name || "板块")}多角色观察</h2>
        </div>
        <p class="section-hint">这页只放角色评论，和别的模块彻底拆开。</p>
      </div>
      <div class="comment-grid">
        ${comments.map(renderCommentCard).join("") || renderEmptyCard("当前板块还没有角色评论。")}
      </div>
    </section>
    </div>
  `;
}

function renderSourcesPage() {
  const category = getActiveCategory();
  const sources = category?.sourcesUsed || [];

  return `
    <div class="page-stack page-stack--sources">
    ${renderMobileShellBar({ includeCategories: true })}
    ${renderCategoryHero("来源与数据出口", category, "核对来源、原文和 JSON 出口都放到一个单独页面里。")}
    ${renderCategoryTabs("sources", category?.id)}

    <section class="dashboard-grid">
      <article class="surface">
        <p class="eyebrow">Source Mix</p>
        <h2 class="section-title">当前板块来源</h2>
        <div class="source-grid">
          ${sources.map(renderSourceCard).join("") || renderEmptyCard("当前板块还没有来源列表。")}
        </div>
      </article>

      <article class="surface">
        <p class="eyebrow">Data Export</p>
        <h2 class="section-title">数据出口</h2>
        <div class="stack-list">
          ${renderActionCard("./data/latest/digest.json", "最新 digest.json", "适合前端读取和调试最终输出。")}
          ${renderActionCard("./data/raw/latest.json", "最新 raw.json", "适合查抓取源、时间戳和原始标题。")}
          ${renderActionCard("./data/history/index.json", "历史索引", "适合回看不同时间窗口的快照。")}
        </div>
      </article>
    </section>
    </div>
  `;
}

function renderMonitorPage() {
  const snapshot = state.rawSnapshot;

  return `
    <div class="page-stack page-stack--monitor">
    ${renderMobileShellBar({ includeCategories: false })}
    ${renderHero({
      kicker: "Raw Monitor",
      title: "原始监控",
      description: "如果重点新闻不对，先查这页，看问题出在抓取源还是排序层。",
      metrics: [
        { label: "最新抓取", value: snapshot?.generatedAt || "-" },
        { label: "原始分类", value: `${snapshot?.categories?.length || 0} 个` },
        { label: "接入来源", value: `${snapshot?.sourceCount || 0} 个` },
        { label: "统计窗口", value: snapshot?.windowLabel || "-" },
      ],
    })}

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Raw Categories</p>
          <h2 class="section-title">各板块原始抓取情况</h2>
        </div>
        <p class="section-hint">这里显示原始样本，方便判断是新闻源偏了，还是摘要层偏了。</p>
      </div>
      <div class="monitor-grid">
        ${(snapshot?.categories || []).map(renderMonitorCard).join("") || renderEmptyCard("当前还没有原始抓取结果。")}
      </div>
    </section>
    </div>
  `;
}

function renderAboutPage() {
  const site = state.digest.site ?? {};

  return `
    <div class="page-stack page-stack--about">
    ${renderMobileShellBar({ includeCategories: false })}
    ${renderHero({
      kicker: "About",
      title: "关于本站",
      description: "这次改成多页面结构，是为了让用户先找到对的功能页，而不是在首页里迷路。",
      metrics: [
        { label: "更新节奏", value: "07:00 / 12:30" },
        { label: "托管方式", value: "GitHub Pages" },
        { label: "作者", value: site.author || "乐毅" },
        { label: "当前仓库", value: "daily-news-insight" },
      ],
    })}

    <section class="dashboard-grid">
      <article class="surface">
        <p class="eyebrow">Why It Changed</p>
        <h2 class="section-title">为什么改成多页面</h2>
        <div class="stack-list">
          ${renderBullet("首页只保留摘要和入口，不再堆七个模块的全部内容。")}
          ${renderBullet("七个核心模块拆成独立网页，后期每个页面都能单独迭代。")}
          ${renderBullet("板块相关页面统一带 category 参数，后续加筛选和跳转会更顺。")}
        </div>
      </article>

      <article class="surface">
        <p class="eyebrow">Update Logic</p>
        <h2 class="section-title">更新机制</h2>
        <div class="stack-list">
          ${renderBullet("工作日北京时间 07:00 更新昨收后到早上 07:00 的消息。")}
          ${renderBullet("工作日北京时间 12:30 更新当天 07:00 到 12:30 的消息。")}
          ${renderBullet("页面、脚本和配置 push 到 main 后，也会自动触发一次重新部署。")}
        </div>
      </article>
    </section>

    <section class="surface">
      <div class="section-head">
        <div>
          <p class="eyebrow">Contact</p>
          <h2 class="section-title">作者与联系</h2>
        </div>
      </div>
      <div class="card-grid card-grid--three">
        ${renderAboutCard("作者", site.author || "乐毅")}
        ${renderAboutCard("微信", site.contact_wechat || "cfu800")}
        ${renderAboutCard("邮箱", site.contact_email || "3119268060@qq.com", `mailto:${site.contact_email || "3119268060@qq.com"}`)}
      </div>
      <div class="action-row">
        <a class="action-link action-link--secondary" href="${escapeAttribute(site.repo_url || "https://github.com/acc6cc6-png/daily-news-insight")}" target="_blank" rel="noreferrer">
          打开 GitHub 仓库
        </a>
      </div>
    </section>
    </div>
  `;
}

function renderHero({ kicker, title, description, metrics }) {
  return `
    <section class="page-hero">
      <div class="hero-grid">
        <div class="hero-copy">
          <p class="eyebrow">${escapeHtml(kicker)}</p>
          <h1>${escapeHtml(title)}</h1>
          <p class="hero-text">${escapeHtml(description)}</p>
        </div>
        <div class="metric-grid">
          ${(metrics || []).map(renderMetricCard).join("")}
        </div>
      </div>
    </section>
  `;
}

function renderMobileShellBar({ includeCategories }) {
  const category = getActiveCategory();
  const siteTitle = state.digest?.site?.title || "小程AI的新闻日报";
  const pageLabel = pageName(state.pageId);
  const quickPages = ["home", "trade", "pulse", "priority", "feed", "boards"];

  return `
    <section class="mobile-shellbar">
      <div class="mobile-shellbar-head">
        <div>
          <p class="eyebrow">手机快览</p>
          <h2>${escapeHtml(pageLabel)}</h2>
        </div>
        <div class="mobile-shellbar-status">
          <span>${escapeHtml(siteTitle)}</span>
          <strong>${escapeHtml(state.digest?.edition?.windowLabel || "-")}</strong>
        </div>
      </div>

      <div class="mobile-shellbar-scroll">
        ${quickPages
          .map((pageId) => {
            const active = pageId === state.pageId ? " is-active" : "";
            return `
              <a class="mobile-pill${active}" href="${escapeAttribute(buildPageHref(pageId, category?.id))}">
                ${escapeHtml(pageName(pageId))}
              </a>
            `;
          })
          .join("")}
      </div>

      ${
        includeCategories
          ? `
            <div class="mobile-shellbar-scroll mobile-shellbar-scroll--boards">
              ${(state.digest?.categories || [])
                .map((item) => {
                  const active = item.id === state.activeCategoryId ? " is-active" : "";
                  return `
                    <a class="mobile-board-pill${active}" href="${escapeAttribute(buildPageHref(state.pageId, item.id))}">
                      ${escapeHtml(item.name)}
                    </a>
                  `;
                })
                .join("")}
            </div>
          `
          : `
            <div class="mobile-shellbar-note">
              <span>当前更新时间</span>
              <strong>${escapeHtml(state.digest?.edition?.generatedAt || "-")}</strong>
            </div>
          `
      }
    </section>
  `;
}

function renderCategoryHero(pageLabel, category, description) {
  return renderHero({
    kicker: pageLabel,
    title: `${category?.name || "板块"} · ${pageLabel}`,
    description,
    metrics: [
      { label: "当前板块", value: category?.name || "-" },
      { label: "重点数量", value: `${category?.stats?.priorityCount || 0} 条` },
      { label: "全天动态", value: `${category?.stats?.allStoryCount || 0} 条` },
      { label: "统计窗口", value: category?.windowLabel || state.digest.edition?.windowLabel || "-" },
    ],
  });
}

function renderCategoryTabs(activePageId, categoryId) {
  const tabs = [
    { id: "priority", title: "重点排序" },
    { id: "feed", title: "全天动态" },
    { id: "boards", title: "板块策略" },
    { id: "comments", title: "多角色观察" },
    { id: "sources", title: "来源出口" },
  ];

  return `
    <nav class="page-tabs" aria-label="板块子页面切换">
      ${tabs
        .map((tab) => {
          const active = tab.id === activePageId ? " is-active" : "";
          return `
            <a class="page-tab${active}" href="${escapeAttribute(buildPageHref(tab.id, categoryId))}">
              ${escapeHtml(tab.title)}
            </a>
          `;
        })
        .join("")}
    </nav>
  `;
}

function renderModuleCards(activeCategory, focusCategory) {
  const compass = state.digest.tradeCompass ?? {};
  const pulse = state.digest.marketPulse ?? {};
  const cards = [
    {
      href: "./trade.html",
      title: "今日交易指北",
      badge: compass.biasLabel || "等待判断",
      summary: compass.summary || "先看偏向、观察项和板块强弱。",
      meta: `${(compass.watchlist || []).length} 个观察项`,
    },
    {
      href: "./pulse.html",
      title: "跨市场主线",
      badge: `${(pulse.highlights || []).length} 条主线`,
      summary: pulse.headline || "把真正影响市场定价的主线单独拎出来。",
      meta: pulse.windowLabel || state.digest.edition?.windowLabel || "-",
    },
    {
      href: buildPageHref("priority", focusCategory?.id),
      title: "重点影响排序",
      badge: `${focusCategory?.stats?.priorityCount || 0} 条重点`,
      summary: focusCategory?.priorityStories?.[0]?.title || "只保留重点，按影响排序。",
      meta: focusCategory?.name || "重点新闻",
    },
    {
      href: buildPageHref("feed", activeCategory?.id),
      title: "全天全量动态",
      badge: `${activeCategory?.stats?.allStoryCount || 0} 条动态`,
      summary: activeCategory?.lead || "完整时间流只放在这一页。",
      meta: activeCategory?.windowLabel || "-",
    },
    {
      href: buildPageHref("boards", activeCategory?.id),
      title: "板块策略",
      badge: activeCategory?.name || "当前板块",
      summary: activeCategory?.strategyTake || activeCategory?.cycleView || "周期判断和策略建议。",
      meta: shortenText(activeCategory?.lens || "观察镜头", 20),
    },
    {
      href: buildPageHref("comments", activeCategory?.id),
      title: "多角色观察",
      badge: `${activeCategory?.personaComments?.length || 0} 个角色`,
      summary: activeCategory?.personaComments?.[0]?.content || "同一批新闻，换不同角色去看。",
      meta: activeCategory?.personaComments?.[0]?.role || activeCategory?.name || "当前板块",
    },
    {
      href: buildPageHref("sources", activeCategory?.id),
      title: "来源与数据出口",
      badge: `${activeCategory?.sourcesUsed?.length || 0} 个来源`,
      summary: buildSourceSummary(activeCategory),
      meta: "查看原文、来源和 JSON 出口",
    },
  ];

  return cards.map(renderModuleCard).join("");
}

function renderModuleCard(card) {
  return `
    <article class="module-card">
      <span class="module-badge">${escapeHtml(card.badge)}</span>
      <h3>${escapeHtml(card.title)}</h3>
      <p>${escapeHtml(card.summary)}</p>
      <div class="module-footer">
        <span>${escapeHtml(card.meta)}</span>
        <a class="action-link action-link--secondary" href="${escapeAttribute(card.href)}">进入</a>
      </div>
    </article>
  `;
}

function renderMetricCard(metric) {
  return `
    <article class="metric-card">
      <span class="metric-label">${escapeHtml(metric.label)}</span>
      <strong>${escapeHtml(metric.value)}</strong>
    </article>
  `;
}

function renderTradeColumn(title, tone, items) {
  const values = Array.isArray(items) && items.length
    ? items
    : [{ name: "等待更多确认", reason: "当前窗口还不足以给出更明确的方向。" }];

  return `
    <section class="surface tone-card tone-card--${escapeAttribute(tone)}">
      <p class="eyebrow">${escapeHtml(title)}</p>
      <h2 class="section-title">${escapeHtml(title)}</h2>
      <div class="stack-list">
        ${values.map((item) => `
          <article class="mini-card">
            <strong>${escapeHtml(item.name)}</strong>
            <p>${escapeHtml(item.reason)}</p>
          </article>
        `).join("")}
      </div>
    </section>
  `;
}

function renderPulseCard(story) {
  return `
    <article class="story-card">
      <div class="story-meta">
        <span class="chip">${escapeHtml(story.categoryName)}</span>
        <span class="chip chip--${escapeAttribute(signalClass(story.signal))}">${escapeHtml(story.impactLabel || story.signalLabel || "重点")}</span>
      </div>
      <h3>${escapeHtml(story.title)}</h3>
      <p>${escapeHtml(story.impactReason || story.reason || "")}</p>
      <div class="story-footer">
        <span>${escapeHtml(buildStoryFooter(story))}</span>
        <a class="text-link" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">原文</a>
      </div>
    </article>
  `;
}

function renderStoryCard(story, kind) {
  const rank = kind === "priority" ? `#${story.priorityRank || "-"}` : `#${story.index || "-"}`;
  return `
    <article class="story-card story-card--priority">
      <div class="story-meta">
        <span class="chip">${escapeHtml(rank)}</span>
        <span class="chip chip--${escapeAttribute(signalClass(story.signal))}">${escapeHtml(story.impactLabel || story.signalLabel || "观察")}</span>
        ${story.inWindow ? '<span class="chip chip--window">当前窗口</span>' : ""}
      </div>
      <h3>${escapeHtml(story.title)}</h3>
      <p class="story-summary">${escapeHtml(story.summary || "")}</p>
      <p>${escapeHtml(story.impactReason || story.reason || "")}</p>
      ${story.sourceTitle ? `<p class="story-origin">原始标题：${escapeHtml(story.sourceTitle)}</p>` : ""}
      <div class="story-footer">
        <span>${escapeHtml(buildStoryFooter(story))}</span>
        <a class="text-link" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">原文</a>
      </div>
    </article>
  `;
}

function renderFeedRow(story) {
  return `
    <article class="feed-row">
      <div class="feed-rank">${escapeHtml(String(story.index || "-"))}</div>
      <div class="feed-main">
        <div class="story-meta">
          <span class="chip chip--${escapeAttribute(signalClass(story.signal))}">${escapeHtml(story.signalLabel || story.impactLabel || "观察")}</span>
          ${story.inWindow ? '<span class="chip chip--window">当前窗口</span>' : ""}
        </div>
        <h3>${escapeHtml(story.title)}</h3>
        <p>${escapeHtml(story.summary || "")}</p>
        <div class="feed-footnote">${escapeHtml(buildStoryFooter(story))}</div>
      </div>
      <a class="action-link action-link--secondary" href="${escapeAttribute(story.url)}" target="_blank" rel="noreferrer">原文</a>
    </article>
  `;
}

function renderSignalCard(title, tone, copy) {
  return `
    <article class="signal-card signal-card--${escapeAttribute(tone)}">
      <span class="signal-label">${escapeHtml(title)}</span>
      <p>${escapeHtml(copy || "等待更多信号。")}</p>
    </article>
  `;
}

function renderCommentCard(comment) {
  return `
    <article class="comment-card">
      <div class="comment-head">
        <strong>${escapeHtml(comment.name)}</strong>
        <span class="chip">${escapeHtml(comment.role)}</span>
      </div>
      <p class="comment-emotion">${escapeHtml(comment.emotion || "观察")}</p>
      <p>${escapeHtml(comment.content || "")}</p>
    </article>
  `;
}

function renderSourceCard(source) {
  const full = getSourceRegistryItem(source.id) || source;
  return `
    <a class="source-card" href="${escapeAttribute(full.home || "#")}" target="_blank" rel="noreferrer">
      <div>
        <strong>${escapeHtml(full.label || source.label || source.id)}</strong>
        <p>${escapeHtml(sourceKindLabel(full.kind))}</p>
      </div>
      <span class="chip">${escapeHtml(source.id || "-")}</span>
    </a>
  `;
}

function renderMonitorCard(category) {
  return `
    <article class="monitor-card">
      <div class="monitor-head">
        <div>
          <p class="eyebrow">Raw Category</p>
          <h3>${escapeHtml(category.name)}</h3>
        </div>
        <span class="chip">${escapeHtml(String(category.itemCount || category.items?.length || 0))} 条</span>
      </div>
      <p class="section-copy">${escapeHtml(category.description || "")}</p>
      <p class="monitor-meta-copy">来源：${escapeHtml((category.sourceIds || []).join(", "))}</p>
      <div class="stack-list stack-list--compact">
        ${(category.items || []).slice(0, 12).map((item) => `
          <a class="monitor-item" href="${escapeAttribute(item.url)}" target="_blank" rel="noreferrer">
            <strong>${escapeHtml(item.title)}</strong>
            <span>${escapeHtml(item.sourceLabel || item.sourceId || "")}${item.publishedAt ? ` · ${escapeHtml(item.publishedAt)}` : ""}</span>
          </a>
        `).join("")}
      </div>
    </article>
  `;
}

function renderActionCard(href, title, copy) {
  return `
    <a class="action-card" href="${escapeAttribute(href)}" target="_blank" rel="noreferrer">
      <strong>${escapeHtml(title)}</strong>
      <p>${escapeHtml(copy)}</p>
    </a>
  `;
}

function renderAboutCard(label, value, href = "") {
  if (href) {
    return `
      <a class="mini-card mini-card--link" href="${escapeAttribute(href)}">
        <span class="metric-label">${escapeHtml(label)}</span>
        <strong>${escapeHtml(value)}</strong>
      </a>
    `;
  }

  return `
    <article class="mini-card">
      <span class="metric-label">${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </article>
  `;
}

function renderCompactBullet(title, label = "") {
  return `
    <article class="mini-card mini-card--bullet">
      <strong>${escapeHtml(title)}</strong>
      ${label ? `<span>${escapeHtml(label)}</span>` : ""}
    </article>
  `;
}

function renderNumberedItem(index, copy) {
  return `
    <article class="numbered-item">
      <span class="numbered-index">${String(index).padStart(2, "0")}</span>
      <p>${escapeHtml(copy)}</p>
    </article>
  `;
}

function renderChip(value) {
  return `<span class="chip">${escapeHtml(value)}</span>`;
}

function renderSoftChip(value) {
  return `<span class="chip chip--soft">${escapeHtml(value)}</span>`;
}

function renderMetricLine(label, value) {
  return `
    <div class="metric-line">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(value)}</strong>
    </div>
  `;
}

function renderBullet(copy) {
  return `
    <article class="mini-card mini-card--bullet">
      <p>${escapeHtml(copy)}</p>
    </article>
  `;
}

function renderEmptyCard(copy) {
  return `<article class="mini-card mini-card--empty"><p>${escapeHtml(copy)}</p></article>`;
}

function buildSourceSummary(category) {
  const labels = (category?.sourcesUsed || [])
    .slice(0, 3)
    .map((item) => getSourceRegistryItem(item.id)?.label || item.label || item.id)
    .filter(Boolean);

  if (!labels.length) {
    return "核对来源、查看原始 JSON 和当前板块的数据出口。";
  }

  return `当前主要来源：${labels.join("、")}`;
}

function buildStoryFooter(story) {
  const pieces = [];
  if (story.source) {
    pieces.push(story.source);
  }
  if (story.publishedAt) {
    pieces.push(story.publishedAt);
  }
  return pieces.join(" · ") || "等待来源信息";
}

function getSourceRegistryItem(sourceId) {
  return (state.digest?.sourceRegistry || []).find((item) => item.id === sourceId) || null;
}

function getActiveCategory() {
  return getCategoryById(state.activeCategoryId);
}

function getCategoryById(categoryId) {
  return (state.digest?.categories || []).find((item) => item.id === categoryId) || null;
}

function buildPageHref(pageId, categoryId = "") {
  const file = pageId === "home" ? "./index.html" : `./${pageId}.html`;
  if (!categoryId || !CATEGORY_PAGE_IDS.has(pageId)) {
    return file;
  }
  return `${file}?category=${encodeURIComponent(categoryId)}`;
}

function pageName(pageId) {
  for (const group of PAGE_GROUPS) {
    const match = group.items.find((item) => item.id === pageId);
    if (match) {
      return match.title;
    }
  }
  return "主页面";
}

function sourceKindLabel(kind) {
  if (kind === "rss") {
    return "国际媒体 / RSS";
  }
  if (kind === "newsnow") {
    return "中文聚合 / 快讯";
  }
  return "数据来源";
}

function signalClass(signal) {
  if (signal === "bullish") {
    return "bullish";
  }
  if (signal === "bearish") {
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

function shortenText(value, limit) {
  const text = String(value ?? "");
  if (text.length <= limit) {
    return text;
  }
  return `${text.slice(0, Math.max(0, limit - 1))}…`;
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
