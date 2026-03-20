document.addEventListener("DOMContentLoaded", () => {
  void bootstrapMonitor();
});

async function bootstrapMonitor() {
  try {
    const snapshot = await fetchJson("./data/raw/latest.json");
    renderMonitor(snapshot);
  } catch (error) {
    renderMonitorError(error);
  }
}

async function fetchJson(url) {
  const response = await fetch(url, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  return await response.json();
}

function renderMonitor(snapshot) {
  const generated = document.querySelector("#raw-generated");
  const container = document.querySelector("#monitor-grid");
  if (!generated || !container) {
    return;
  }

  generated.textContent = `${snapshot.generatedAt} · ${snapshot.categories.length} 个板块`;
  container.innerHTML = snapshot.categories
    .map(
      (category) => `
        <article class="monitor-card">
          <div class="monitor-title-row">
            <div>
              <p class="eyebrow">Raw Category</p>
              <h4>${escapeHtml(category.name)}</h4>
            </div>
            <span class="monitor-count">${category.items.length} 条</span>
          </div>
          <p class="monitor-copy">${escapeHtml(category.description)}</p>
          <div class="monitor-meta">
            <span class="story-meta">${escapeHtml(category.sourceIds.join(", "))}</span>
          </div>
          <ol class="monitor-list">
            ${category.items
              .slice(0, 20)
              .map(
                (item) => `
                  <li class="monitor-item">
                    <a href="${escapeAttribute(item.url)}" target="_blank" rel="noreferrer">${escapeHtml(item.title)}</a>
                    <div class="story-meta">${escapeHtml(item.sourceLabel)}${item.publishedAt ? ` · ${escapeHtml(item.publishedAt)}` : ""}</div>
                  </li>
                `,
              )
              .join("")}
          </ol>
        </article>
      `,
    )
    .join("");
}

function renderMonitorError(error) {
  const generated = document.querySelector("#raw-generated");
  const container = document.querySelector("#monitor-grid");
  if (generated) {
    generated.textContent = "加载失败";
  }
  if (container) {
    container.innerHTML = `
      <article class="monitor-card">
        <h4>原始数据载入失败</h4>
        <p>${escapeHtml(error?.message ?? "无法读取 raw 数据")}</p>
      </article>
    `;
  }
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
