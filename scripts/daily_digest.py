from __future__ import annotations

import html
import json
import os
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

try:
    from openai import OpenAI
except Exception:
    OpenAI = None


ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = ROOT / "docs"
DATA_DIR = DOCS_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
LATEST_DIR = DATA_DIR / "latest"
HISTORY_DIR = DATA_DIR / "history"
CONFIG_PATH = ROOT / "config" / "sources.json"
LATEST_RAW_PATH = RAW_DIR / "latest.json"
LATEST_DIGEST_PATH = LATEST_DIR / "digest.json"
HISTORY_INDEX_PATH = HISTORY_DIR / "index.json"
NEWSNOW_ENDPOINT = "https://newsnow.busiyi.world/api/s/entire"
TZ = timezone(timedelta(hours=8))


BULLISH_KEYWORDS = (
    "增长",
    "新高",
    "签约",
    "合作",
    "融资",
    "扩产",
    "盈利",
    "回购",
    "发布",
    "上线",
    "获批",
    "落地",
    "反弹",
    "上涨",
    "订单",
    "突破",
)

BEARISH_KEYWORDS = (
    "下跌",
    "亏损",
    "裁员",
    "制裁",
    "调查",
    "警告",
    "风险",
    "冲突",
    "召回",
    "违约",
    "暴跌",
    "受损",
    "减产",
    "停牌",
    "处罚",
    "起诉",
    "火灾",
    "关闭",
    "叫停",
    "退市",
    "*ST",
)

WATCH_KEYWORDS = (
    "计划",
    "预计",
    "拟",
    "将",
    "可能",
    "传",
    "建议",
    "征求意见",
    "进展",
    "启动",
)


def main() -> int:
    load_local_env(ROOT / ".env.local")
    config = load_json(CONFIG_PATH)
    now = datetime.now(TZ)
    print(f"[daily-digest] build started at {now.isoformat()}")

    raw_by_source = fetch_all_sources(config)
    raw_snapshot = build_raw_snapshot(config, raw_by_source, now)
    write_json(LATEST_RAW_PATH, raw_snapshot)

    digest = build_digest(config, raw_by_source, now)
    write_json(LATEST_DIGEST_PATH, digest)
    archive_digest(digest, now)

    print(
        f"[daily-digest] completed with mode={digest['edition']['mode']} "
        f"categories={len(digest['categories'])}"
    )
    return 0


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_local_env(path: Path) -> None:
    if not path.exists():
        return

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def fetch_all_sources(config: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    sources = config["sources"]
    results: dict[str, list[dict[str, Any]]] = {source_id: [] for source_id in sources}

    newsnow_ids = [source_id for source_id, meta in sources.items() if meta["type"] == "newsnow"]
    if newsnow_ids:
        results.update(fetch_newsnow_sources(newsnow_ids, sources))

    for source_id, meta in sources.items():
        if meta["type"] != "rss":
            continue
        try:
            xml_bytes, final_url, _ = fetch_bytes(meta["url"])
            results[source_id] = parse_feed_items(xml_bytes, source_id, meta, final_url)
            print(f"[rss] {source_id}: {len(results[source_id])} items")
        except Exception as exc:  # noqa: BLE001
            print(f"[rss] {source_id}: failed -> {exc}")
            results[source_id] = []

    return results


def fetch_newsnow_sources(
    source_ids: list[str],
    source_registry: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    payload = json.dumps({"sources": source_ids}).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://newsnow.busiyi.world",
        "Referer": "https://newsnow.busiyi.world/",
        "User-Agent": "daily-news-insight/1.0",
    }
    try:
        data_bytes, _, _ = fetch_bytes(
            NEWSNOW_ENDPOINT,
            method="POST",
            data=payload,
            headers=headers,
        )
    except HTTPError as exc:
        if exc.code != 403:
            raise
        data_bytes = fetch_newsnow_with_curl(payload)
    raw_data = json.loads(data_bytes.decode("utf-8"))
    parsed: dict[str, list[dict[str, Any]]] = {source_id: [] for source_id in source_ids}

    for source_payload in raw_data:
        source_id = source_payload.get("id")
        if source_id not in parsed:
            continue

        meta = source_registry[source_id]
        items = []
        for rank, item in enumerate(source_payload.get("items", [])[:40], start=1):
            title = clean_text(item.get("title", ""))
            url = item.get("url") or item.get("mobileUrl") or "#"
            published_at = (
                coerce_timestamp(item.get("pubDate"))
                or coerce_timestamp(item.get("extra", {}).get("date"))
                or None
            )
            if not title:
                continue
            items.append(
                {
                    "title": title,
                    "url": url,
                    "publishedAt": published_at,
                    "sourceId": source_id,
                    "sourceLabel": meta["label"],
                    "sourceHome": meta["home"],
                    "sourceType": meta["type"],
                    "rank": rank,
                }
            )

        parsed[source_id] = items
        print(f"[newsnow] {source_id}: {len(items)} items")

    return parsed


def fetch_newsnow_with_curl(payload: bytes) -> bytes:
    curl_binary = shutil.which("curl") or shutil.which("curl.exe")
    if not curl_binary:
        raise RuntimeError("NewsNow returned 403 and curl is not available for fallback.")

    command = [
        curl_binary,
        "-sS",
        "-X",
        "POST",
        NEWSNOW_ENDPOINT,
        "-H",
        "Content-Type: application/json",
        "-H",
        "Origin: https://newsnow.busiyi.world",
        "-H",
        "Referer: https://newsnow.busiyi.world/",
        "-H",
        "User-Agent: Mozilla/5.0",
        "--data-raw",
        payload.decode("utf-8"),
    ]

    result = subprocess.run(command, capture_output=True, check=True)
    return result.stdout


def fetch_bytes(
    url: str,
    method: str = "GET",
    data: bytes | None = None,
    headers: dict[str, str] | None = None,
    max_redirects: int = 6,
) -> tuple[bytes, str, str]:
    current_url = url
    request_headers = {
        "User-Agent": "daily-news-insight/1.0",
        "Accept": "*/*",
    }
    if headers:
        request_headers.update(headers)

    for _ in range(max_redirects + 1):
        request = Request(current_url, data=data, headers=request_headers, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                return (
                    response.read(),
                    response.geturl(),
                    response.headers.get("Content-Type", ""),
                )
        except HTTPError as exc:
            if exc.code in {301, 302, 303, 307, 308}:
                location = exc.headers.get("Location")
                if not location:
                    raise
                current_url = urljoin(current_url, location)
                if exc.code == 303:
                    method = "GET"
                    data = None
                continue
            raise
        except URLError:
            raise

    raise RuntimeError(f"Too many redirects while fetching {url}")


def parse_feed_items(
    xml_bytes: bytes,
    source_id: str,
    meta: dict[str, Any],
    final_url: str,
) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    items: list[dict[str, Any]] = []

    if tag_name(root.tag) == "rss":
        for rank, item_node in enumerate(find_nodes(root, "item")[:40], start=1):
            title = clean_text(child_text(item_node, "title"))
            url = clean_text(child_text(item_node, "link"))
            published = child_text(item_node, "pubDate")
            if not title or not url:
                continue
            items.append(
                {
                    "title": title,
                    "url": url,
                    "publishedAt": coerce_timestamp(published),
                    "sourceId": source_id,
                    "sourceLabel": meta["label"],
                    "sourceHome": meta["home"],
                    "sourceType": meta["type"],
                    "rank": rank,
                }
            )
        return items

    if tag_name(root.tag) == "feed":
        for rank, entry_node in enumerate(find_nodes(root, "entry")[:40], start=1):
            title = clean_text(child_text(entry_node, "title"))
            url = ""
            for child in entry_node:
                if tag_name(child.tag) != "link":
                    continue
                href = child.attrib.get("href")
                rel = child.attrib.get("rel", "alternate")
                if href and rel == "alternate":
                    url = href
                    break
            published = (
                child_text(entry_node, "published")
                or child_text(entry_node, "updated")
                or child_text(entry_node, "issued")
            )
            if not title or not url:
                continue
            items.append(
                {
                    "title": title,
                    "url": urljoin(final_url, url),
                    "publishedAt": coerce_timestamp(published),
                    "sourceId": source_id,
                    "sourceLabel": meta["label"],
                    "sourceHome": meta["home"],
                    "sourceType": meta["type"],
                    "rank": rank,
                }
            )
        return items

    return items


def build_raw_snapshot(
    config: dict[str, Any],
    raw_by_source: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> dict[str, Any]:
    categories = []
    for category in config["categories"]:
        category_items = collect_category_items(category, raw_by_source, limit=20)
        categories.append(
            {
                "id": category["id"],
                "name": category["name"],
                "description": category["description"],
                "sourceIds": category["sources"],
                "items": category_items,
            }
        )

    return {
        "generatedAt": iso_clock(now),
        "timezone": config["site"]["timezone"],
        "sourceCount": len(config["sources"]),
        "categories": categories,
    }


def build_digest(
    config: dict[str, Any],
    raw_by_source: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> dict[str, Any]:
    ai_client = create_openai_client()
    mode = "ai" if ai_client is not None else "template"
    categories = []

    for category in config["categories"]:
        items = collect_category_items(category, raw_by_source, limit=30)
        if ai_client is not None and items:
            try:
                category_digest = generate_ai_category_digest(
                    ai_client,
                    config,
                    category,
                    items,
                )
            except Exception as exc:  # noqa: BLE001
                print(f"[ai] {category['id']}: failed -> {exc}")
                category_digest = build_template_category_digest(config, category, items)
                mode = "mixed"
        else:
            category_digest = build_template_category_digest(config, category, items)

        categories.append(category_digest)
        time.sleep(0.5)

    source_registry = [
        {
            "id": source_id,
            "label": meta["label"],
            "kind": meta["type"],
            "home": meta["home"],
        }
        for source_id, meta in config["sources"].items()
    ]

    story_count = sum(len(category["stories"]) for category in categories)
    active_sources = {
        story["sourceId"]
        for category in categories
        for story in category["stories"]
        if story.get("sourceId")
    }

    return {
        "site": {
            "title": config["site"]["title"],
            "subtitle": config["site"]["subtitle"],
        },
        "edition": {
            "generatedAt": iso_clock(now),
            "dateLabel": now.strftime("%Y-%m-%d"),
            "timeLabel": now.strftime("%H:%M"),
            "timezone": config["site"]["timezone"],
            "mode": mode,
        },
        "stats": {
            "categoryCount": len(categories),
            "storyCount": story_count,
            "sourceCount": len(active_sources),
            "personaCount": config["persona_count"],
        },
        "categories": categories,
        "sourceRegistry": source_registry,
    }


def collect_category_items(
    category: dict[str, Any],
    raw_by_source: dict[str, list[dict[str, Any]]],
    limit: int,
) -> list[dict[str, Any]]:
    collected: list[dict[str, Any]] = []
    seen: set[str] = set()

    for priority, source_id in enumerate(category["sources"], start=1):
        source_items = raw_by_source.get(source_id, [])
        if not source_items:
            continue

        ranked_source_items = sorted(
            source_items,
            key=lambda item: (
                item.get("publishedAt") or "",
                -(item.get("rank") or 9999),
            ),
            reverse=True,
        )

        for item in ranked_source_items[:8]:
            dedupe_key = normalize_dedupe_key(item["title"], item["url"])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            enriched = dict(item)
            enriched["sourcePriority"] = priority
            enriched["signal"] = classify_signal(item["title"])
            collected.append(enriched)
            if len(collected) >= limit:
                return collected

    return collected


def build_template_category_digest(
    config: dict[str, Any],
    category: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    selected_items = items[:10]
    if not selected_items:
        selected_items = [
            {
                "title": "当前没有抓到可用新闻，建议检查源站或稍后再试。",
                "url": "#",
                "publishedAt": None,
                "sourceId": "fallback",
                "sourceLabel": "系统提示",
                "sourceHome": "#",
                "sourceType": "system",
                "signal": "watch",
            }
        ]

    stories = []
    for index, item in enumerate(selected_items, start=1):
        signal = item.get("signal") or classify_signal(item["title"])
        stories.append(
            {
                "index": index,
                "title": item["title"],
                "summary": template_story_summary(category, item, index),
                "reason": template_story_reason(category, item, signal),
                "signal": signal,
                "signalLabel": signal_label(signal),
                "url": item["url"],
                "sourceId": item["sourceId"],
                "source": item["sourceLabel"],
                "publishedAt": item.get("publishedAt"),
            }
        )

    bullish_story = first_story_by_signal(stories, "bullish")
    bearish_story = first_story_by_signal(stories, "bearish")
    watch_story = first_story_by_signal(stories, "watch")
    comments = build_template_comments(config["personas"], category, stories, config["persona_count"])

    return {
        "id": category["id"],
        "name": category["name"],
        "description": category["description"],
        "lens": category["lens"],
        "lead": build_category_lead(category, stories),
        "bullish": build_signal_paragraph(category, bullish_story, "bullish"),
        "bearish": build_signal_paragraph(category, bearish_story, "bearish"),
        "watch": build_signal_paragraph(category, watch_story, "watch"),
        "economistTake": build_economist_take(category, stories),
        "stories": stories,
        "personaComments": comments,
        "sourcesUsed": unique_sources_from_stories(stories),
    }


def generate_ai_category_digest(
    client: Any,
    config: dict[str, Any],
    category: dict[str, Any],
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    schema = {
        "type": "object",
        "properties": {
            "lead": {"type": "string"},
            "bullish": {"type": "string"},
            "bearish": {"type": "string"},
            "watch": {"type": "string"},
            "economistTake": {"type": "string"},
            "topStories": {
                "type": "array",
                "minItems": 8,
                "maxItems": 10,
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "summary": {"type": "string"},
                        "reason": {"type": "string"},
                        "signal": {
                            "type": "string",
                            "enum": ["bullish", "bearish", "watch"],
                        },
                    },
                    "required": ["title", "summary", "reason", "signal"],
                    "additionalProperties": False,
                },
            },
            "personaComments": {
                "type": "array",
                "minItems": config["persona_count"],
                "maxItems": config["persona_count"],
                "items": {
                    "type": "object",
                    "properties": {
                        "role": {"type": "string"},
                        "name": {"type": "string"},
                        "emotion": {"type": "string"},
                        "content": {"type": "string"},
                    },
                    "required": ["role", "name", "emotion", "content"],
                    "additionalProperties": False,
                },
            },
        },
        "required": [
            "lead",
            "bullish",
            "bearish",
            "watch",
            "economistTake",
            "topStories",
            "personaComments",
        ],
        "additionalProperties": False,
    }

    prompt = build_ai_prompt(config, category, items)
    response = client.responses.create(
        model=os.environ.get("OPENAI_MODEL", "gpt-5-mini"),
        input=prompt,
        text={
            "format": {
                "type": "json_schema",
                "name": "daily_news_category_digest",
                "strict": True,
                "schema": schema,
            }
        },
    )

    payload = json.loads(response.output_text)
    item_lookup = {normalize_title(item["title"]): item for item in items}
    stories = []
    for index, story in enumerate(payload["topStories"], start=1):
        matched = item_lookup.get(normalize_title(story["title"]))
        if matched is None:
            matched = fuzzy_match_story(story["title"], items)
        if matched is None:
            continue
        stories.append(
            {
                "index": index,
                "title": matched["title"],
                "summary": clean_text(story["summary"]),
                "reason": clean_text(story["reason"]),
                "signal": story["signal"],
                "signalLabel": signal_label(story["signal"]),
                "url": matched["url"],
                "sourceId": matched["sourceId"],
                "source": matched["sourceLabel"],
                "publishedAt": matched.get("publishedAt"),
            }
        )

    if len(stories) < 8:
        return build_template_category_digest(config, category, items)

    comments = []
    expected_personas = {(p["role"], p["handle"]) for p in config["personas"]}
    for comment in payload["personaComments"]:
        comments.append(
            {
                "role": clean_text(comment["role"]),
                "name": clean_text(comment["name"]),
                "emotion": clean_text(comment["emotion"]),
                "content": clean_text(comment["content"]),
            }
        )

    used_personas = {(comment["role"], comment["name"]) for comment in comments}
    if len(used_personas.intersection(expected_personas)) < max(10, config["persona_count"] // 2):
        comments = build_template_comments(config["personas"], category, stories, config["persona_count"])

    return {
        "id": category["id"],
        "name": category["name"],
        "description": category["description"],
        "lens": category["lens"],
        "lead": clean_text(payload["lead"]),
        "bullish": clean_text(payload["bullish"]),
        "bearish": clean_text(payload["bearish"]),
        "watch": clean_text(payload["watch"]),
        "economistTake": clean_text(payload["economistTake"]),
        "stories": stories[:10],
        "personaComments": comments[: config["persona_count"]],
        "sourcesUsed": unique_sources_from_stories(stories),
    }


def build_ai_prompt(
    config: dict[str, Any],
    category: dict[str, Any],
    items: list[dict[str, Any]],
) -> str:
    personas_block = "\n".join(
        f"- 角色：{persona['role']}｜网名：{persona['handle']}｜关注：{persona['style']}"
        for persona in config["personas"]
    )
    news_block = "\n".join(
        f"{index}. [{item['sourceLabel']}] {item['title']}"
        for index, item in enumerate(items[:30], start=1)
    )

    return f"""
你是一个中文 AI 财经与时事编辑部，要为静态新闻网站生成单个板块日报。

今天的板块：{category['name']}
板块描述：{category['description']}
经济分析视角：{category['lens']}

你只能使用下面这些原始标题，不要虚构事实，不要编造来源，不要臆造不存在的数字。

原始标题：
{news_block}

输出要求：
1. lead：90-150字，写成今日主线摘要。
2. bullish：40-90字，指出今天最偏利多的一条或一组信号。
3. bearish：40-90字，指出今天最偏利空的一条或一组信号。
4. watch：40-90字，指出最值得继续跟踪的变量。
5. economistTake：120-220字，要像经济分析师，用因果关系解释市场会怎么定价。
6. topStories：输出 8-10 条，每条都必须从原始标题里选；title 必须与原始标题完全一致。
7. personaComments：输出 25 条评论，每条对应一个不同角色，风格必须区分明显，要像真实评论区。

必须使用以下 25 个角色和网名，每个只能出现一次：
{personas_block}

写作约束：
- 使用中文
- 不要使用 Markdown
- 不要重复同一个标题
- 不要把不存在的新闻写进 topStories
- personaComments 的 content 控制在 20-80 字
""".strip()


def create_openai_client() -> Any | None:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return None
    return OpenAI(api_key=api_key)


def archive_digest(digest: dict[str, Any], now: datetime) -> None:
    date_part = now.strftime("%Y-%m-%d")
    time_part = now.strftime("%H-%M")
    archive_path = HISTORY_DIR / date_part / time_part / "digest.json"
    write_json(archive_path, digest)

    history_index: list[dict[str, Any]]
    if HISTORY_INDEX_PATH.exists():
        history_index = load_json(HISTORY_INDEX_PATH)
    else:
        history_index = []

    entry = {
        "id": f"{date_part}-{time_part}",
        "label": f"{date_part} {time_part.replace('-', ':')}",
        "path": f"data/history/{date_part}/{time_part}/digest.json",
    }

    history_index = [item for item in history_index if item["id"] != entry["id"]]
    history_index.insert(0, entry)
    history_index = history_index[:90]
    write_json(HISTORY_INDEX_PATH, history_index)


def build_category_lead(category: dict[str, Any], stories: list[dict[str, Any]]) -> str:
    headlines = [story["title"] for story in stories[:3]]
    if not headlines:
        return f"{category['name']} 暂无足够新闻，当前版本先保留源站监控结果。"
    lead_titles = "、".join(shorten_title(title, 18) for title in headlines)
    return (
        f"{category['name']} 今天的主线围绕 {lead_titles} 展开。"
        f"从 {category['lens']} 的角度看，短线更值得关注的是消息能否从标题热度继续传导到"
        "真实订单、资金方向和后续政策动作。"
    )


def build_signal_paragraph(
    category: dict[str, Any],
    story: dict[str, Any] | None,
    signal: str,
) -> str:
    if story is None:
        fallback = {
            "bullish": "利多线索暂时不够集中，更多像是零散事件在试探情绪。",
            "bearish": "利空线索暂时不够集中，市场还没有形成单边恐慌。",
            "watch": "当前更适合继续观察，等待更高确定性的下一条验证信息。",
        }
        return fallback[signal]

    if signal == "bullish":
        return (
            f"利多抓手先看《{shorten_title(story['title'], 28)}》。"
            "这类消息通常会先影响风险偏好，再决定资金是否愿意追价，关键是后续有没有连续验证。"
        )
    if signal == "bearish":
        return (
            f"利空压力来自《{shorten_title(story['title'], 28)}》。"
            "它更像风险定价事件，短线会先打情绪和估值，真正要不要扩大影响，还得看第二波数据。"
        )
    return (
        f"继续观察《{shorten_title(story['title'], 28)}》。"
        "这条新闻现在还处在预期形成阶段，比起立刻下结论，更重要的是跟踪兑现节奏。"
    )


def build_economist_take(category: dict[str, Any], stories: list[dict[str, Any]]) -> str:
    positive_count = sum(1 for story in stories if story["signal"] == "bullish")
    negative_count = sum(1 for story in stories if story["signal"] == "bearish")
    balance = "情绪偏暖" if positive_count >= negative_count else "情绪更谨慎"
    anchor = stories[0]["title"] if stories else "暂无主线"
    return (
        f"经济分析师视角下，{category['name']} 目前呈现出“{balance}、等待确认”的结构。"
        f"领头事件是《{shorten_title(anchor, 26)}》，但真正影响定价的不是单条标题本身，"
        f"而是它能否沿着 {category['lens']} 这条链路继续扩散。如果接下来政策、订单或资金流没有接力，"
        "那今天的热度更可能停留在交易层面；如果连续出现验证，板块就有机会从情绪修复转向趋势定价。"
    )


def template_story_summary(category: dict[str, Any], item: dict[str, Any], index: int) -> str:
    openers = (
        "这条新闻把市场焦点重新拉回到",
        "资金今天关注的核心变量之一是",
        "从板块主线看，这条信息对应的是",
        "如果把它放回全天节奏里看，真正重要的是",
    )
    opener = openers[(index - 1) % len(openers)]
    return (
        f"{opener}{category['description']}。"
        f"《{shorten_title(item['title'], 30)}》更像一个高频信号，适合结合后续数据和资金反应一起判断。"
    )


def template_story_reason(category: dict[str, Any], item: dict[str, Any], signal: str) -> str:
    if signal == "bullish":
        return f"偏利多，因为它容易改善 {category['name']} 的风险偏好，并给后续资金接力留下理由。"
    if signal == "bearish":
        return f"偏利空，因为它会先压制预期和估值，市场往往会提前计入不确定性。"
    return f"更适合观察，因为它对 {category['name']} 的影响还需要更多数据或后续动作来确认。"


def build_template_comments(
    personas: list[dict[str, Any]],
    category: dict[str, Any],
    stories: list[dict[str, Any]],
    comment_count: int,
) -> list[dict[str, Any]]:
    comments = []
    templates = {
        "bullish": (
            "站在 {role} 的角度，这条《{title}》我先记一笔偏多。只要后面真的能兑现，{focus} 这条线就还有想象空间。",
            "《{title}》这种消息最怕看标题就上头，但如果兑现速度跟上，{focus} 这块会先受益。",
            "我更在意《{title}》后面的二阶影响。要是真往下传导，{category} 不是一天行情。",
        ),
        "bearish": (
            "《{title}》这条我会先当风险项处理。标题热度是一回事，真正麻烦的是它会不会继续伤到 {focus}。",
            "别看现在评论区闹得凶，真正的压力在兑现层。{title} 这种消息，最容易先打掉市场耐心。",
            "我对《{title}》会偏谨慎。要是后面没有修复动作，{category} 这条线就得重新估值。",
        ),
        "watch": (
            "《{title}》先别急着站队，我会继续盯后续细节。对 {focus} 来说，节奏比态度更重要。",
            "这条《{title}》还在形成预期，先观察。很多时候不是新闻不重要，而是市场还没等到确认信号。",
            "先看后手。{title} 这类消息真正值钱的地方，在于它会不会把 {category} 的主线继续推下去。",
        ),
    }
    emotions = {
        "bullish": "偏乐观",
        "bearish": "偏谨慎",
        "watch": "继续观察",
    }

    for index, persona in enumerate(personas[:comment_count]):
        story = stories[index % len(stories)]
        signal = story["signal"]
        template = templates[signal][index % len(templates[signal])]
        comments.append(
            {
                "role": persona["role"],
                "name": persona["handle"],
                "emotion": emotions[signal],
                "content": template.format(
                    role=persona["role"],
                    title=shorten_title(story["title"], 26),
                    focus=persona["style"],
                    category=category["name"],
                ),
            }
        )

    return comments


def first_story_by_signal(stories: list[dict[str, Any]], signal: str) -> dict[str, Any] | None:
    for story in stories:
        if story["signal"] == signal:
            return story
    return stories[0] if stories else None


def unique_sources_from_stories(stories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    sources = []
    for story in stories:
        source_id = story.get("sourceId")
        if not source_id or source_id in seen:
            continue
        seen.add(source_id)
        sources.append({"id": source_id, "label": story.get("source")})
    return sources


def coerce_timestamp(value: Any) -> str | None:
    if value is None or value == "":
        return None

    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000.0
        try:
            return datetime.fromtimestamp(timestamp, tz=TZ).strftime("%Y-%m-%d %H:%M")
        except (OverflowError, OSError, ValueError):
            return None

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%a, %d %b %Y %H:%M:%S %z"):
            try:
                dt = datetime.strptime(text, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=TZ)
                return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
            except ValueError:
                continue
        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=TZ)
            return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
        except (TypeError, ValueError):
            return text

    return None


def normalize_dedupe_key(title: str, url: str) -> str:
    return f"{normalize_title(title)}::{url.strip().lower()}"


def normalize_title(title: str) -> str:
    normalized = clean_text(title).lower()
    normalized = re.sub(r"[\s\u3000]+", "", normalized)
    normalized = re.sub(r"[^\w\u4e00-\u9fff]+", "", normalized)
    return normalized


def clean_text(value: Any) -> str:
    if value is None:
        return ""
    text = html.unescape(str(value))
    text = text.replace("\u00a0", " ").replace("\r", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def classify_signal(title: str) -> str:
    normalized = clean_text(title)
    bullish_hits = sum(1 for keyword in BULLISH_KEYWORDS if keyword in normalized)
    bearish_hits = sum(1 for keyword in BEARISH_KEYWORDS if keyword in normalized)
    watch_hits = sum(1 for keyword in WATCH_KEYWORDS if keyword in normalized)

    if bullish_hits > bearish_hits:
        return "bullish"
    if bearish_hits > bullish_hits:
        return "bearish"
    if watch_hits:
        return "watch"
    return "watch"


def signal_label(signal: str) -> str:
    mapping = {
        "bullish": "利多",
        "bearish": "利空",
        "watch": "继续观察",
    }
    return mapping.get(signal, signal)


def shorten_title(title: str, width: int) -> str:
    title = clean_text(title)
    if len(title) <= width:
        return title
    return title[: max(0, width - 1)] + "…"


def tag_name(tag: str) -> str:
    if "}" in tag:
        return tag.split("}", 1)[1]
    return tag


def find_nodes(root: ET.Element, name: str) -> list[ET.Element]:
    return [node for node in root.iter() if tag_name(node.tag) == name]


def child_text(node: ET.Element, child_name: str) -> str:
    for child in node:
        if tag_name(child.tag) == child_name:
            return clean_text(child.text or "")
    return ""


def fuzzy_match_story(title: str, items: list[dict[str, Any]]) -> dict[str, Any] | None:
    needle = normalize_title(title)
    for item in items:
        candidate = normalize_title(item["title"])
        if needle and (needle in candidate or candidate in needle):
            return item
    return None


def iso_clock(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M")


if __name__ == "__main__":
    sys.exit(main())
