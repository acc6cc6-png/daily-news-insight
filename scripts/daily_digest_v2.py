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
from urllib.error import HTTPError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

try:
    from miniapp_factory import build_miniapp_factory
except Exception:
    build_miniapp_factory = None

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

RAW_LIMIT = 24
FULL_STORY_LIMIT = 48
PER_SOURCE_LIMIT = 12
PRIORITY_LIMIT = 10
PULSE_LIMIT = 8

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
    "关闭",
    "退市",
    "*st",
)

WATCH_KEYWORDS = (
    "计划",
    "预计",
    "拟",
    "将",
    "可能",
    "或",
    "建议",
    "征求意见",
    "进展",
    "启动",
)

IMPACT_KEYWORDS = {
    "央行": 22,
    "财政": 18,
    "降准": 24,
    "降息": 24,
    "加息": 24,
    "关税": 22,
    "制裁": 20,
    "冲突": 18,
    "财报": 15,
    "业绩": 14,
    "净利润": 14,
    "营收": 12,
    "回购": 12,
    "并购": 16,
    "融资": 12,
    "扩产": 10,
    "订单": 12,
    "产能": 10,
    "出口": 12,
    "进口": 12,
    "油价": 16,
    "原油": 16,
    "铜": 10,
    "黄金": 12,
    "碳酸锂": 14,
    "纯碱": 12,
    "玻璃": 12,
    "人工智能": 14,
    "大模型": 16,
    "agent": 12,
    "芯片": 14,
    "半导体": 14,
    "算力": 14,
    "房地产": 16,
    "地产": 16,
    "消费": 10,
    "补贴": 12,
    "监管": 16,
    "通胀": 16,
    "非农": 14,
}

CATEGORY_KEYWORDS = {
    "china-macro": {"政策": 14, "宽信用": 14, "财政": 12, "地产": 10},
    "china-markets": {"北向": 10, "南向": 10, "券商": 12, "量能": 8},
    "global-macro": {"美联储": 18, "通胀": 16, "利率": 14, "美元": 10},
    "geopolitics": {"冲突": 18, "停火": 12, "制裁": 16, "航运": 10},
    "ai-models": {"大模型": 18, "人工智能": 16, "agent": 14, "推理": 12},
    "chips-devices": {"芯片": 16, "半导体": 16, "封装": 12, "服务器": 12},
    "energy-commodities": {"原油": 18, "库存": 14, "铜": 12, "黄金": 12},
    "crypto-web3": {"btc": 16, "bitcoin": 16, "eth": 14, "etf": 12},
    "industry-consumer": {"消费": 12, "平台": 10, "广告": 10, "零售": 10},
    "social-trends": {"热搜": 12, "热榜": 12, "教育": 8, "医疗": 8},
}

CATEGORY_PLAYBOOK = {
    "china-macro": [
        "如果宽信用继续落地，先看券商与保险的风险偏好修复，再看地产链里玻璃、纯碱、建材等高弹性环节。",
        "如果财政加码转成实物工作量，工程机械、铜铝和运输链条通常快于总量数据反应。",
        "如果只有 headline 没有融资和销售验证，更适合分批观察，不适合追高。",
    ],
    "china-markets": [
        "当风险偏好回升时，弹性往往先出现在券商、港股互联网和高贝塔成长。",
        "如果量能只修复半天没有接力，更适合事件驱动而非机械定投。",
        "可以顺着指数权重到中游景气资产寻找更高弹性。",
    ],
    "global-macro": [
        "如果海外利率预期转松，先看美元与美债收益率，再看黄金、成长资产和新兴市场风险偏好。",
        "如果通胀黏性超预期，长端利率和高估值资产的压力通常快于总量数据。",
        "更合适的做法是按宏观变量链条拆分仓位，而不是只追单条 headline。",
    ],
    "geopolitics": [
        "地缘升级先定价原油、航运和避险资产，随后才传导到通胀与权益风险溢价。",
        "如果冲突只停留在口头阶段，适合看二阶验证，不适合把一次事件线性外推。",
        "真正值得跟的是供给扰动能否扩散到能源、运价和关键原材料价格。",
    ],
    "ai-models": [
        "大模型 headline 不该只盯终端产品，通常更高弹性的环节在算力、IDC、电力配套和推理基础设施。",
        "如果模型能力升级能转成开发者采用率，软件工具链和应用分发平台才会接到第二波定价。",
        "当前更适合观察调用量、成本曲线和客户预算，而不是只看发布会热度。",
    ],
    "chips-devices": [
        "如果算力需求继续上修，通常先看 HBM、先进封装、光模块和服务器电源等高弹性配套。",
        "如果终端销量修复不及预期，设备与晶圆环节的改善会慢于 headline。",
        "更适合等订单、交期和资本开支验证，再决定是做趋势还是做事件。",
    ],
    "energy-commodities": [
        "更适合沿着供给扰动到库存再到价格链条去看，而不是只停留在事件表面。",
        "如果需求回暖，弹性更大的往往是上游供给约束更强的品种，而非终端现货本身。",
        "策略上要先区分是供给冲击还是需求修复，两类行情的持续性完全不同。",
    ],
    "crypto-web3": [
        "不能只盯币价，真正的传导链条在流动性、ETF 资金、交易所活动度和链上活跃度。",
        "如果只是监管 headline，没有成交和资金配合，持续性往往有限。",
        "更稳妥的节奏是等成交量和风险偏好同步放大后再做趋势判断。",
    ],
    "industry-consumer": [
        "消费 headline 更应该拆到订单、客单价、渠道库存和广告投放强度，而不是只看舆论热度。",
        "如果政策刺激出现，弹性往往先落在平台流量、物流履约和可选消费。",
        "适合把节奏放在验证阶段：先看订单和流量，再考虑分批布局。",
    ],
    "social-trends": [
        "社会热点更适合作为情绪温度计，而不是直接投资结论，最好看它有没有传导到消费、政策或平台流量。",
        "如果热点只停留在传播层，影响往往短；如果能带动搜索、下单或监管动作，持续性才会上来。",
        "这类板块更适合观察二阶变量，不建议把热搜本身当成交易信号。",
    ],
}

GENERAL_AVOID_KEYWORDS = (
    "演唱会",
    "明星",
    "民宅",
    "火葬场",
    "长寿基因",
    "学校",
    "综艺",
    "比赛",
    "球员",
    "粉丝",
    "网红",
    "直播间",
    "恋情",
    "婚礼",
    "车祸",
    "坠楼",
    "模拟交易大赛",
    "获奖名单",
    "圆满收官",
    "专家顾问团",
    "系列采访",
    "兴趣激增",
    "生意火爆",
    "经销店",
    "9折",
    "解锁",
    "异动雷达",
)

HARD_DROP_KEYWORDS = (
    "模拟交易大赛",
    "获奖名单",
    "圆满收官",
    "专家顾问团",
    "系列采访",
    "兴趣激增",
    "生意火爆",
    "经销店",
    "9折",
    "解锁",
    "异动雷达",
)

EDITORIALIZED_TITLE_TERMS = (
    "背后",
    "意味着什么",
    "这一次不一样",
    "怎么看",
    "为何",
    "如何看",
    "如何理解",
    "如何交易",
    "会怎样",
    "解析",
    "预测",
    "专访",
    "为什么",
)

A_SHARE_OPEN_KEYWORDS = {
    "a股": 24,
    "港股": 22,
    "中概": 18,
    "美股": 20,
    "纳指": 18,
    "标普": 16,
    "道指": 14,
    "美债": 20,
    "债市": 20,
    "收益率": 18,
    "人民币": 18,
    "离岸人民币": 22,
    "美元": 14,
    "美联储": 20,
    "加息": 18,
    "降息": 18,
    "cpi": 16,
    "ppi": 14,
    "非农": 16,
    "中东": 16,
    "伊朗": 16,
    "以色列": 16,
    "原油": 18,
    "油价": 18,
    "黄金": 18,
    "金价": 16,
    "社融": 18,
    "pmi": 16,
    "地产": 18,
    "楼市": 16,
    "券商": 12,
    "玻璃": 14,
    "纯碱": 14,
    "碳酸锂": 14,
    "工业硅": 12,
    "稀土": 12,
    "关税": 18,
    "特朗普": 14,
}

FOCUS_PRIORITY_KEYWORDS = {
    "特朗普": 24,
    "trump": 24,
    "美股": 22,
    "纳指": 22,
    "纳斯达克": 22,
    "nasdaq": 22,
    "标普": 20,
    "s&p": 20,
    "道指": 18,
    "dow": 18,
    "黄金": 22,
    "金价": 18,
    "债市": 22,
    "债券": 18,
    "美债": 22,
    "收益率": 14,
    "原油": 22,
    "油价": 22,
    "中东": 22,
    "伊朗": 22,
    "以色列": 22,
    "关税": 20,
    "制裁": 20,
    "美联储": 24,
    "降息": 22,
    "加息": 22,
    "非农": 18,
    "cpi": 18,
    "ppi": 16,
    "a股": 18,
    "港股": 18,
    "人民币": 14,
    "社融": 18,
    "pmi": 16,
    "三大指数": 20,
    "全球债市": 24,
}

CATEGORY_RULES = {
    "focus-news": {
        "must": (
            "特朗普",
            "中东",
            "伊朗",
            "以色列",
            "美股",
            "纳指",
            "标普",
            "道指",
            "黄金",
            "债市",
            "美债",
            "原油",
            "油价",
            "美联储",
            "关税",
            "制裁",
            "a股",
            "港股",
        ),
        "soft": ("社融", "pmi", "cpi", "ppi", "非农", "人民币", "通胀", "指数"),
        "avoid": GENERAL_AVOID_KEYWORDS,
    },
    "china-macro": {
        "must": ("国务院", "央行", "财政", "社融", "信贷", "房贷", "地产", "降息", "降准", "发改委", "pmi", "cpi", "ppi"),
        "soft": ("政策", "财政部", "商务部", "刺激", "专项债", "国债", "贷款", "宽信用", "房地产"),
        "avoid": GENERAL_AVOID_KEYWORDS + ("伊朗", "以色列", "原油", "载人绕月", "mac mini"),
    },
    "china-markets": {
        "must": ("a股", "港股", "上证", "深成指", "创业板", "北向", "南向", "etf", "指数", "收盘", "沪深"),
        "soft": ("纳指", "美股", "黄金", "原油", "市场", "成交额", "券商", "情绪", "中概"),
        "avoid": GENERAL_AVOID_KEYWORDS,
    },
    "global-macro": {
        "must": ("美股", "纳指", "标普", "道指", "美债", "债市", "收益率", "黄金", "原油", "美元", "美联储", "cpi", "ppi", "非农"),
        "soft": ("欧洲央行", "日本央行", "通胀", "失业率", "房贷利率", "全球", "关税"),
        "avoid": GENERAL_AVOID_KEYWORDS + ("停车", "app", "推特投资者"),
    },
    "geopolitics": {
        "must": ("伊朗", "以色列", "中东", "俄", "乌", "关税", "制裁", "停火", "红海", "外交", "特朗普"),
        "soft": ("原油", "航运", "北约", "军方", "总统", "国防", "谈判"),
        "avoid": GENERAL_AVOID_KEYWORDS + ("火葬场", "民宅", "韩国工厂", "chuck norris"),
    },
}

RELEVANCE_THRESHOLDS = {
    "focus-news": 22,
    "china-macro": 14,
    "china-markets": 10,
    "global-macro": 14,
    "geopolitics": 14,
}

MARKET_PRIORITY_GROUPS = {
    "geopolitics": ("中东", "伊朗", "以色列", "停火", "袭击", "空袭", "红海", "霍尔木兹", "俄乌", "制裁"),
    "global_rates": ("美联储", "降息", "加息", "非农", "cpi", "ppi", "债市", "美债", "收益率", "通胀"),
    "risk_assets": ("美股", "纳指", "纳斯达克", "标普", "道指", "a股", "港股", "中概", "三大指数"),
    "commodities": ("黄金", "金价", "原油", "油价", "天然气", "铜价", "铝价"),
    "china_macro": ("人民币", "社融", "pmi", "央行", "财政", "地产", "楼市", "关税"),
}

MARKET_INTENSITY_TERMS = (
    "新高",
    "新低",
    "最大单周跌幅",
    "最大周跌幅",
    "最大跌幅",
    "血洗",
    "暴跌",
    "重挫",
    "飙升",
    "急升",
    "急跌",
    "转机",
    "升级",
    "缓和",
    "停火",
)

MICRO_STORY_PATTERNS = (
    re.compile(r"[（(]\d{4,6}\.[A-Z]{2,4}[）)]", re.IGNORECASE),
    re.compile(r"\b\d{6}\.(?:sh|sz)\b", re.IGNORECASE),
    re.compile(r"\b\d{4,5}\.(?:hk)\b", re.IGNORECASE),
)

MICRO_STORY_TERMS = (
    "归母净利润",
    "拟派利",
    "年度业绩",
    "年度净利润",
    "财务指标全面改善",
    "担任第",
    "职工董事",
    "董事会",
    "年度报告",
    "获得受理",
    "ipo获受理",
    "签署合作",
    "战略合作",
    "发布首款",
    "全面改善",
)


def main() -> int:
    load_local_env(ROOT / ".env.local")
    config = load_json(CONFIG_PATH)
    now = datetime.now(TZ)
    print(f"[daily-digest-v2] build started at {now.isoformat()}")

    raw_by_source = fetch_all_sources(config)
    raw_snapshot = build_raw_snapshot(config, raw_by_source, now)
    write_json(LATEST_RAW_PATH, raw_snapshot)

    digest = build_digest(config, raw_by_source, now)
    if build_miniapp_factory is not None:
        try:
            digest["miniappFactory"] = build_miniapp_factory(config, raw_by_source, now, HISTORY_DIR)
        except Exception as exc:  # noqa: BLE001
            print(f"[miniapp-factory] failed -> {exc}")
    write_json(LATEST_DIGEST_PATH, digest)
    archive_digest(digest, now)

    print(
        f"[daily-digest-v2] completed with mode={digest['edition']['mode']} "
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
        for rank, item in enumerate(source_payload.get("items", [])[:80], start=1):
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
    return parsed


def fetch_newsnow_with_curl(payload: bytes) -> bytes:
    curl_binary = shutil.which("curl") or shutil.which("curl.exe")
    if not curl_binary:
        raise RuntimeError("curl is not available for NewsNow fallback")
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
    request_headers = {"User-Agent": "daily-news-insight/1.0", "Accept": "*/*"}
    if headers:
        request_headers.update(headers)

    for _ in range(max_redirects + 1):
        request = Request(current_url, data=data, headers=request_headers, method=method)
        try:
            with urlopen(request, timeout=30) as response:
                return response.read(), response.geturl(), response.headers.get("Content-Type", "")
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
    raise RuntimeError(f"too many redirects while fetching {url}")


def parse_feed_items(
    xml_bytes: bytes,
    source_id: str,
    meta: dict[str, Any],
    final_url: str,
) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_bytes)
    items: list[dict[str, Any]] = []
    if tag_name(root.tag) == "rss":
        nodes = find_nodes(root, "item")
    elif tag_name(root.tag) == "feed":
        nodes = find_nodes(root, "entry")
    else:
        nodes = []

    for rank, node in enumerate(nodes[:80], start=1):
        title = clean_text(child_text(node, "title"))
        published = child_text(node, "pubDate") or child_text(node, "published") or child_text(node, "updated")
        url = clean_text(child_text(node, "link"))
        if not url:
            for child in node:
                if tag_name(child.tag) == "link" and child.attrib.get("href"):
                    url = urljoin(final_url, child.attrib["href"])
                    break
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


def build_raw_snapshot(
    config: dict[str, Any],
    raw_by_source: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> dict[str, Any]:
    window_start = market_window_start(now)
    categories = []
    for category in config["categories"]:
        items = collect_category_items(category, raw_by_source, now, RAW_LIMIT)
        categories.append(
            {
                "id": category["id"],
                "name": category["name"],
                "description": category["description"],
                "sourceIds": category["sources"],
                "windowStart": iso_clock(window_start),
                "windowLabel": format_window_label(window_start, now),
                "itemCount": len(items),
                "items": items,
            }
        )
    return {
        "generatedAt": iso_clock(now),
        "windowStart": iso_clock(window_start),
        "windowLabel": format_window_label(window_start, now),
        "timezone": config["site"]["timezone"],
        "sourceCount": len(config["sources"]),
        "categories": categories,
    }


def build_digest(
    config: dict[str, Any],
    raw_by_source: dict[str, list[dict[str, Any]]],
    now: datetime,
) -> dict[str, Any]:
    client = create_openai_client()
    window_start = market_window_start(now)
    categories = []
    modes: list[str] = []

    for category in config["categories"]:
        items = collect_category_items(category, raw_by_source, now, FULL_STORY_LIMIT)
        digest, mode = build_category_digest(client, config, category, items, now, window_start)
        categories.append(digest)
        modes.append(mode)
        time.sleep(0.35)

    remix_focus_category(categories)

    source_registry = [
        {"id": source_id, "label": meta["label"], "kind": meta["type"], "home": meta["home"]}
        for source_id, meta in config["sources"].items()
    ]
    active_sources = {
        story["sourceId"]
        for category in categories
        for story in category["allStories"]
        if story.get("sourceId")
    }
    story_count = sum(len(category["allStories"]) for category in categories)
    highlight_count = sum(len(category["priorityStories"]) for category in categories)

    if all(mode == "ai" for mode in modes):
        edition_mode = "ai"
    elif any(mode == "ai" for mode in modes):
        edition_mode = "mixed"
    else:
        edition_mode = "template"

    return {
        "site": {
            **config["site"],
        },
        "edition": {
            "generatedAt": iso_clock(now),
            "dateLabel": now.strftime("%Y-%m-%d"),
            "timeLabel": now.strftime("%H:%M"),
            "timezone": config["site"]["timezone"],
            "mode": edition_mode,
            "windowStart": iso_clock(window_start),
            "windowLabel": format_window_label(window_start, now),
        },
        "stats": {
            "categoryCount": len(categories),
            "storyCount": story_count,
            "highlightCount": highlight_count,
            "sourceCount": len(active_sources),
            "personaCount": config["persona_count"],
        },
        "marketPulse": build_market_pulse(categories, window_start, now),
        "categories": categories,
        "sourceRegistry": source_registry,
    }


def remix_focus_category(categories: list[dict[str, Any]]) -> None:
    focus_category = next((category for category in categories if category["id"] == "focus-news"), None)
    if focus_category is None:
        return

    core_source_ids = {"china-markets", "global-macro", "energy-commodities"}
    support_source_ids = {"geopolitics", "china-macro"}
    pool: list[dict[str, Any]] = []
    seen: set[str] = set()
    for category in categories:
        if category["id"] in core_source_ids:
            source_stories = category.get("allStories", [])
        elif category["id"] in support_source_ids:
            source_stories = category.get("priorityStories", [])[:5]
        else:
            continue
        for story in source_stories:
            normalized = clean_text(story["title"]).lower()
            if any(keyword.lower() in normalized for keyword in HARD_DROP_KEYWORDS):
                continue
            if (
                float(story.get("selectionScore", 0)) < 60
                and float(story.get("marketPriority", 0)) < 40
                and float(story.get("aShareOpenScore", 0)) < 28
            ):
                continue
            dedupe_key = normalize_title(story["title"])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            pool.append(dict(story))

    if len(pool) < PRIORITY_LIMIT * 2:
        for story in focus_category.get("priorityStories", []):
            dedupe_key = normalize_title(story["title"])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            pool.append(dict(story))

    if not pool:
        return

    pool.sort(
        key=lambda story: (
            1 if story.get("inWindow") else 0,
            float(story.get("selectionScore", 0)),
            float(story.get("marketPriority", 0)),
            story.get("publishedAt") or "",
        ),
        reverse=True,
    )
    focus_category["allStories"] = pool[:FULL_STORY_LIMIT]
    focus_ranked = rank_priority_stories({"id": "focus-news"}, focus_category["allStories"])
    focus_category["priorityStories"] = synthesize_template_priority_stories(
        focus_category,
        focus_category["allStories"],
        focus_ranked,
    )
    focus_category["stories"] = focus_category["priorityStories"]
    focus_category["sourcesUsed"] = unique_sources_from_stories(focus_category["allStories"])
    focus_category["stats"]["allStoryCount"] = len(focus_category["allStories"])
    focus_category["stats"]["priorityCount"] = len(focus_category["priorityStories"])


def build_category_digest(
    client: Any | None,
    config: dict[str, Any],
    category: dict[str, Any],
    items: list[dict[str, Any]],
    now: datetime,
    window_start: datetime,
) -> tuple[dict[str, Any], str]:
    prepared_items = items or [fallback_story_item(category)]
    all_stories = build_story_records(category, prepared_items, now, window_start)
    priority_stories = rank_priority_stories(category, all_stories)
    editorial = build_template_editorial(config, category, all_stories, priority_stories, now, window_start)
    mode = "template"

    if client is not None and items:
        try:
            ai_editorial = generate_ai_editorial(client, config, category, all_stories, window_start, now)
            priority_stories = rank_priority_stories(category, all_stories, ai_editorial.get("priorityTitles"))
            editorial.update(
                {
                    "lead": clean_text(ai_editorial["lead"]),
                    "cycleView": clean_text(ai_editorial["cycleView"]),
                    "strategyTake": clean_text(ai_editorial["strategyTake"]),
                    "bullish": clean_text(ai_editorial["bullish"]),
                    "bearish": clean_text(ai_editorial["bearish"]),
                    "watch": clean_text(ai_editorial["watch"]),
                    "economistTake": clean_text(ai_editorial["economistTake"]),
                    "linkageIdeas": [clean_text(item) for item in ai_editorial["linkageIdeas"][:3]],
                    "personaComments": normalize_persona_comments(
                        ai_editorial["personaComments"],
                        config["personas"],
                        category,
                        priority_stories,
                        config["persona_count"],
                    ),
                }
            )
            mode = "ai"
        except Exception as exc:  # noqa: BLE001
            print(f"[ai] {category['id']}: failed -> {exc}")

    return (
        {
            "id": category["id"],
            "name": category["name"],
            "description": category["description"],
            "lens": category["lens"],
            "windowLabel": format_window_label(window_start, now),
            "lead": editorial["lead"],
            "cycleView": editorial["cycleView"],
            "strategyTake": editorial["strategyTake"],
            "linkageIdeas": editorial["linkageIdeas"],
            "bullish": editorial["bullish"],
            "bearish": editorial["bearish"],
            "watch": editorial["watch"],
            "economistTake": editorial["economistTake"],
            "priorityStories": priority_stories,
            "allStories": all_stories,
            "stories": priority_stories,
            "personaComments": editorial["personaComments"][: config["persona_count"]],
            "sourcesUsed": unique_sources_from_stories(all_stories),
            "stats": {
                "allStoryCount": len(all_stories),
                "priorityCount": len(priority_stories),
            },
        },
        mode,
    )


def collect_category_items(
    category: dict[str, Any],
    raw_by_source: dict[str, list[dict[str, Any]]],
    now: datetime,
    limit: int,
) -> list[dict[str, Any]]:
    window_start = market_window_start(now)
    collected: list[dict[str, Any]] = []
    seen: set[str] = set()
    threshold = RELEVANCE_THRESHOLDS.get(category["id"], 0)
    per_source_limit = 24 if category["id"] == "focus-news" else PER_SOURCE_LIMIT

    for priority, source_id in enumerate(category["sources"], start=1):
        source_items = raw_by_source.get(source_id, [])
        if not source_items:
            continue
        source_items = sorted(
            source_items,
            key=lambda item: source_item_sort_key(category, item, window_start),
            reverse=True,
        )
        for item in source_items[:per_source_limit]:
            dedupe_key = normalize_dedupe_key(item["title"], item["url"])
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            enriched = dict(item)
            enriched["sourcePriority"] = priority
            enriched["signal"] = classify_signal(item["title"])
            enriched["relevanceScore"] = category_relevance_score(category, item["title"])
            enriched["marketPriority"] = market_priority_score(item["title"])
            enriched["aShareOpenScore"] = a_share_open_score(item["title"])
            enriched["microPenalty"] = micro_story_penalty(category, item["title"])
            enriched["editorialPenalty"] = editorialized_title_penalty(item["title"])
            enriched["selectionScore"] = (
                enriched["relevanceScore"]
                + enriched["marketPriority"]
                + enriched["aShareOpenScore"]
                - enriched["microPenalty"]
                - enriched["editorialPenalty"]
            )
            if should_drop_story(category, item["title"], enriched["selectionScore"]):
                continue
            collected.append(enriched)

    preferred = [
        item
        for item in collected
        if item["selectionScore"] >= threshold
    ]
    backups = [item for item in collected if item not in preferred]

    ranked_preferred = sorted(preferred, key=lambda item: selection_sort_key(item, window_start), reverse=True)
    ranked_backups = sorted(backups, key=lambda item: selection_sort_key(item, window_start), reverse=True)

    chosen = ranked_preferred[:limit]
    minimum_fill = min(limit, max(8, limit // 3))
    if len(chosen) < minimum_fill:
        for item in ranked_backups:
            if item in chosen:
                continue
            if len(chosen) >= limit:
                break
            if item["selectionScore"] >= max(0, threshold - 8) or len(chosen) < minimum_fill:
                chosen.append(item)

    if not chosen:
        chosen = ranked_backups[:limit]
    return chosen[:limit]


def build_story_records(
    category: dict[str, Any],
    items: list[dict[str, Any]],
    now: datetime,
    window_start: datetime,
) -> list[dict[str, Any]]:
    stories = []
    for item in items:
        signal = item.get("signal") or classify_signal(item["title"])
        impact_score = story_impact_score(category, item, now, window_start)
        impact_reason = build_impact_reason(category, item, impact_score, window_start)
        stories.append(
            {
                "title": item["title"],
                "summary": build_story_summary(category, item, signal),
                "reason": impact_reason,
                "impactReason": impact_reason,
                "signal": signal,
                "signalLabel": signal_label(signal),
                "impactScore": impact_score,
                "impactLabel": story_impact_label(impact_score),
                "relevanceScore": item.get("relevanceScore", 0),
                "marketPriority": item.get("marketPriority", 0),
                "aShareOpenScore": item.get("aShareOpenScore", 0),
                "selectionScore": item.get("selectionScore", 0),
                "editorialPenalty": item.get("editorialPenalty", 0),
                "url": item["url"],
                "sourceId": item["sourceId"],
                "source": item["sourceLabel"],
                "sourceKind": item.get("sourceType"),
                "publishedAt": item.get("publishedAt"),
                "inWindow": is_within_window(item.get("publishedAt"), window_start),
            }
        )
    stories.sort(key=story_time_sort_key, reverse=True)
    for index, story in enumerate(stories, start=1):
        story["index"] = index
    return stories


def rank_priority_stories(
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    ai_priority_titles: list[str] | None = None,
) -> list[dict[str, Any]]:
    ai_order = {
        normalize_title(title): position
        for position, title in enumerate(ai_priority_titles or [], start=1)
    }

    def priority_key(story: dict[str, Any]) -> tuple[float, int, str]:
        boost = float(story.get("selectionScore", story["impactScore"])) + float(story.get("marketPriority", 0)) * 0.3
        if category["id"] in {"focus-news", "china-markets", "global-macro"}:
            boost += float(story.get("aShareOpenScore", 0)) * 0.6
        if story.get("inWindow"):
            boost += 12
        match = ai_order.get(normalize_title(story["title"]))
        if match is not None:
            boost += max(0, 24 - match * 2)
        return boost, 1 if story.get("inWindow") else 0, story.get("publishedAt") or ""

    ranked = sorted(all_stories, key=priority_key, reverse=True)
    bucket_limits = priority_bucket_limits(category["id"])
    bucket_counts: dict[str, int] = {}
    result = []
    for story in ranked:
        bucket = priority_story_bucket(story["title"])
        if bucket_counts.get(bucket, 0) >= bucket_limits.get(bucket, 2):
            continue
        bucket_counts[bucket] = bucket_counts.get(bucket, 0) + 1
        enriched = dict(story)
        enriched["priorityRank"] = len(result) + 1
        result.append(enriched)
        if len(result) >= PRIORITY_LIMIT:
            break

    if len(result) < PRIORITY_LIMIT:
        seen_titles = {normalize_title(story["title"]) for story in result}
        for story in ranked:
            if normalize_title(story["title"]) in seen_titles:
                continue
            enriched = dict(story)
            enriched["priorityRank"] = len(result) + 1
            result.append(enriched)
            if len(result) >= PRIORITY_LIMIT:
                break
    return result


def priority_story_bucket(title: str) -> str:
    normalized = clean_text(title).lower()
    if any(keyword in normalized for keyword in ("黄金", "金价")):
        return "gold"
    if any(keyword in normalized for keyword in ("债市", "美债", "收益率", "英债", "日债")):
        return "bonds"
    if any(keyword in normalized for keyword in ("原油", "油价", "天然气", "铜价", "铝价")):
        return "energy"
    if any(keyword in normalized for keyword in ("美股", "纳指", "纳斯达克", "标普", "道指", "a股", "港股", "中概", "三大指数", "etf")):
        return "equities"
    if any(keyword in normalized for keyword in ("美联储", "降息", "加息", "非农", "cpi", "ppi", "pmi", "社融", "央行", "人民币")):
        return "policy"
    if any(keyword in normalized for keyword in ("中东", "伊朗", "以色列", "停火", "袭击", "空袭", "霍尔木兹", "制裁", "特朗普")):
        return "geopolitics"
    return "other"


def priority_bucket_limits(category_id: str) -> dict[str, int]:
    if category_id == "focus-news":
        return {
            "gold": 2,
            "bonds": 2,
            "energy": 2,
            "equities": 2,
            "policy": 2,
            "geopolitics": 2,
            "other": 1,
        }
    if category_id == "global-macro":
        return {
            "gold": 2,
            "bonds": 2,
            "energy": 2,
            "equities": 2,
            "policy": 2,
            "geopolitics": 1,
            "other": 1,
        }
    return {
        "gold": 2,
        "bonds": 2,
        "energy": 2,
        "equities": 3,
        "policy": 2,
        "geopolitics": 2,
        "other": 2,
    }


def build_template_editorial(
    config: dict[str, Any],
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    priority_stories: list[dict[str, Any]],
    now: datetime,
    window_start: datetime,
) -> dict[str, Any]:
    cycle_name = infer_cycle_name(priority_stories)
    return {
        "lead": build_category_lead(category, all_stories, priority_stories, window_start, now),
        "cycleView": build_cycle_view(category, cycle_name, priority_stories),
        "strategyTake": build_strategy_take(category, cycle_name, priority_stories),
        "linkageIdeas": build_linkage_ideas(category, cycle_name, priority_stories),
        "bullish": build_signal_paragraph(category, first_story_by_signal(priority_stories, "bullish"), "bullish"),
        "bearish": build_signal_paragraph(category, first_story_by_signal(priority_stories, "bearish"), "bearish"),
        "watch": build_signal_paragraph(category, first_story_by_signal(priority_stories, "watch"), "watch"),
        "economistTake": build_economist_take(category, priority_stories, cycle_name),
        "personaComments": build_template_comments(
            config["personas"],
            category,
            priority_stories,
            config["persona_count"],
        ),
    }


def generate_ai_editorial(
    client: Any,
    config: dict[str, Any],
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    window_start: datetime,
    now: datetime,
) -> dict[str, Any]:
    schema = {
        "type": "object",
        "properties": {
            "lead": {"type": "string"},
            "cycleView": {"type": "string"},
            "strategyTake": {"type": "string"},
            "bullish": {"type": "string"},
            "bearish": {"type": "string"},
            "watch": {"type": "string"},
            "economistTake": {"type": "string"},
            "linkageIdeas": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {"type": "string"},
            },
            "priorityTitles": {
                "type": "array",
                "minItems": min(6, len(all_stories)),
                "maxItems": min(PRIORITY_LIMIT, len(all_stories)),
                "items": {"type": "string"},
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
            "cycleView",
            "strategyTake",
            "bullish",
            "bearish",
            "watch",
            "economistTake",
            "linkageIdeas",
            "priorityTitles",
            "personaComments",
        ],
        "additionalProperties": False,
    }
    response = client.responses.create(
        model=os.environ.get("OPENAI_MODEL", "gpt-5-mini"),
        input=build_ai_prompt(config, category, all_stories, window_start, now),
        text={
            "format": {
                "type": "json_schema",
                "name": "daily_news_editorial_board",
                "strict": True,
                "schema": schema,
            }
        },
    )
    return json.loads(response.output_text)


def build_ai_prompt(
    config: dict[str, Any],
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    window_start: datetime,
    now: datetime,
) -> str:
    news_block = "\n".join(
        f"{index}. [{story['source']}] {story['title']} (时间：{story.get('publishedAt') or '未标注'}；影响分：{story['impactScore']})"
        for index, story in enumerate(all_stories[:FULL_STORY_LIMIT], start=1)
    )
    personas_block = "\n".join(
        f"- 角色：{persona['role']}；网名：{persona['handle']}；关注：{persona['style']}"
        for persona in config["personas"]
    )
    playbook = "\n".join(f"- {item}" for item in CATEGORY_PLAYBOOK.get(category["id"], []))
    return f"""
你是一名中文财经总编兼策略分析师，要为一个正式投研新闻站写单个板块的全天候简报。

板块：{category['name']}
板块描述：{category['description']}
观察镜头：{category['lens']}
分析窗口：{window_start.strftime("%Y-%m-%d %H:%M")} 到 {now.strftime("%Y-%m-%d %H:%M")}（Asia/Shanghai）

原始新闻如下，只能基于这些标题做判断，不要虚构事实、数字、来源或个股代码：
{news_block}

写作要求：
1. 不要出现“AI”“自动生成”等字样。
2. 重点排序必须按影响力，而不是按时间顺序。
3. strategyTake 必须像研究员备忘录，不直接喊单，要讲节奏、验证条件和更该盯的变量。
4. 可以借鉴投资联想法，从 headline 推导到更高弹性的上游、中游、配套或价格链变量，但不要机械套用。
5. linkageIdeas 写成三条专业提示，每条都要体现“headline -> 传导链 -> 更该盯的变量/环节”。

这个板块的联想链参考：
{playbook or '- 暂无固定链条，请根据标题自行提炼。'}

必须使用以下 25 个角色与网名，各出现一次：
{personas_block}
""".strip()


def build_market_pulse(categories: list[dict[str, Any]], window_start: datetime, now: datetime) -> dict[str, Any]:
    category_weights = {
        "focus-news": 36,
        "global-macro": 30,
        "china-macro": 30,
        "china-markets": 28,
        "geopolitics": 24,
        "energy-commodities": 24,
        "chips-devices": 16,
        "ai-models": 16,
        "crypto-web3": 10,
        "industry-consumer": 8,
        "social-trends": 0,
    }
    highlights = []
    for category in categories:
        category_weight = category_weights.get(category["id"], 0)
        if category_weight <= 0:
            continue
        for story in category["priorityStories"][:3]:
            if category["id"] != "focus-news" and float(story.get("marketPriority", 0)) < 18 and float(story.get("aShareOpenScore", 0)) < 16:
                continue
            highlights.append(
                {
                    "title": story["title"],
                    "categoryId": category["id"],
                    "categoryName": category["name"],
                    "impactLabel": story["impactLabel"],
                    "impactScore": story["impactScore"],
                    "signal": story["signal"],
                    "signalLabel": story["signalLabel"],
                    "impactReason": story["impactReason"],
                    "url": story["url"],
                    "source": story["source"],
                    "publishedAt": story.get("publishedAt"),
                    "pulseScore": (
                        float(story.get("impactScore", 0))
                        + category_weight
                        + float(story.get("marketPriority", 0)) * 0.35
                        + float(story.get("aShareOpenScore", 0)) * 0.25
                    ),
                }
            )
    highlights.sort(
        key=lambda item: (
            float(item.get("pulseScore", 0)),
            item["impactScore"],
            item.get("publishedAt") or "",
        ),
        reverse=True,
    )
    highlights = highlights[:PULSE_LIMIT]
    focus = "、".join(dict.fromkeys(item["categoryName"] for item in highlights[:4])) or "多板块"
    return {
        "headline": (
            f"从昨收后到现在，影响最大的消息主要集中在{focus}。"
            "更值得关注的不是 headline 数量，而是它们能否继续传导到订单、价格、流动性和风险偏好。"
        ),
        "windowLabel": format_window_label(window_start, now),
        "highlights": highlights,
    }


def normalize_persona_comments(
    comments: list[dict[str, Any]],
    personas: list[dict[str, Any]],
    category: dict[str, Any],
    stories: list[dict[str, Any]],
    comment_count: int,
) -> list[dict[str, Any]]:
    normalized = [
        {
            "role": clean_text(comment.get("role")),
            "name": clean_text(comment.get("name")),
            "emotion": clean_text(comment.get("emotion")),
            "content": clean_text(comment.get("content")),
        }
        for comment in comments
    ]
    expected = {(persona["role"], persona["handle"]) for persona in personas}
    actual = {(comment["role"], comment["name"]) for comment in normalized}
    if len(expected.intersection(actual)) < max(10, comment_count // 2):
        return build_template_comments(personas, category, stories, comment_count)
    return normalized[:comment_count]


def fallback_story_item(category: dict[str, Any]) -> dict[str, Any]:
    return {
        "title": f"{category['name']} 当前暂无足够新闻，先保留监控结果等待下一轮更新。",
        "url": "#",
        "publishedAt": None,
        "sourceId": "fallback",
        "sourceLabel": "系统提示",
        "sourceHome": "#",
        "sourceType": "system",
        "rank": 999,
        "sourcePriority": 99,
        "signal": "watch",
    }


def build_category_lead(
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    priority_stories: list[dict[str, Any]],
    window_start: datetime,
    now: datetime,
) -> str:
    if not priority_stories:
        return f"{category['name']} 当前还没有足够的新动态，先保持监控，等待窗口内的下一批新闻进入。"
    top_titles = "、".join(shorten_title(story["title"], 18) for story in priority_stories[:3])
    if category["id"] == "focus-news":
        return f"本窗口共跟踪 {len(all_stories)} 条动态，当前主线集中在{top_titles}。先看这些变量会先改写利率、油金，还是先改写风险偏好。"
    return (
        f"从{window_start.strftime('%m-%d %H:%M')}到{now.strftime('%m-%d %H:%M')}，"
        f"{category['name']} 共追踪到 {len(all_stories)} 条动态。当前最值得盯的主线集中在{top_titles}。"
        f"更关键的是这些消息能否沿着 {category['lens']} 继续传导到订单、价格、流动性或风险偏好。"
    )


def priority_titles_blob(priority_stories: list[dict[str, Any]], limit: int = 8) -> str:
    return " ".join(clean_text(story["title"]).lower() for story in priority_stories[:limit])


def detect_market_setup(category: dict[str, Any], priority_stories: list[dict[str, Any]]) -> str:
    text = priority_titles_blob(priority_stories)
    if any(keyword in text for keyword in ("社融", "降准", "降息", "财政", "专项债", "地产", "楼市", "宽信用", "pmi")):
        return "domestic_policy"
    if (
        any(keyword in text for keyword in ("美债", "债市", "收益率", "美联储", "加息", "降息"))
        and any(keyword in text for keyword in ("美股", "纳指", "标普", "黄金", "原油", "三大指数"))
    ):
        return "global_repricing"
    if any(keyword in text for keyword in ("中东", "伊朗", "以色列", "油价", "原油", "霍尔木兹")):
        return "commodity_shock"
    if any(keyword in text for keyword in ("转涨", "修复", "反弹", "回暖", "缓和")):
        return "risk_repair"
    return "wait_confirm"


def open_focus_variables(setup: str) -> str:
    mapping = {
        "global_repricing": "美债收益率、油价、黄金、美股期货和离岸人民币",
        "commodity_shock": "原油、黄金、航运、化工期货和港股能源股",
        "domestic_policy": "社融、地产销售、国债利率、券商与地产链",
        "risk_repair": "港股、中概、北向资金、成交额和高贝塔板块",
        "wait_confirm": "竞价强弱、成交额、北向资金和期货联动",
    }
    return mapping.get(setup, "竞价强弱、成交额、北向资金和期货联动")


def setup_board_hint(setup: str) -> str:
    mapping = {
        "global_repricing": "油气、油运、煤化工、黄金、防御红利，以及受外盘拖累后可能分化的港股互联网与高估值成长",
        "commodity_shock": "油运、油服、煤化工、贵金属和上游资源，谨慎对待高估值久期资产",
        "domestic_policy": "券商、地产链、玻璃、纯碱、建材，以及宽信用最先受益的顺周期资产",
        "risk_repair": "券商、港股互联网、中概、高贝塔成长和情绪修复最快的核心资产",
        "wait_confirm": "先列强弱名单，再等真正放量的主线板块自己走出来",
    }
    return mapping.get(setup, "先列强弱名单，再等真正放量的主线板块自己走出来")


def infer_cycle_name(priority_stories: list[dict[str, Any]]) -> str:
    bullish = sum(1 for story in priority_stories if story["signal"] == "bullish")
    bearish = sum(1 for story in priority_stories if story["signal"] == "bearish")
    watch = sum(1 for story in priority_stories if story["signal"] == "watch")
    if bearish >= bullish + 2:
        return "风险收缩"
    if bullish >= bearish + 2:
        return "预期修复"
    if watch >= max(bullish, bearish):
        return "验证观察"
    return "事件驱动"


def build_cycle_view(category: dict[str, Any], cycle_name: str, priority_stories: list[dict[str, Any]]) -> str:
    anchor = priority_stories[0]["title"] if priority_stories else "暂无主线"
    if cycle_name == "风险收缩":
        return (
            f"{category['name']} 当前更像风险收缩阶段。主导变量是《{shorten_title(anchor, 24)}》这类负面或不确定 headline。"
            "市场更容易先压估值、后看修复，适合先观察风险是否扩散到价格与资金面。"
        )
    if cycle_name == "预期修复":
        return (
            f"{category['name']} 更像预期修复阶段。《{shorten_title(anchor, 24)}》说明资金开始寻找改善线索，"
            "但是否能演变成趋势，还得看订单、价格或政策有没有二次确认。"
        )
    if cycle_name == "验证观察":
        return (
            f"{category['name']} 仍处在验证观察阶段。headline 已经出现，但市场尚未形成一致定价，"
            "现在更重要的是盯住兑现节奏，而不是急着下结论。"
        )
    return f"{category['name']} 当前更偏事件驱动，headline 对短线影响明显，但持续性取决于后续是否有新的量价或政策验证。"


def build_strategy_take(category: dict[str, Any], cycle_name: str) -> str:
    if cycle_name == "风险收缩":
        return (
            "策略上先别把 headline 直接翻译成交易冲动，而要沿着链条拆影响。当前更适合轻仓、等待验证，"
            "先看负面冲击是否继续扩散到订单、库存、融资或风险偏好；如果只是情绪波动，不适合机械抄底。"
        )
    if cycle_name == "预期修复":
        return (
            "策略上更适合顺着 headline 去找弹性更高的环节，而不是只盯表层受益者。"
            "节奏上优先分批试错、等二次验证，不建议一次性追高；如果后续看到订单、价格或资金接力，再升级成趋势配置。"
        )
    if cycle_name == "验证观察":
        return (
            "策略上先把注意力放在确认变量上。当前更像等待市场给出第二次证据的阶段，"
            "适合列观察名单、做小仓位跟踪，不适合因为单条消息就把它当成长线定投逻辑。"
        )
    return f"策略上要把 {category['name']} 里的事件拆成 headline、链条传导、数据验证三段，只有验证连续出现，才值得提高参与强度。"


def build_linkage_ideas(category: dict[str, Any], cycle_name: str) -> list[str]:
    chains = CATEGORY_PLAYBOOK.get(category["id"], [])
    if not chains:
        return [
            "先盯 headline 能否传导到订单、价格或资金，而不是只看情绪热度。",
            "把板块拆成上游、中游、下游和配套环节，优先跟踪弹性更大的变量。",
            "没有二次验证时，以跟踪和分批观察替代一次性重仓。",
        ]
    if cycle_name == "风险收缩":
        return [chains[2], chains[0], chains[1]][:3]
    return chains[:3]


def build_signal_paragraph(category: dict[str, Any], story: dict[str, Any] | None, signal: str) -> str:
    if story is None:
        return {
            "bullish": "顺风线索暂时不够集中，当前更像局部改善，仍需等待二次验证。",
            "bearish": "逆风线索暂时没有形成单边压制，风险定价还在试探阶段。",
            "watch": "更适合继续观察，等下一条更强的订单、价格或政策线索来确认。",
        }[signal]
    if signal == "bullish":
        return f"顺风线索先看《{shorten_title(story['title'], 30)}》。这类消息值得盯，不在于标题本身，而在于它有没有能力继续推动风险偏好和价格链条。"
    if signal == "bearish":
        return f"逆风压力主要来自《{shorten_title(story['title'], 30)}》。如果这类风险继续扩散，市场会先压估值、再看基本面是否受伤。"
    return f"继续观察《{shorten_title(story['title'], 30)}》。目前更像变量刚露头，真正决定持续性的还是后续兑现速度。"


def build_economist_take(category: dict[str, Any], priority_stories: list[dict[str, Any]], cycle_name: str) -> str:
    anchor = priority_stories[0]["title"] if priority_stories else "暂无主线"
    return (
        f"从定价逻辑看，{category['name']} 当前处在“{cycle_name}”阶段。领头事件是《{shorten_title(anchor, 26)}》，"
        f"但市场真正定价的不是标题本身，而是它能否沿着 {category['lens']} 继续扩散。"
        "如果后续看到订单、价格、库存或流动性继续验证，行情才会从情绪层走向趋势层；如果没有接力，热度更可能停留在短线交易。"
    )


def build_template_comments(
    personas: list[dict[str, Any]],
    category: dict[str, Any],
    stories: list[dict[str, Any]],
    comment_count: int,
) -> list[dict[str, Any]]:
    if not stories:
        return []
    templates = {
        "bullish": (
            "站在 {role} 的角度，这条《{title}》我会先记成偏正面的线索。关键不是 headline 本身，而是它能不能继续传导到 {focus}。",
            "《{title}》如果后面真有订单、价格或资金配合，{category} 这条线会比表面 headline 更有弹性。",
            "这条《{title}》让我更想看二阶变量。要是验证跟上，真正受益的往往不是最直观的对象，而是链条里更高弹性的环节。",
        ),
        "bearish": (
            "《{title}》这条我会先按风险项处理。真正要紧的是它会不会继续伤到 {focus}，而不只是今天吓一下市场。",
            "我对《{title}》偏谨慎。只要后面没有修复动作，{category} 的预期就得重新定价。",
            "这条《{title}》最怕被当成一次性噪音，但如果冲击继续扩散，估值和风险偏好都会先受影响。",
        ),
        "watch": (
            "《{title}》先别急着站队，我更想等后面的数据或政策确认。对 {focus} 来说，节奏比态度更重要。",
            "这条《{title}》现在像变量刚露头，值钱的地方在于它会不会把 {category} 的主线继续往下推。",
            "先观察。《{title}》这类消息如果没有二次验证，通常只停留在短线热度层。",
        ),
    }
    emotions = {"bullish": "偏积极", "bearish": "偏谨慎", "watch": "继续观察"}
    comments = []
    for index, persona in enumerate(personas[:comment_count]):
        story = stories[index % len(stories)]
        signal = story["signal"]
        content = templates[signal][index % len(templates[signal])].format(
            role=persona["role"],
            title=shorten_title(story["title"], 24),
            focus=persona["style"],
            category=category["name"],
        )
        comments.append(
            {
                "role": persona["role"],
                "name": persona["handle"],
                "emotion": emotions[signal],
                "content": content,
            }
        )
    return comments


def selection_sort_key(item: dict[str, Any], window_start: datetime) -> tuple[float, int, str, int, int]:
    return (
        float(item.get("selectionScore", 0)),
        1 if is_within_window(item.get("publishedAt"), window_start) else 0,
        item.get("publishedAt") or "",
        -int(item.get("sourcePriority") or 99),
        -int(item.get("rank") or 9999),
    )


def source_item_sort_key(category: dict[str, Any], item: dict[str, Any], window_start: datetime) -> tuple[int, int, str, int]:
    title = item.get("title", "")
    relevance_score = category_relevance_score(category, title)
    priority_score = market_priority_score(title)
    open_score = a_share_open_score(title)
    return (
        relevance_score + priority_score + open_score,
        1 if is_within_window(item.get("publishedAt"), window_start) else 0,
        item.get("publishedAt") or "",
        -(item.get("rank") or 9999),
    )


def category_relevance_score(category: dict[str, Any], title: str) -> int:
    rules = CATEGORY_RULES.get(category["id"])
    normalized = clean_text(title).lower()
    if not rules:
        return market_priority_score(title)

    must_hits = sum(1 for keyword in rules.get("must", ()) if keyword.lower() in normalized)
    soft_hits = sum(1 for keyword in rules.get("soft", ()) if keyword.lower() in normalized)
    avoid_hits = sum(1 for keyword in rules.get("avoid", ()) if keyword.lower() in normalized)

    score = must_hits * 12 + soft_hits * 6 - avoid_hits * 18

    groups = matched_priority_groups(normalized)
    if len(groups) >= 2:
        score += 10 + (len(groups) - 2) * 4

    priority_score = market_priority_score(title)
    if category["id"] == "focus-news":
        score += priority_score
    elif (must_hits or soft_hits) and category["id"] in {"global-macro", "china-macro", "geopolitics"}:
        score += min(28, priority_score)
    elif (must_hits or soft_hits) and category["id"] == "china-markets":
        score += min(18, priority_score)

    if looks_like_micro_story(title) and category["id"] in {"focus-news", "china-macro", "global-macro", "geopolitics"}:
        score -= 22
    elif looks_like_micro_story(title) and category["id"] == "china-markets":
        score -= 10
    return score


def market_priority_score(title: str) -> int:
    normalized = clean_text(title).lower()
    score = 0
    for keyword, weight in FOCUS_PRIORITY_KEYWORDS.items():
        if keyword.lower() in normalized:
            score += weight

    groups = matched_priority_groups(normalized)
    if len(groups) >= 2:
        score += 12 + (len(groups) - 2) * 6

    intensity_hits = sum(1 for term in MARKET_INTENSITY_TERMS if term.lower() in normalized)
    score += min(18, intensity_hits * 6)

    if re.search(r"创.{0,8}年以来", normalized):
        score += 12
    if re.search(r"(最大|最深|最差|最猛).{0,6}(跌幅|跌|涨幅|波动)", normalized):
        score += 12
    return min(score, 80)


def a_share_open_score(title: str) -> int:
    normalized = clean_text(title).lower()
    score = 0
    for keyword, weight in A_SHARE_OPEN_KEYWORDS.items():
        if keyword.lower() in normalized:
            score += weight

    groups = matched_priority_groups(normalized)
    if "risk_assets" in groups and "global_rates" in groups:
        score += 16
    if "geopolitics" in groups and "commodities" in groups:
        score += 14
    if "china_macro" in groups and ("risk_assets" in groups or "commodities" in groups):
        score += 14
    return min(score, 70)


def matched_priority_groups(normalized: str) -> set[str]:
    return {
        group_name
        for group_name, keywords in MARKET_PRIORITY_GROUPS.items()
        if any(keyword.lower() in normalized for keyword in keywords)
    }


def looks_like_micro_story(title: str) -> bool:
    normalized = clean_text(title).lower()
    if any(pattern.search(title) for pattern in MICRO_STORY_PATTERNS):
        return True
    return any(term.lower() in normalized for term in MICRO_STORY_TERMS)


def micro_story_penalty(category: dict[str, Any], title: str) -> int:
    if not looks_like_micro_story(title):
        return 0
    if category["id"] in {"focus-news", "china-macro", "global-macro", "geopolitics"}:
        return 24
    if category["id"] == "china-markets" and market_priority_score(title) < 12:
        return 12
    return 0


def editorialized_title_penalty(title: str) -> int:
    normalized = clean_text(title).lower()
    score = 0
    if "？" in title or "?" in title:
        score += 10
    if any(term.lower() in normalized for term in EDITORIALIZED_TITLE_TERMS):
        score += 12
    if any(term.lower() in normalized for term in ("重演", "镜像", "不一样", "财富")):
        score += 6
    return min(score, 24)


def should_drop_story(category: dict[str, Any], title: str, selection_score: int) -> bool:
    normalized = clean_text(title).lower()
    rules = CATEGORY_RULES.get(category["id"], {})
    if any(keyword.lower() in normalized for keyword in HARD_DROP_KEYWORDS):
        return True
    avoid_hits = sum(1 for keyword in rules.get("avoid", ()) if keyword.lower() in normalized)
    must_hits = sum(1 for keyword in rules.get("must", ()) if keyword.lower() in normalized)
    soft_hits = sum(1 for keyword in rules.get("soft", ()) if keyword.lower() in normalized)
    groups = matched_priority_groups(normalized)

    if avoid_hits and must_hits == 0 and selection_score < RELEVANCE_THRESHOLDS.get(category["id"], 0) + 8:
        return True
    if category["id"] in {"china-macro", "global-macro", "geopolitics", "china-markets"} and must_hits == 0 and soft_hits == 0:
        return True
    if category["id"] == "focus-news" and must_hits == 0 and soft_hits == 0 and not groups:
        return True
    if category["id"] in {"focus-news", "china-macro", "global-macro", "geopolitics"}:
        if looks_like_micro_story(title) and market_priority_score(title) < 18:
            return True
    return False


def story_impact_score(
    category: dict[str, Any],
    item: dict[str, Any],
    now: datetime,
    window_start: datetime,
) -> int:
    title = clean_text(item["title"]).lower()
    score = 12
    if is_within_window(item.get("publishedAt"), window_start):
        score += 24
    elif item.get("publishedAt") is None:
        score += 10

    source_priority = int(item.get("sourcePriority") or 99)
    score += max(0, 16 - source_priority * 2)

    rank = int(item.get("rank") or 99)
    score += max(0, 10 - min(rank, 10))

    published_dt = parse_timestamp(item.get("publishedAt"))
    if published_dt is not None:
        hours_ago = max(0.0, (now - published_dt).total_seconds() / 3600.0)
        if hours_ago <= 2:
            score += 16
        elif hours_ago <= 6:
            score += 12
        elif hours_ago <= 12:
            score += 8
        elif hours_ago <= 24:
            score += 4

    for keyword, weight in IMPACT_KEYWORDS.items():
        if keyword.lower() in title:
            score += weight
    for keyword, weight in CATEGORY_KEYWORDS.get(category["id"], {}).items():
        if keyword.lower() in title:
            score += weight

    score += max(0, int(item.get("relevanceScore") or 0))
    score += min(36, int(item.get("marketPriority") or 0))
    score += min(24, int(item.get("aShareOpenScore") or 0))
    score -= min(24, int(item.get("microPenalty") or 0))
    score -= min(18, int(item.get("editorialPenalty") or 0))

    if should_drop_story(category, item["title"], int(item.get("selectionScore") or 0)):
        score -= 30

    signal = item.get("signal") or classify_signal(item["title"])
    score += {"bullish": 6, "bearish": 8, "watch": 3}[signal]
    return min(score, 100)


def story_impact_label(score: int) -> str:
    if score >= 82:
        return "高权重"
    if score >= 64:
        return "中权重"
    if score >= 48:
        return "次要线索"
    return "观察线索"


def build_story_summary(category: dict[str, Any], item: dict[str, Any], signal: str) -> str:
    theme = detect_story_theme(item["title"], category)
    if signal == "bullish":
        return f"这条消息更偏向 {theme} 的改善线索，市场会关注它能否继续传导到订单、价格或风险偏好。"
    if signal == "bearish":
        return f"这条消息更像 {theme} 的压力信号，短线容易先压缩预期，后续要看冲击是否继续扩散。"
    return f"这条消息先提供了 {theme} 的新变量，眼下更值得盯后续是否有政策、数据或成交层面的确认。"


def detect_story_theme(title: str, category: dict[str, Any]) -> str:
    lowered = clean_text(title).lower()
    if any(word in lowered for word in ("财报", "净利润", "营收", "业绩")):
        return "业绩兑现"
    if any(word in lowered for word in ("央行", "降息", "降准", "财政", "监管", "政策")):
        return "政策与流动性"
    if any(word in lowered for word in ("订单", "产能", "扩产", "交付", "库存")):
        return "供需与景气"
    if any(word in lowered for word in ("关税", "冲突", "战争", "制裁", "停火")):
        return "风险溢价"
    if any(word in lowered for word in ("大模型", "人工智能", "芯片", "算力", "agent")):
        return "技术与资本开支"
    if any(word in lowered for word in ("热搜", "热榜", "知乎", "微博", "舆情")):
        return "情绪与传播"
    return category["description"]


def build_impact_reason(
    category: dict[str, Any],
    item: dict[str, Any],
    impact_score: int,
    window_start: datetime,
) -> str:
    lens_focus = shorten_title(category["lens"], 18)
    if is_within_window(item.get("publishedAt"), window_start):
        window_copy = "位于昨收后的主窗口"
    else:
        window_copy = "不在主窗口但仍有跟踪价值"
    if impact_score >= 82:
        return f"{window_copy}，而且直接触及 {lens_focus} 这条定价链，因此被列入高权重重点。"
    if impact_score >= 64:
        return f"{window_copy}，对 {lens_focus} 有明确扰动，适合放进重点排序继续追踪。"
    if impact_score >= 48:
        return f"{window_copy}，但更多像辅助线索，后续还需要新的验证。"
    return f"{window_copy}，当前更适合作为背景变量观察。"


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

    write_json(HISTORY_INDEX_PATH, rebuild_history_index()[:120])


def rebuild_history_index() -> list[dict[str, str]]:
    entries = []
    for digest_path in HISTORY_DIR.glob("*/*/digest.json"):
        date_part = digest_path.parent.parent.name
        time_part = digest_path.parent.name
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_part):
            continue
        if not re.fullmatch(r"\d{2}-\d{2}", time_part):
            continue
        entries.append(
            {
                "id": f"{date_part}-{time_part}",
                "label": f"{date_part} {time_part.replace('-', ':')}",
                "path": f"data/history/{date_part}/{time_part}/digest.json",
            }
        )
    entries.sort(key=lambda item: item["id"], reverse=True)
    return entries


def market_window_start(now: datetime) -> datetime:
    anchor = now.replace(hour=15, minute=0, second=0, microsecond=0)
    if now < anchor:
        anchor -= timedelta(days=1)
    while anchor.weekday() >= 5:
        anchor -= timedelta(days=1)
    return anchor


def format_window_label(start: datetime, now: datetime) -> str:
    return f"{start.strftime('%m-%d %H:%M')} - {now.strftime('%m-%d %H:%M')}"


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


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=TZ)
        except ValueError:
            continue
    return None


def is_within_window(value: str | None, window_start: datetime) -> bool:
    dt = parse_timestamp(value)
    return dt is not None and dt >= window_start


def story_time_sort_key(story: dict[str, Any]) -> tuple[int, str, int]:
    return 1 if story.get("publishedAt") else 0, story.get("publishedAt") or "", story.get("impactScore") or 0


def normalize_dedupe_key(title: str, url: str) -> str:
    normalized = normalize_title(title)
    normalized = re.sub(r"^(快讯|突发|更新|最新)+", "", normalized)
    return normalized or url.strip().lower()


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
    normalized = clean_text(title).lower()
    bullish_hits = sum(1 for keyword in BULLISH_KEYWORDS if keyword.lower() in normalized)
    bearish_hits = sum(1 for keyword in BEARISH_KEYWORDS if keyword.lower() in normalized)
    watch_hits = sum(1 for keyword in WATCH_KEYWORDS if keyword.lower() in normalized)
    if bullish_hits > bearish_hits:
        return "bullish"
    if bearish_hits > bullish_hits:
        return "bearish"
    if watch_hits:
        return "watch"
    return "watch"


def signal_label(signal: str) -> str:
    return {"bullish": "偏正面", "bearish": "偏负面", "watch": "待验证"}.get(signal, signal)


def shorten_title(title: str, width: int) -> str:
    title = clean_text(title)
    if len(title) <= width:
        return title
    return title[: max(0, width - 1)] + "…"


def tag_name(tag: str) -> str:
    return tag.split("}", 1)[1] if "}" in tag else tag


def find_nodes(root: ET.Element, name: str) -> list[ET.Element]:
    return [node for node in root.iter() if tag_name(node.tag) == name]


def child_text(node: ET.Element, child_name: str) -> str:
    for child in node:
        if tag_name(child.tag) == child_name:
            return clean_text(child.text or "")
    return ""


def iso_clock(now: datetime) -> str:
    return now.strftime("%Y-%m-%d %H:%M")


def build_cycle_view(category: dict[str, Any], cycle_name: str, priority_stories: list[dict[str, Any]]) -> str:
    anchor = priority_stories[0]["title"] if priority_stories else "暂无主线"
    setup = detect_market_setup(category, priority_stories)
    if category["id"] == "focus-news":
        setup_label = "海外利率上行叠加风险资产重估" if setup == "global_repricing" else "事件驱动的开盘前窗口"
        return f"现在更像{setup_label}。锚点是《{shorten_title(anchor, 24)}》。A股开盘前先看 {open_focus_variables(setup)}。"
    if category["id"] == "china-markets":
        return (
            f"{category['name']} 当前更像先由外盘和情绪定方向、再由量能决定持续性的阶段。"
            f"锚点是《{shorten_title(anchor, 24)}》，更重要的不是 headline 本身，而是竞价后成交额、北向和港股是否给出同向确认。"
        )
    if category["id"] == "global-macro":
        setup_label = "利率抬升叠加地缘扰动" if setup == "global_repricing" else cycle_name
        return (
            f"{category['name']} 当前处在{setup_label}窗口。核心不是单条消息，而是《{shorten_title(anchor, 24)}》这类变量能否继续通过债、股、金、油共振扩散。"
        )
    if category["id"] == "china-macro":
        return (
            f"{category['name']} 当前更像在观察宽信用能否接住弱复苏。领头消息是《{shorten_title(anchor, 24)}》，"
            "真正值得盯的是信用、销售和施工链条有没有跟上，而不是只看政策表态。"
        )
    return (
        f"{category['name']} 当前更偏{cycle_name}阶段，领头 headline 是《{shorten_title(anchor, 24)}》。"
        "现在更重要的是盯住它会不会继续扩散到价格、流动性和资金风格。"
    )


def build_strategy_take(category: dict[str, Any], cycle_name: str, priority_stories: list[dict[str, Any]]) -> str:
    setup = detect_market_setup(category, priority_stories)
    if category["id"] == "focus-news":
        return (
            f"先盯 {open_focus_variables(setup)}，再看 {setup_board_hint(setup)} 谁先被资金确认。"
            "如果竞价、港股和相关期货同向，就按事件节奏分批看；如果只是标题跳空，没有量能接力，先等二次确认。"
        )
    if category["id"] == "china-markets":
        return (
            f"中国市场这边更适合把隔夜消息翻译成板块强弱名单，优先看 {setup_board_hint(setup)}。"
            "先分清是外盘拖动的风险偏好变化，还是国内资金自己愿意扩散；只有量能和主线一起放大，才值得把事件交易升级成趋势仓位。"
        )
    if category["id"] == "china-macro":
        return (
            "中国宏观这边不要把政策 headline 直接当成买点，更应该先判断当前是去杠杆尾声里的宽信用尝试，还是已经进入真正的需求接力。"
            "如果看到社融、地产销售和施工链同时改善，更适合看券商、地产链、玻璃、纯碱、建材这些弹性环节；"
            "如果只有表态没有数据，按事件波段看，不按长线定投看。"
        )
    if category["id"] == "global-macro":
        return (
            f"全球宏观现在更像{'软着陆预期摇摆叠加高利率重估' if setup == 'global_repricing' else '等待二次验证'}。"
            "先盯美债收益率和美元，再看黄金、原油和美股期货是否跟着同向；对A股来说，重点不是预测所有变量，而是判断外盘冲击会先压高估值成长，还是先利多资源与防御。"
        )
    return (
        f"策略上先把 {category['name']} 里的事件拆成 headline、传导链和验证条件三段。"
        "只有验证连续出现，才值得提高参与强度；如果只是单条消息推动，更适合跟踪而不是直接重仓。"
    )


def build_linkage_ideas(category: dict[str, Any], cycle_name: str, priority_stories: list[dict[str, Any]]) -> list[str]:
    setup = detect_market_setup(category, priority_stories)
    if category["id"] == "focus-news" and setup == "global_repricing":
        return [
            "先把中东和油价 headline 拆成“供给扰动 -> 原油/化工期货 -> A股油运、油服、煤化工”的链条，别只停留在地缘叙事本身。",
            "全球债市大跌先影响的是估值久期，A股开盘前更该盯港股互联网、高贝塔成长和中概映射，而不是把所有板块一刀切。",
            "黄金大跌要先分清是流动性挤兑还是风险偏好回升；先看美元、美债收益率和商品是否共振，再判断黄金股、资源股和防御资产该防守还是回补。",
        ]
    if category["id"] == "china-macro":
        return [
            "如果地产政策从表态走向销售和按揭利率改善，A股不一定先买地产本身，更该盯券商、玻璃、纯碱、建材这些弹性更大的环节。",
            "如果宽信用开始落到专项债和实物工作量，再看工程机械、有色和运输链条，通常比总量数据本身更早反映在价格上。",
            "如果只有政策 headline 而社融、销售和价格没有跟上，就按事件波段看，不按长线定投看。",
        ]
    if category["id"] == "china-markets":
        return [
            "隔夜美债、黄金、原油的共振方向，往往先决定A股高估值成长和资源红利谁更占上风。",
            "如果港股和中概先修复，再看券商和高贝塔成长有没有量能接力；没有接力，更多还是情绪反抽。",
            "真正能把事件线做成趋势线的，不是标题数量，而是成交额、北向和相关期货有没有同步放大。",
        ]
    chains = CATEGORY_PLAYBOOK.get(category["id"], [])
    if chains:
        if cycle_name == "风险收缩":
            return [chains[2], chains[0], chains[1]][:3]
        return chains[:3]
    return [
        "先盯 headline 能否传导到订单、价格或资金，而不是只看情绪热度。",
        "把板块拆成上游、中游、下游和配套环节，优先跟踪弹性更大的变量。",
        "没有二次验证时，以跟踪和分批观察替代一次性重仓。",
    ]


def build_economist_take(category: dict[str, Any], priority_stories: list[dict[str, Any]], cycle_name: str) -> str:
    anchor = priority_stories[0]["title"] if priority_stories else "暂无主线"
    setup = detect_market_setup(category, priority_stories)
    if category["id"] == "focus-news":
        return (
            f"把《{shorten_title(anchor, 26)}》放回定价链里看，真正要交易的不是标题本身，"
            "而是它先压估值、先推油金，还是先修复风险偏好。对A股来说，先分清资源、防御和成长谁会先承接。"
        )
    if category["id"] == "china-macro":
        return (
            f"如果把{category['name']}放在弱复苏与宽信用博弈的阶段里看，领头事件《{shorten_title(anchor, 26)}》只有在社融、销售、价格和施工链条同步改善时，"
            "才值得从政策交易升级成中期配置。更高弹性的映射往往不在 headline 表面，而在玻璃、纯碱、建材、券商这类二阶受益环节。"
        )
    if category["id"] == "global-macro":
        return (
            f"从全球定价看，《{shorten_title(anchor, 26)}》代表的是利率、风险偏好和大宗商品重新找平衡。"
            "对A股更关键的是外盘压力最终传到估值还是传到成本，如果是前者，先影响高久期成长；如果是后者，先影响资源、化工和防御资产。"
        )
    return (
        f"从定价逻辑看，{category['name']} 当前处在“{cycle_name}”阶段。领头事件是《{shorten_title(anchor, 26)}》，"
        f"但市场真正定价的不是标题本身，而是它能否沿着 {category['lens']} 继续扩散。"
    )


def build_ai_prompt(
    config: dict[str, Any],
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    window_start: datetime,
    now: datetime,
) -> str:
    news_block = "\n".join(
        f"{index}. [{story['source']}] {story['title']} (时间：{story.get('publishedAt') or '未标注'}；影响分：{story['impactScore']})"
        for index, story in enumerate(all_stories[:FULL_STORY_LIMIT], start=1)
    )
    personas_block = "\n".join(
        f"- 角色：{persona['role']}；网名：{persona['handle']}；关注：{persona['style']}"
        for persona in config["personas"]
    )
    playbook = "\n".join(f"- {item}" for item in CATEGORY_PLAYBOOK.get(category["id"], []))
    return f"""
你是一名中文财经总编兼策略分析师，要为一个正式投研新闻站写单个板块的全天候简报。

板块：{category['name']}
板块描述：{category['description']}
观察镜头：{category['lens']}
分析窗口：{window_start.strftime("%Y-%m-%d %H:%M")} 到 {now.strftime("%Y-%m-%d %H:%M")}（Asia/Shanghai）

原始新闻如下，只能基于这些标题做判断，不要虚构事实、数字、来源或个股代码：
{news_block}

写作要求：
1. 不要出现“AI”“自动生成”等字样。
2. 重点排序必须按影响力，而不是按时间顺序。
3. cycleView 要先回答“现在更像什么周期/阶段”，并明确 A股开盘前最该先看的变量。
4. strategyTake 必须像研究员备忘录，不直接喊单，要讲节奏、验证条件、适合先看哪些板块，以及更适合事件交易还是分批观察/定投。
5. 可以借鉴投资联想法，把 headline 拆成“headline -> 传导链 -> 更高弹性的环节/变量”，但不要机械套用。
6. linkageIdeas 写成三条专业提示，每条都要体现“headline -> 传导链 -> 更该盯的变量/环节”。

这个板块的联想链参考：
{playbook or '- 暂无固定链条，请根据标题自行提炼。'}

必须使用以下 25 个角色与网名，各出现一次：
{personas_block}
""".strip()


def build_category_digest(
    client: Any | None,
    config: dict[str, Any],
    category: dict[str, Any],
    items: list[dict[str, Any]],
    now: datetime,
    window_start: datetime,
) -> tuple[dict[str, Any], str]:
    prepared_items = items or [fallback_story_item(category)]
    all_stories = build_story_records(category, prepared_items, now, window_start)
    raw_priority_stories = rank_priority_stories(category, all_stories)
    priority_stories = synthesize_template_priority_stories(category, all_stories, raw_priority_stories)
    editorial = build_template_editorial(config, category, all_stories, priority_stories, now, window_start)
    mode = "template"

    if client is not None and items:
        try:
            ai_editorial = generate_ai_editorial(
                client,
                config,
                category,
                all_stories,
                raw_priority_stories,
                window_start,
                now,
            )
            ai_priority = normalize_ai_priority_stories(
                all_stories,
                raw_priority_stories,
                ai_editorial.get("priorityItems") or [],
            )
            if ai_priority:
                priority_stories = ai_priority
            editorial.update(
                {
                    "lead": clean_text(ai_editorial["lead"]),
                    "cycleView": clean_text(ai_editorial["cycleView"]),
                    "strategyTake": clean_text(ai_editorial["strategyTake"]),
                    "bullish": clean_text(ai_editorial["bullish"]),
                    "bearish": clean_text(ai_editorial["bearish"]),
                    "watch": clean_text(ai_editorial["watch"]),
                    "economistTake": clean_text(ai_editorial["economistTake"]),
                    "linkageIdeas": [clean_text(item) for item in ai_editorial["linkageIdeas"][:3]],
                    "personaComments": normalize_persona_comments(
                        ai_editorial["personaComments"],
                        config["personas"],
                        category,
                        priority_stories,
                        config["persona_count"],
                    ),
                }
            )
            mode = "ai"
        except Exception as exc:  # noqa: BLE001
            print(f"[ai] {category['id']}: failed -> {exc}")

    return (
        {
            "id": category["id"],
            "name": category["name"],
            "description": category["description"],
            "lens": category["lens"],
            "windowLabel": format_window_label(window_start, now),
            "lead": editorial["lead"],
            "cycleView": editorial["cycleView"],
            "strategyTake": editorial["strategyTake"],
            "linkageIdeas": editorial["linkageIdeas"],
            "bullish": editorial["bullish"],
            "bearish": editorial["bearish"],
            "watch": editorial["watch"],
            "economistTake": editorial["economistTake"],
            "priorityStories": priority_stories,
            "allStories": all_stories,
            "stories": priority_stories,
            "personaComments": editorial["personaComments"][: config["persona_count"]],
            "sourcesUsed": unique_sources_from_stories(all_stories),
            "stats": {
                "allStoryCount": len(all_stories),
                "priorityCount": len(priority_stories),
            },
        },
        mode,
    )


def synthesize_template_priority_stories(
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    raw_priority_stories: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if category["id"] == "focus-news":
        return synthesize_focus_priority_stories(all_stories, raw_priority_stories)
    if category["id"] not in {"china-markets", "global-macro", "china-macro"}:
        return raw_priority_stories

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for story in raw_priority_stories:
        synthetic = build_template_priority_story(category, story)
        key = normalize_title(synthetic["title"])
        if not key or key in seen:
            continue
        seen.add(key)
        item = dict(synthetic)
        item["priorityRank"] = len(result) + 1
        result.append(item)
        if len(result) >= PRIORITY_LIMIT:
            break
    return result or raw_priority_stories


def synthesize_focus_priority_stories(
    all_stories: list[dict[str, Any]],
    raw_priority_stories: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    candidates = sorted(
        all_stories or raw_priority_stories,
        key=focus_story_candidate_key,
        reverse=True,
    )[: max(PRIORITY_LIMIT * 4, 24)]
    if not candidates:
        return raw_priority_stories

    clusters: dict[str, list[dict[str, Any]]] = {}
    for story in candidates:
        theme = focus_priority_theme(story["title"])
        clusters.setdefault(theme, []).append(story)

    if "middle-east-escalation" in clusters:
        clusters.pop("geopolitics-generic", None)
    if "gold-slump" in clusters:
        clusters.pop("gold-generic", None)
    if "us-equity-selloff" in clusters:
        clusters.pop("equities-generic", None)
    if "oil-supply-repricing" in clusters:
        clusters.pop("energy-generic", None)
    if "global-bond-selloff" in clusters or "fed-repricing" in clusters:
        clusters.pop("policy-generic", None)
    clusters.pop("other-generic", None)

    items = [build_focus_priority_story(theme, stories) for theme, stories in clusters.items()]
    items.sort(
        key=lambda item: (
            float(item.get("clusterScore", 0)),
            float(item.get("impactScore", 0)),
            item.get("publishedAt") or "",
        ),
        reverse=True,
    )
    result = items[:PRIORITY_LIMIT] or raw_priority_stories
    for index, item in enumerate(result, start=1):
        item["priorityRank"] = index
    return result


def focus_story_candidate_key(story: dict[str, Any]) -> tuple[float, float, int, str]:
    return (
        float(story.get("selectionScore", story.get("impactScore", 0))),
        float(story.get("marketPriority", 0)) + float(story.get("aShareOpenScore", 0)),
        1 if story.get("inWindow") else 0,
        story.get("publishedAt") or "",
    )


def focus_lead_story_key(story: dict[str, Any]) -> tuple[int, float, float, int, str]:
    editorial_penalty = int(story.get("editorialPenalty") or 0)
    return (
        max(0, 24 - editorial_penalty),
        float(story.get("selectionScore", story.get("impactScore", 0))),
        float(story.get("marketPriority", 0)) + float(story.get("aShareOpenScore", 0)),
        1 if story.get("inWindow") else 0,
        story.get("publishedAt") or "",
    )


def focus_priority_theme(title: str) -> str:
    normalized = clean_text(title).lower()
    if any(term in normalized for term in ("全球债市", "债市", "美债", "英债", "收益率")) and any(
        term in normalized for term in ("血洗", "大跌", "重挫", "抬升", "上行", "首破", "加息概率")
    ):
        return "global-bond-selloff"
    if any(term in normalized for term in ("黄金", "金价")) and any(
        term in normalized for term in ("1983", "单周跌幅", "暴跌", "大抛售", "走弱", "回落")
    ):
        return "gold-slump"
    if "特朗普" in normalized and any(
        term in normalized for term in ("减少对伊", "减少对伊朗", "逐步考虑减少", "降级", "停火", "缓和", "转涨")
    ):
        return "trump-deescalation"
    if any(term in normalized for term in ("霍尔木兹", "伊朗石油", "石油制裁", "供油", "原油缓冲", "日本船只", "天然气供应恢复")):
        return "oil-supply-repricing"
    if any(term in normalized for term in ("原油", "油价")) and any(
        term in normalized for term in ("供应", "豁免", "缓冲", "通过", "回落")
    ):
        return "oil-supply-repricing"
    if any(term in normalized for term in ("美股", "纳指", "标普", "道指", "三大指数", "中概")) and any(
        term in normalized for term in ("连跌", "下挫", "收低", "齐挫", "跌超", "承压")
    ):
        return "us-equity-selloff"
    if any(term in normalized for term in ("美联储", "加息", "降息", "非农", "cpi", "ppi")) and any(
        term in normalized for term in ("概率", "预期", "路径", "摇摆", "再定价")
    ):
        return "fed-repricing"
    if any(term in normalized for term in ("中东", "伊朗", "以色列", "纳坦兹", "空袭", "袭击", "海峡", "报复", "核设施")):
        return "middle-east-escalation"
    return f"{priority_story_bucket(title)}-generic"


def build_focus_priority_story(theme: str, stories: list[dict[str, Any]]) -> dict[str, Any]:
    ranked = sorted(stories, key=focus_story_candidate_key, reverse=True)
    lead_story = sorted(stories, key=focus_lead_story_key, reverse=True)[0]
    lead = dict(lead_story)
    lead["title"] = focus_theme_title(theme, ranked)
    lead["summary"] = focus_theme_summary(theme, ranked)
    lead["impactReason"] = focus_theme_reason(theme, ranked)
    lead["reason"] = lead["impactReason"]
    lead["signal"] = focus_theme_signal(theme)
    lead["signalLabel"] = signal_label(lead["signal"])
    lead["isSynthetic"] = True
    lead["sourceTitle"] = lead_story["title"]
    lead["theme"] = theme
    lead["relatedTitles"] = [story["title"] for story in ranked[:3]]
    lead["clusterScore"] = focus_theme_score(theme, ranked)
    lead["clusterSize"] = len(ranked)
    lead["sourceCount"] = len({story.get("sourceId") or story.get("source") for story in ranked})
    lead["impactLabel"] = story_impact_label(int(lead.get("impactScore") or 0))
    return lead


def focus_theme_score(theme: str, stories: list[dict[str, Any]]) -> int:
    base = {
        "global-bond-selloff": 34,
        "trump-deescalation": 30,
        "us-equity-selloff": 28,
        "gold-slump": 28,
        "oil-supply-repricing": 26,
        "middle-east-escalation": 16,
        "fed-repricing": 22,
    }.get(theme, 12)
    best = max(int(story.get("selectionScore") or story.get("impactScore") or 0) for story in stories)
    count_bonus = min(15, len(stories) * 3)
    source_bonus = len({story.get("sourceId") or story.get("source") for story in stories}) * 4
    return base + best + count_bonus + source_bonus


def focus_theme_signal(theme: str) -> str:
    mapping = {
        "global-bond-selloff": "bearish",
        "us-equity-selloff": "bearish",
        "gold-slump": "watch",
        "trump-deescalation": "bullish",
        "oil-supply-repricing": "watch",
        "middle-east-escalation": "watch",
        "fed-repricing": "watch",
    }
    return mapping.get(theme, "watch")


def focus_theme_title(theme: str, stories: list[dict[str, Any]]) -> str:
    titles_blob = " ".join(clean_text(story["title"]).lower() for story in stories[:3])
    if theme == "global-bond-selloff":
        return "全球债市重挫，长端利率继续上行"
    if theme == "gold-slump":
        if any(term in titles_blob for term in ("1983", "单周跌幅", "大抛售")):
            return "黄金创1983年以来最大单周跌幅"
        return "黄金回撤，避险资产重新定价"
    if theme == "us-equity-selloff":
        return "美股三大指数连跌，成长资产继续承压"
    if theme == "trump-deescalation":
        return "特朗普口径转向，市场押注中东风险边际降温"
    if theme == "oil-supply-repricing":
        if any(term in titles_blob for term in ("豁免", "供油", "制裁")):
            return "伊朗石油豁免引发重估，原油供应链再起波动"
        return "霍尔木兹风险未退，原油供应链继续重估"
    if theme == "middle-east-escalation":
        return "中东冲突反复，地缘风险仍未退出交易"
    if theme == "fed-repricing":
        return "加息预期回摆，全球资产重新找平衡"
    return rewrite_priority_title("focus-news", priority_story_bucket(stories[0]["title"]), stories[0]["title"])


def focus_theme_summary(theme: str, stories: list[dict[str, Any]]) -> str:
    mapping = {
        "global-bond-selloff": "加息预期回摆后，美债与英债同步承压，全球估值中枢继续上修。",
        "gold-slump": "避险资产大幅回撤，市场更在交易利率与流动性，而不只是地缘情绪。",
        "us-equity-selloff": "纳指、中概与科技资产走弱，外盘风险偏好仍未稳住。",
        "trump-deescalation": "军事表态从升级转向缓和，油价回落，风险资产短线修复。",
        "oil-supply-repricing": "石油豁免、霍尔木兹通道和供给预期反复，原油与化工链仍在重估。",
        "middle-east-escalation": "袭击、报复与航道风险反复出现，地缘溢价还没有退场。",
        "fed-repricing": "美联储路径被重新定价，债、股、金同步调整。",
    }
    if theme in mapping:
        return mapping[theme]
    return rewrite_priority_summary("focus-news", priority_story_bucket(stories[0]["title"]), stories[0])


def focus_theme_reason(theme: str, stories: list[dict[str, Any]]) -> str:
    mapping = {
        "global-bond-selloff": "先看美债、美元和离岸人民币。",
        "gold-slump": "先分清流动性冲击还是风险偏好回升。",
        "us-equity-selloff": "先看美股期货、港股科技和北向情绪。",
        "trump-deescalation": "先看原油能否继续回落。",
        "oil-supply-repricing": "先看油价、运价和煤化工期货。",
        "middle-east-escalation": "先看原油、黄金和防御板块。",
        "fed-repricing": "先看利率、美元与高估值成长。",
    }
    if theme in mapping:
        return mapping[theme]
    return rewrite_priority_reason("focus-news", priority_story_bucket(stories[0]["title"]), stories[0])


def build_template_priority_story(category: dict[str, Any], story: dict[str, Any]) -> dict[str, Any]:
    bucket = priority_story_bucket(story["title"])
    item = dict(story)
    item["title"] = rewrite_priority_title(category["id"], bucket, story["title"])
    item["summary"] = rewrite_priority_summary(category["id"], bucket, story)
    item["impactReason"] = rewrite_priority_reason(category["id"], bucket, story)
    item["reason"] = item["impactReason"]
    item["isSynthetic"] = True
    item["sourceTitle"] = story["title"]
    return item


def rewrite_priority_title(category_id: str, bucket: str, raw_title: str) -> str:
    title = clean_text(raw_title)
    normalized = title.lower()
    if bucket == "bonds":
        return "全球债市大跌，美债与英债收益率同步抬升"
    if bucket == "gold":
        if any(word in normalized for word in ("1983", "最大单周跌幅", "大抛售")):
            return "黄金创1983年以来最大单周跌幅"
        return "黄金走弱，避险资产重新定价"
    if bucket == "equities":
        if any(word in normalized for word in ("三大指数", "纳指", "标普", "道指", "连跌")):
            return "美股三大指数连跌，成长资产继续承压"
        return "风险资产承压，权益市场继续分化"
    if bucket == "energy":
        return "中东扰动未消，原油供应链仍在重估"
    if bucket == "policy":
        return "美联储路径再摇摆，全球资产重新找平衡"
    if bucket == "geopolitics":
        if "特朗普" in normalized and any(word in normalized for word in ("伊朗", "军事行动", "降级", "停火")):
            return "特朗普口径转向，市场押注中东局势边际缓和"
        return "中东局势反复，地缘风险仍是主导变量"
    if category_id == "china-macro":
        return f"中国宏观仍待验证：{shorten_title(title, 22)}"
    return title


def rewrite_priority_summary(category_id: str, bucket: str, story: dict[str, Any]) -> str:
    fallback = clean_text(story.get("summary") or story.get("impactReason") or story["title"])
    if category_id == "focus-news":
        mapping = {
            "bonds": "这类消息真正影响的不是单一债券，而是全球利率中枢与A股估值久期，先看美债收益率、美元和港股成长是否同向。",
            "gold": "这类消息要先分清是流动性踩踏还是避险回落，再看黄金股、资源股和防御资产是否同步重估。",
            "equities": "先盯美股期货、中概和港股科技，再看A股高贝塔成长会不会被外盘拖累。",
            "energy": "关键不只是油价本身，而是原油能否继续传导到化工、航运和A股上游资源链。",
            "policy": "真正的验证变量是利率、美元和风险偏好，而不是单条表态本身。",
            "geopolitics": "市场更关心冲突会不会继续推升油价与避险需求，而不是单个战报本身。",
        }
        return mapping.get(bucket, fallback)
    if category_id == "china-markets":
        mapping = {
            "equities": "A股开盘前更该看竞价、成交额、北向和港股是否一起给出确认。",
            "bonds": "如果海外利率继续上行，先压的是高估值久期资产，再看资源与防御有没有承接。",
            "gold": "黄金与资源板块的反应，能帮助判断当前是风险收缩还是流动性扰动。",
        }
        return mapping.get(bucket, fallback)
    if category_id == "china-macro":
        mapping = {
            "policy": "只有社融、销售和施工链条同步改善，才值得把政策 headline 升级成配置逻辑。",
            "energy": "海外商品冲击会先通过成本和价格传导，再影响国内顺周期与制造业利润。",
        }
        return mapping.get(bucket, fallback)
    return fallback


def rewrite_priority_reason(category_id: str, bucket: str, story: dict[str, Any]) -> str:
    if category_id == "focus-news":
        mapping = {
            "bonds": "先影响全球利率定价，再压制A股与港股高估值资产。",
            "gold": "先决定避险资产强弱，再影响资源股和防御仓位配置。",
            "equities": "先影响隔夜风险偏好，再决定A股次日是修复还是继续去估值。",
            "energy": "先改写油价预期，再向油运、油服、煤化工和化工期货扩散。",
            "policy": "先影响美债和美元，再传导至黄金、原油与权益市场风格。",
            "geopolitics": "先改变中东风险溢价，再决定油价与全球风险资产的开盘方向。",
        }
        return mapping.get(bucket, story.get("impactReason") or story.get("reason") or "")
    return story.get("impactReason") or story.get("reason") or ""


def generate_ai_editorial(
    client: Any,
    config: dict[str, Any],
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    raw_priority_stories: list[dict[str, Any]],
    window_start: datetime,
    now: datetime,
) -> dict[str, Any]:
    max_priority = min(PRIORITY_LIMIT, max(6, len(raw_priority_stories)))
    schema = {
        "type": "object",
        "properties": {
            "lead": {"type": "string"},
            "cycleView": {"type": "string"},
            "strategyTake": {"type": "string"},
            "bullish": {"type": "string"},
            "bearish": {"type": "string"},
            "watch": {"type": "string"},
            "economistTake": {"type": "string"},
            "linkageIdeas": {
                "type": "array",
                "minItems": 3,
                "maxItems": 3,
                "items": {"type": "string"},
            },
            "priorityItems": {
                "type": "array",
                "minItems": min(6, max_priority),
                "maxItems": max_priority,
                "items": {
                    "type": "object",
                    "properties": {
                        "title": {"type": "string"},
                        "summary": {"type": "string"},
                        "signal": {"type": "string", "enum": ["bullish", "bearish", "watch"]},
                        "impact": {"type": "string"},
                        "sourceTitle": {"type": "string"},
                    },
                    "required": ["title", "summary", "signal", "impact", "sourceTitle"],
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
            "cycleView",
            "strategyTake",
            "bullish",
            "bearish",
            "watch",
            "economistTake",
            "linkageIdeas",
            "priorityItems",
            "personaComments",
        ],
        "additionalProperties": False,
    }
    response = client.responses.create(
        model=os.environ.get("OPENAI_MODEL", "gpt-5-mini"),
        input=build_ai_prompt(config, category, all_stories, raw_priority_stories, window_start, now),
        text={
            "format": {
                "type": "json_schema",
                "name": "daily_news_editorial_board_v3",
                "strict": True,
                "schema": schema,
            }
        },
    )
    return json.loads(response.output_text)


def build_ai_prompt(
    config: dict[str, Any],
    category: dict[str, Any],
    all_stories: list[dict[str, Any]],
    raw_priority_stories: list[dict[str, Any]],
    window_start: datetime,
    now: datetime,
) -> str:
    news_block = "\n".join(
        f"{index}. [{story['source']}] {story['title']} (时间：{story.get('publishedAt') or '未标注'}；影响分：{story['impactScore']})"
        for index, story in enumerate(all_stories[:FULL_STORY_LIMIT], start=1)
    )
    candidate_block = "\n".join(
        f"- {story['title']}"
        for story in raw_priority_stories[:PRIORITY_LIMIT]
    )
    personas_block = "\n".join(
        f"- 角色：{persona['role']}；网名：{persona['handle']}；关注：{persona['style']}"
        for persona in config["personas"]
    )
    playbook = "\n".join(f"- {item}" for item in CATEGORY_PLAYBOOK.get(category["id"], []))
    return f"""
你是一名中文财经总编兼投研编辑，要为正式市场简报撰写一个板块的重点摘要。

板块：{category['name']}
板块描述：{category['description']}
观察镜头：{category['lens']}
分析窗口：{window_start.strftime("%Y-%m-%d %H:%M")} 到 {now.strftime("%Y-%m-%d %H:%M")}（Asia/Shanghai）

原始新闻如下，只能基于这些标题判断，不要虚构事实、数字、来源或个股代码：
{news_block}

原始候选重点如下，它们只代表候选，不代表最终写法：
{candidate_block}

写作要求：
1. 不要出现“AI”“自动生成”等字样。
2. priorityItems 不是简单复述原始标题，而是要把同主题的多条快讯归并成一个“市场主线标题”。
3. priorityItems 的 title 要像财经编辑标题，短、硬、像栏目标题，避免空话、套话和机器腔。
4. 优先保留真正会影响A股下一交易日定价的变量：美债、美元、油价、黄金、美股三大指数、中东局势、关税、人民币、社融、地产、政策。
5. 单公司新闻除非会改变板块风格或市场主线，否则不要放进 priorityItems。
6. 同一事件不要拆成三四条重复快讯，要合并成一条更高层的市场线索。
7. strategyTake 要像研究员晨会备忘录，回答“先看什么变量、适合看什么板块、等确认还是可以做事件节奏”，不要直接喊单。
8. linkageIdeas 必须体现“headline -> 传导链 -> 更该盯的变量/环节”。
9. sourceTitle 必须填写一个最能代表该主线的原始标题，且必须来自给定原始新闻，方便前端挂链接。

你可以参考的重点层级示例：
- 全球债市大跌，美债与英债收益率同步抬升
- 美股三大指数连跌四周，成长资产继续承压
- 黄金创1983年以来最大单周跌幅
- 特朗普口径转向，市场押注中东局势边际缓和
- 中东扰动未消，原油供应链仍在重估

这个板块的联想链参考：
{playbook or '- 暂无固定链条，请根据标题自行提炼。'}

必须使用以下 25 个角色与网名，各出现一次：
{personas_block}
""".strip()


def normalize_ai_priority_stories(
    all_stories: list[dict[str, Any]],
    raw_priority_stories: list[dict[str, Any]],
    priority_items: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not priority_items:
        return []
    title_map = {normalize_title(story["title"]): story for story in all_stories}
    fallback_pool = raw_priority_stories or all_stories
    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in priority_items:
        source_story = match_story_by_title(title_map, fallback_pool, item.get("sourceTitle", ""))
        if source_story is None:
            source_story = fallback_pool[min(len(result), len(fallback_pool) - 1)]
        key = normalize_title(item["title"])
        if not key or key in seen:
            continue
        seen.add(key)
        story = dict(source_story)
        story["title"] = clean_text(item["title"])
        story["summary"] = clean_text(item["summary"])
        story["signal"] = item.get("signal") or source_story.get("signal") or "watch"
        story["signalLabel"] = signal_label(story["signal"])
        story["impactReason"] = clean_text(item["impact"])
        story["reason"] = story["impactReason"]
        story["impactLabel"] = source_story.get("impactLabel") or story_impact_label(int(source_story.get("impactScore") or 70))
        story["priorityRank"] = len(result) + 1
        story["isSynthetic"] = True
        story["sourceTitle"] = source_story["title"]
        result.append(story)
        if len(result) >= PRIORITY_LIMIT:
            break
    return result


def match_story_by_title(
    title_map: dict[str, dict[str, Any]],
    fallback_pool: list[dict[str, Any]],
    source_title: str,
) -> dict[str, Any] | None:
    normalized = normalize_title(source_title)
    if normalized in title_map:
        return title_map[normalized]
    if not normalized:
        return None
    for story in fallback_pool:
        story_normalized = normalize_title(story["title"])
        if normalized in story_normalized or story_normalized in normalized:
            return story
    return None


if __name__ == "__main__":
    sys.exit(main())
