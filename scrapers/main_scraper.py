#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HK/SG Quant Jobs - 主爬虫 (scrapers/main_scraper.py)
=====================================================

零依赖实现：只用 Python 标准库，不需要 pip install 任何东西。
Playwright 仅在 custom 基金显式开启时「延迟导入」，没装也不影响主流程。

分层（对应全栈开发规范，CLI 版本）
    http_get_json          传输层：超时 / 重试 / 退避 / 4xx 快速失败
    fetch_*               适配层：把 ATS / 官网 JSON API / 聚合源的字段翻译成统一内部格式
    finalize_job           领域层：地点过滤 + quant 关键词过滤 + 打标签 + 雇主类型
    classify_employer      雇主类型：直招 / 猎头（规则见 funds_config.py 第 3b 节）
    deduplicate            去重层：canonical URL -> 公司+标题+地点，官方源优先，确定性选优
    write_output           存储层：原子写 data/jobs.json
    main                   编排层：并发、统计、错误处理、CLI

运行
    python scrapers/main_scraper.py
    python scrapers/main_scraper.py --no-content --workers 4
    python scrapers/main_scraper.py --fund point72 --dry-run --verbose
    python scrapers/main_scraper.py --fund "LinkedIn Public" --dry-run   # 手动跑 disabled 源
    python scrapers/main_scraper.py --locations "Hong Kong"

    # 需要 APIFY_API_TOKEN；产出独立缓存，由全量抓取自动合并进 jobs.json
    python scrapers/main_scraper.py --fund "LinkedIn via Apify" \
        --output data/linkedin_apify_cache.json

输出
    data/jobs.json  （顶层保留 fetched_at / total_jobs，兼容现有 frontend/index.html）
"""

from __future__ import annotations

import argparse
import html
import json
import logging
import os
import re
import ssl
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import Request, urlopen

# 允许 `python scrapers/main_scraper.py` 和 `cd scrapers && python main_scraper.py` 两种用法
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from funds_config import (  # noqa: E402
    AGENCY_DESCRIPTION_SIGNALS,
    AGENCY_SIGNAL_SOURCES,
    DIRECT_EMPLOYER_OVERRIDES,
    EXCLUDE_TITLE_KEYWORDS,
    FRESHTEAM_ENDPOINTS,
    FUNDS,
    PLAYWRIGHT_TEMPLATE,
    QUANT_KEYWORDS,
    RECRUITER_AGENCIES,
    RECRUITER_NAME_PATTERNS,
    ROLE_KEYWORDS,
    SETTINGS,
    SKILL_KEYWORDS,
    STRONG_QUANT_KEYWORDS,
    TARGET_LOCATIONS,
)

LOG = logging.getLogger("quantjobs")
SSL_CONTEXT = ssl.create_default_context()
SCHEMA_VERSION = "1.0"


class ScraperError(Exception):
    """可预期的抓取失败（网络 / 解析 / 配置），不应中断其它基金。"""


# ---------------------------------------------------------------------------
# 传输层
# ---------------------------------------------------------------------------

def _request(
    url: str,
    opts: "RunOptions",
    *,
    retries: Optional[int] = None,
    method: str = "GET",
    data: Optional[bytes] = None,
    extra_headers: Optional[Dict[str, str]] = None,
    timeout_override: Optional[float] = None,
) -> bytes:
    """
    发起 GET/POST。5xx / 429 / 网络错误重试；4xx 直接失败（重试也救不了）。

    timeout_override 给「同步运行型」API 用：Apify run-sync 最长 300s，
    不能沿用常规抓取的 20s socket 超时。
    """
    max_retries = opts.retries if retries is None else retries
    headers = {
        "User-Agent": opts.user_agent,
        "Accept": "application/json, text/html;q=0.9, */*;q=0.8",
        "Accept-Encoding": "identity",  # urllib 不自动解 gzip，明确关闭
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "close",
    }
    if data is not None and method.upper() in ("POST", "PUT", "PATCH"):
        headers.setdefault("Content-Type", "application/json")
        headers.setdefault("Content-Length", str(len(data)))
    if extra_headers:
        headers.update(extra_headers)
    last_error: Optional[Exception] = None

    socket_timeout = timeout_override if timeout_override is not None else opts.timeout
    for attempt in range(max_retries + 1):
        try:
            req = Request(url, data=data, headers=headers, method=method)
            with urlopen(req, timeout=socket_timeout, context=SSL_CONTEXT) as resp:
                return resp.read()
        except HTTPError as exc:
            last_error = exc
            if exc.code < 500 and exc.code != 429:
                # 4xx 带上响应体片段：Apify 402 = 免费额度耗尽，必须一眼看出来
                detail = ""
                try:
                    detail = exc.read(240).decode("utf-8", "replace").strip()
                except Exception:  # noqa: BLE001 - 诊断信息读不到就算了
                    pass
                raise ScraperError(
                    f"HTTP {exc.code} for {url}" + (f" -> {detail}" if detail else "")
                ) from exc
        except (URLError, TimeoutError, ssl.SSLError, OSError) as exc:
            last_error = exc

        if attempt < max_retries:
            time.sleep(0.6 * (2 ** attempt))  # 指数退避：0.6s / 1.2s

    raise ScraperError(f"请求失败（重试 {max_retries} 次）: {url} -> {last_error}")


def _json_get(url: str, opts: "RunOptions", *, retries: Optional[int] = None) -> Any:
    raw = _request(url, opts, retries=retries)
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise ScraperError(f"响应不是合法 JSON: {url} ({raw[:80]!r})") from exc


# ---------------------------------------------------------------------------
# 通用工具
# ---------------------------------------------------------------------------

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def ms_to_iso(value: Any) -> str:
    """Lever 的 createdAt 是毫秒时间戳。"""
    try:
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
    except (TypeError, ValueError, OSError, OverflowError):
        return ""


def epoch_to_iso(value: Any) -> str:
    """Eightfold 的 t_create / t_update 可能是秒或毫秒时间戳，也可能是字符串。"""
    if isinstance(value, str) and "T" in value:
        return value[:19].replace(" ", "T") + ("Z" if value.endswith("Z") else "")
    try:
        number = float(value)
    except (TypeError, ValueError):
        return ""
    if number > 1e11:  # 毫秒
        number /= 1000.0
    try:
        return datetime.fromtimestamp(number, tz=timezone.utc).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
    except (OSError, OverflowError, ValueError):
        return ""


def capture(pattern: str, text: str) -> str:
    """从 HTML 片段里取第一个捕获组并转成纯文本（LinkedIn 卡片用）。"""
    match = re.search(pattern, text, flags=re.S | re.IGNORECASE)
    return html_to_text(match.group(1)) if match else ""


def html_to_text(raw: Any) -> str:
    """Greenhouse content 是实体转义过的 HTML，转成纯文本。"""
    if not raw:
        return ""
    text = html.unescape(html.unescape(str(raw)))
    text = re.sub(r"(?is)<(script|style|noscript).*?</\1>", " ", text)
    text = re.sub(r"(?i)<br\s*/?>|</p>|</li>|</div>|</h[1-6]>|</tr>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"[ \t\xa0]+", " ", text)
    text = re.sub(r"\n\s*\n+", "\n", text)
    return text.strip()


def unique(items: Sequence[str]) -> List[str]:
    seen, out = set(), []
    for item in items:
        value = (item or "").strip()
        if value and value.lower() not in seen:
            seen.add(value.lower())
            out.append(value)
    return out


def raw_job(**kwargs: Any) -> Dict[str, Any]:
    """所有适配器统一输出的内部格式。"""
    base = {
        "source": "", "slug": "", "company": "", "raw_id": "", "title": "",
        "url": "", "apply_url": "", "department": "", "description": "",
        "posted_at": "", "updated_at": "", "location_candidates": [],
        "career_url": "", "company_override": "",
    }
    base.update(kwargs)
    return base


# 只影响「同一个职位」识别、不影响职位本身的查询参数。
# 例如 Greenhouse 的 ?gh_jid= 只是来源追踪，squarepoint 的 ?id= 才是岗位 id，
# 所以这里用黑名单，绝不无脑清空全部 query。
TRACKING_PARAMS = {
    "gh_jid", "gh_src", "utm_source", "utm_medium", "utm_campaign",
    "utm_term", "utm_content", "utm_id", "source", "src", "ref", "referrer",
    "fbclid", "gclid", "msclkid", "mc_cid", "mc_eid", "_ga", "spm",
    # LinkedIn 搜索结果链接的追踪 / 分页参数：同一岗位每次抓都不同
    "position", "pagenum", "refid", "trackingid", "trk", "lipi", "licu",
    "originalsubdomain", "ebp", "logintype", "track",
}
TRACKING_PREFIXES = ("utm_", "attribution_", "pk_", "vero_")
# 同一 ATS 常有多个对外域名；统一后 canonical URL 才能真正相等。
URL_HOST_ALIASES = {
    "job-boards.greenhouse.io": "boards.greenhouse.io",
    "boards.greenhouse.io": "boards.greenhouse.io",
    "www.efinancialcareers.com": "efinancialcareers.com",
    "www.linkedin.com": "linkedin.com",
}


def _is_tracking_param(key: str) -> bool:
    lower = key.lower()
    return lower in TRACKING_PARAMS or lower.startswith(TRACKING_PREFIXES)


def canonical_url(url: str) -> str:
    """
    把「同一个职位的不同链接」折叠成同一个键。

    处理：tracking / utm 参数、查询参数顺序、fragment、末尾斜杠、www、
    默认端口、Greenhouse 双域名、Lever 的 /apply 后缀。LinkedIn /jobs/view/
    的 query 全是可变追踪信息，直接清空（否则每天都会当成新岗位）。
    """
    try:
        parts = urlsplit((url or "").strip())
    except ValueError:
        return (url or "").strip()
    if not parts.scheme or not parts.netloc:
        return (url or "").strip()

    host = re.sub(r":(?:80|443)$", "", parts.netloc.lower())
    if host.startswith("www."):
        host = host[4:]
    host = URL_HOST_ALIASES.get(host, host)

    path = parts.path.rstrip("/")
    if "lever.co" in host:
        path = re.sub(r"/apply$", "", path, flags=re.IGNORECASE)

    if "linkedin.com" in host and "/jobs/view/" in path.lower():
        query = ""
    else:
        # sorted() 让 ?a=1&b=2 与 ?b=2&a=1 得到同一个键。
        query = urlencode(sorted(
            (k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True)
            if not _is_tracking_param(k)
        ))
    return urlunsplit((parts.scheme.lower(), host, path, query, ""))


# ---------------------------------------------------------------------------
# 去重层
# ---------------------------------------------------------------------------
# 同一岗位被多渠道抓到时，保留 tier 更小的那条（公司官方 ATS / 官网 API
# 永远优于聚合站，聚合站优于第三方社交抓取）。新增数据源时在这里登记即可。
SOURCE_TIER: Dict[str, int] = {
    "greenhouse": 0, "lever": 0, "freshteam": 0,
    "eightfold": 0, "phenom": 0, "polymer_api": 0,
    "optiver_api": 0, "custom": 0,
    "eFC": 1,
    "LinkedIn": 2, "linkedin_guest": 2,
}
DEFAULT_SOURCE_TIER = 1

# 设为 True 可开启「JD 差异保护」：同一渠道里两条都带长 JD、但用词差异很大
# 的同名岗位（例如 Jane Street 的实习体验项目 vs 全职研究岗）会被视为不同
# posting 而保留。默认 False = 严格执行「公司名 + 职位标题 + 地点」规则，
# 保证前端不出现同公司同名同地点的明显重复条目；被折叠掉的不同 JD 数量
# 会记在 dedupe.distinct_descriptions_collapsed 里，便于评估这个取舍。
IDENTITY_DESCRIPTION_GUARD = False

# 只剥离法律实体后缀，不动业务词：Citadel / Citadel Securities 是两家公司，
# Jane Street / Jane Street Capital 才是同一家。
_LEGAL_SUFFIXES = {
    "ltd", "limited", "llc", "inc", "incorporated", "plc", "pte", "pvt",
    "private", "co", "company", "corp", "corporation", "gmbh", "sa", "ag",
    "holdings", "group", "lp", "llp", "sarl", "bv", "pty",
}
_TITLE_LOCATION_TAIL = re.compile(
    r"[\-,/|]\s*(?:hong\s*kong|singapore|hk|sg|apac|asia(?:\s+pacific)?)\b.*$",
    re.IGNORECASE,
)
_TITLE_LOCATION_PAREN = re.compile(
    r"\((?:hong\s*kong|singapore|hk|sg|apac)[^)]*\)", re.IGNORECASE
)


def _ascii_lower(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return text.encode("ascii", "ignore").decode("ascii").lower()


def normalize_company(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", " ", _ascii_lower(value)).strip()
    tokens = text.split()
    while tokens and tokens[-1] in _LEGAL_SUFFIXES:
        tokens.pop()
    return " ".join(tokens)


# ---------------------------------------------------------------------------
# 雇主类型：直招 vs 猎头
#   规则全部来自 funds_config.py（第 3b 节），这里只做归一化 + 判定。
#   聚合站的 company 是「发广告的公司」，常是猎头而非真实雇主；A 类官方
#   ATS 源天然是直招，但仍统一走一遍判定，保证字段永远存在。
# ---------------------------------------------------------------------------
_AGENCY_KEYS = frozenset(normalize_company(n) for n in RECRUITER_AGENCIES)
_DIRECT_KEYS = frozenset(normalize_company(n) for n in DIRECT_EMPLOYER_OVERRIDES)
_AGENCY_NAME_RE = re.compile(
    "|".join("(?:%s)" % p for p in RECRUITER_NAME_PATTERNS), re.IGNORECASE
)
_AGENCY_DESC_RE = re.compile(
    "|".join("(?:%s)" % p for p in AGENCY_DESCRIPTION_SIGNALS), re.IGNORECASE
)


def classify_employer(company: Any, source: Any, description: Any = "") -> str:
    """返回 "direct" 或 "agency"；判定顺序：白名单 > 名称关键词 > JD 措辞。"""
    key = normalize_company(company)
    if key in _DIRECT_KEYS:
        return "direct"
    if key in _AGENCY_KEYS:
        return "agency"
    if _AGENCY_NAME_RE.search(str(company or "")):
        return "agency"
    # JD 措辞只在聚合站来源上生效：银行 / 交易所的直招 JD 也可能提到
    # "our client"，但那指的是业务客户，不是代招的雇主。
    if str(source or "") in AGENCY_SIGNAL_SOURCES and _AGENCY_DESC_RE.search(str(description or "")):
        return "agency"
    return "direct"


def normalize_title(value: Any) -> str:
    """去掉标题里的地点限定；保留 Senior / Intern / C++ 等真正区分岗位的词。"""
    text = _ascii_lower(value)
    text = _TITLE_LOCATION_PAREN.sub(" ", text)
    text = _TITLE_LOCATION_TAIL.sub(" ", text)
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9+#]+", " ", text)).strip()


def normalize_locations(job: Dict[str, Any]) -> List[str]:
    values = job.get("locations") or []
    if not values and job.get("location"):
        values = [part.strip() for part in str(job["location"]).split(",")]
    out = sorted({_ascii_lower(v).strip() for v in values if str(v).strip()})
    return [v for v in out if v]


def source_tier(job: Dict[str, Any]) -> int:
    return SOURCE_TIER.get(str(job.get("source", "")), DEFAULT_SOURCE_TIER)


def completeness_score(job: Dict[str, Any]) -> float:
    """同一优先级内的胜负手：信息越完整分越高。"""
    # 600 字以内保持原有权重（最高 60 分）；更完整的 JD 再小幅加分，
    # 这样「完整度优先」在长 JD 之间仍然有区分度，又不挤压其他字段。
    desc_len = len(job.get("description") or "")
    score = min(desc_len, 600) / 10.0 + min(max(desc_len - 600, 0), 2400) / 100.0
    score += 20 if job.get("posted_at") else 0
    score += 6 if job.get("updated_at") else 0
    score += 8 if job.get("department") else 0
    score += 5 if job.get("career_url") else 0
    score += 5 if job.get("apply_url") else 0
    score += min(len(job.get("tags") or []), 6) * 2
    score += min(len(job.get("matched_keywords") or []), 4) * 2
    score += min(len(normalize_locations(job)), 2) * 3
    score += 3 if str(job.get("url") or "").startswith("https://") else 0
    return score


def _word_set(text: str) -> set:
    return set(re.findall(r"[a-z0-9+#]{3,}", _ascii_lower(text)))


def description_similarity(a: str, b: str) -> float:
    wa, wb = _word_set(a), _word_set(b)
    if not wa or not wb:
        return 1.0
    return len(wa & wb) / len(wa | wb)


def _distinct_postings(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """
    防误合并安全阀：同一渠道里，两条都带长 JD 且用词差异很大的记录，
    更可能是同名的两个不同岗位（例如不同 team 的 Quantitative Researcher），
    此时不使用「公司+标题+地点」合并。跨渠道不做此限制，因为 JD 是两套模板。
    """
    if not IDENTITY_DESCRIPTION_GUARD:
        return False
    if a.get("source") != b.get("source"):
        return False
    da, db = a.get("description") or "", b.get("description") or ""
    if len(da) < 150 or len(db) < 150:
        return False
    # 只比前缀：5k 字 JD 尾部的免责声明/福利模板会稀释 Jaccard 相似度，
    # 让同一岗位的两条记录看起来像不同岗位。前 1200 字足以区分职责。
    return description_similarity(da[:1200], db[:1200]) < 0.35


class _DisjointSet:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))

    def find(self, x: int) -> int:
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> bool:
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return False
        self.parent[max(ra, rb)] = min(ra, rb)
        return True


def deduplicate(
    candidates: Sequence[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    确定性的两级去重：
      1) canonical URL（去掉 tracking 参数）——最可靠；
      2) 公司名 + 职位标题 + 地点（每个地点各注册一个键，HK 与 HK+SG 也能对上）。
    job.id 作为同源兜底键。用并查集把等价记录分组，每组选「官方来源优先、
    其次信息更完整」的一条，其余全部丢弃。并发完成顺序不再影响结果。
    """
    n = len(candidates)
    dsu = _DisjointSet(n)
    url_map: Dict[str, int] = {}
    identity_map: Dict[Tuple[str, str, str], int] = {}
    id_map: Dict[str, int] = {}
    events = {"by_url": 0, "by_identity": 0, "by_id": 0}

    def link(i: int, j: int, reason: str) -> None:
        if dsu.union(i, j):
            events[reason] += 1

    for i, job in enumerate(candidates):
        curl = canonical_url(str(job.get("url") or ""))
        if curl and curl.startswith("http"):
            if curl in url_map:
                link(i, url_map[curl], "by_url")
            else:
                url_map[curl] = i

        key = str(job.get("id") or "")
        if key:
            if key in id_map:
                link(i, id_map[key], "by_id")
            else:
                id_map[key] = i

        company = normalize_company(job.get("company"))
        title = normalize_title(job.get("title"))
        locs = normalize_locations(job)
        if not company or not title or not locs:
            continue
        for loc in locs:
            ident = (company, title, loc)
            if ident in identity_map:
                owner = identity_map[ident]
                if not _distinct_postings(candidates[owner], job):
                    link(i, owner, "by_identity")
            else:
                identity_map[ident] = i

    groups: Dict[int, List[int]] = {}
    for i in range(n):
        groups.setdefault(dsu.find(i), []).append(i)

    tiers = [source_tier(j) for j in candidates]
    scores = [completeness_score(j) for j in candidates]
    # 完全基于内容排序：不依赖 ThreadPoolExecutor 的完成顺序，结果可复现。
    stables = [
        (str(j.get("source") or ""), str(j.get("id") or ""),
         canonical_url(str(j.get("url") or "")))
        for j in candidates
    ]

    def rank(i: int) -> Tuple[int, float, Tuple[str, str, str]]:
        return (tiers[i], -scores[i], stables[i])

    kept: List[Dict[str, Any]] = []
    preferred = {"official_source": 0, "completeness": 0, "stable_order": 0}
    enriched_fields = 0
    collapsed_descriptions = 0
    for members in groups.values():
        winner = min(members, key=rank)
        job = candidates[winner]
        losers = sorted((m for m in members if m != winner), key=rank)
        for loser in losers:
            if tiers[loser] > tiers[winner]:
                preferred["official_source"] += 1
            elif scores[winner] > scores[loser]:
                preferred["completeness"] += 1
            else:
                preferred["stable_order"] += 1
            da, db = job.get("description") or "", candidates[loser].get("description") or ""
            if len(da) >= 150 and len(db) >= 150 and description_similarity(da, db) < 0.35:
                collapsed_descriptions += 1
        # 只补 winner 缺失的字段，绝不覆盖已有值（尤其不能让猎头公司名回流）。
        for field in ("posted_at", "updated_at", "department", "description", "career_url"):
            if not job.get(field):
                for loser in losers:
                    value = candidates[loser].get(field)
                    if value:
                        job[field] = value
                        enriched_fields += 1
                        break
        kept.append(job)

    kept.sort(key=lambda j: (
        -j["quant_score"], j["company"].lower(), j["title"].lower(), str(j.get("id") or "")
    ))
    report = {
        "strategy": "canonical_url -> company+title+location; official > aggregator > LinkedIn; then completeness",
        "candidates": n,
        "kept": len(kept),
        "removed_total": n - len(kept),
        "by_url": events["by_url"],
        "by_identity": events["by_identity"],
        "by_id": events["by_id"],
        "merged_groups": sum(1 for g in groups.values() if len(g) > 1),
        "largest_group": max((len(g) for g in groups.values()), default=0),
        "official_source_preferred": preferred["official_source"],
        "completeness_preferred": preferred["completeness"],
        "stable_order_preferred": preferred["stable_order"],
        "fields_enriched": enriched_fields,
        "distinct_descriptions_collapsed": collapsed_descriptions,
    }
    return kept, report


# ---------------------------------------------------------------------------
# 适配层：Greenhouse / Lever / Freshteam / custom
# ---------------------------------------------------------------------------

def fetch_greenhouse(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"""
    slug = fund["ats_slug"]
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    if opts.with_content:
        url += "?content=true"

    data = _json_get(url, opts)
    records = data.get("jobs", []) if isinstance(data, dict) else []
    jobs = []
    for item in records:
        offices = item.get("offices") or []
        loc_obj = item.get("location") or {}
        # 注意：location.name 可能是合并字符串 "Hong Kong, Singapore, Japan"
        candidates: List[str] = [str(loc_obj.get("name", ""))]
        for office in offices:
            if isinstance(office, dict):
                candidates.extend([str(office.get("name", "")), str(office.get("location", ""))])
        # 合并字符串拆开，便于逐条匹配
        for chunk in re.split(r"[,/|]", loc_obj.get("name", "") or ""):
            candidates.append(chunk.strip())

        departments = [d.get("name", "") for d in (item.get("departments") or []) if isinstance(d, dict)]
        description = html_to_text(item.get("content")) if opts.with_content else ""
        posted = item.get("first_published") or item.get("updated_at") or ""

        jobs.append(raw_job(
            source="greenhouse", slug=slug, company=fund["name"],
            raw_id=str(item.get("id", "")), title=str(item.get("title", "")),
            url=str(item.get("absolute_url", "")), apply_url=str(item.get("absolute_url", "")),
            department=", ".join(unique(departments)), description=description,
            posted_at=str(posted), updated_at=str(item.get("updated_at", "")),
            location_candidates=unique(candidates)[:12],
            career_url=fund.get("career_url", ""),
        ))
    return jobs, ("ok" if jobs else "empty"), f"Greenhouse {slug}: {len(jobs)} 条原始职位"


def fetch_lever(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """GET https://api.lever.co/v0/postings/{slug}?mode=json"""
    slug = fund["ats_slug"]
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    data = _json_get(url, opts)
    records = data if isinstance(data, list) else []
    jobs = []
    for item in records:
        cats = item.get("categories") or {}
        candidates: List[str] = [str(cats.get("location", ""))]
        candidates.extend(str(x) for x in (cats.get("allLocations") or []))
        job_id = str(item.get("id", ""))
        hosted = str(item.get("hostedUrl") or "")
        apply_url = str(item.get("applyUrl") or "")
        link = hosted or apply_url or f"https://jobs.lever.co/{slug}/{job_id}"

        jobs.append(raw_job(
            source="lever", slug=slug, company=fund["name"], raw_id=job_id,
            title=str(item.get("text", "")), url=link, apply_url=apply_url or link,
            department=", ".join(unique([str(cats.get("department", "")), str(cats.get("team", ""))])),
            description=str(item.get("descriptionPlain") or "") or html_to_text(item.get("description")),
            posted_at=ms_to_iso(item.get("createdAt")), updated_at=ms_to_iso(item.get("createdAt")),
            location_candidates=unique(candidates), career_url=fund.get("career_url", ""),
        ))
    return jobs, ("ok" if jobs else "empty"), f"Lever {slug}: {len(jobs)} 条原始职位"


def fetch_freshteam(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    Freshteam 没有统一的公开 API 契约，逐个尝试常见端点；
    全部拿不到就自动降级到 Playwright 分支。
    """
    base = (fund.get("career_url") or f"https://{fund['ats_slug']}.freshteam.com").rstrip("/")
    for path in FRESHTEAM_ENDPOINTS:
        try:
            data = _json_get(base + path, opts, retries=0)
        except ScraperError:
            continue
        records = data.get("jobs", data) if isinstance(data, dict) else data
        if not isinstance(records, list) or not records:
            continue
        jobs = []
        for item in records:
            if not isinstance(item, dict):
                continue
            job_id = str(item.get("id") or item.get("slug") or "")
            jobs.append(raw_job(
                source="freshteam", slug=str(fund.get("ats_slug", "")),
                company=fund["name"], raw_id=job_id,
                title=str(item.get("title") or item.get("name") or ""),
                url=str(item.get("url") or item.get("job_url") or f"{base}/jobs/{job_id}"),
                apply_url=str(item.get("url") or item.get("job_url") or ""),
                description=html_to_text(item.get("description")),
                posted_at=str(item.get("created_at") or item.get("published_at") or ""),
                location_candidates=unique([str(item.get("location") or "")]),
                career_url=fund.get("career_url", ""),
            ))
        if jobs:
            return jobs, "ok", f"Freshteam {path}: {len(jobs)} 条原始职位"

    LOG.info("[SKIP] %s 的 Freshteam 公开端点不可用，降级到 Playwright 分支", fund["name"])
    return fetch_custom(fund, opts)


def fetch_custom(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    Playwright 预留入口：默认关闭，保证零依赖可直接运行。

    开启方式：把 funds_config.py 中该基金的 playwright.enabled 改为 True 并填好选择器。
    首次使用：pip install playwright && playwright install chromium
    """
    cfg = dict(PLAYWRIGHT_TEMPLATE)
    cfg.update(fund.get("playwright") or {})
    if not cfg.get("enabled"):
        return [], "needs_playwright", f"{fund['name']}: 官网自研页面，需开启 Playwright（当前未启用）"

    try:
        from playwright.sync_api import sync_playwright  # 延迟导入，零依赖主流程
    except ImportError:
        return [], "needs_playwright", (
            f"{fund['name']}: 缺少 playwright，执行 "
            "`pip install playwright && playwright install chromium`"
        )

    url = cfg.get("url") or fund.get("career_url", "")
    selector = cfg.get("job_link_selector") or "a[href*='job']"
    js = """
    els => els.map(e => ({
        text: (e.innerText || '').trim(),
        href: e.href,
        context: ((e.closest('li, tr, article, [class*=job]') || e.parentElement || e).innerText || '').slice(0, 300)
    }))
    """
    jobs = []
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=opts.user_agent)
            page.goto(url, wait_until=cfg.get("wait_until", "domcontentloaded"), timeout=opts.timeout * 1000)
            if cfg.get("wait_for_selector"):
                page.wait_for_selector(cfg["wait_for_selector"], timeout=opts.timeout * 1000)
            if cfg.get("extra_wait_ms"):
                page.wait_for_timeout(int(cfg["extra_wait_ms"]))
            links = page.eval_on_selector_all(selector, js)
            browser.close()
    except Exception as exc:  # Playwright 自身异常类型较多，统一收敛
        raise ScraperError(f"Playwright 抓取失败 {url}: {type(exc).__name__}: {exc}") from exc

    for idx, link in enumerate(links[: int(cfg.get("max_jobs", 100))]):
        title = (link.get("text") or "").split("\n")[0].strip()
        href = link.get("href") or ""
        if not title or not href:
            continue
        context = link.get("context") or ""
        jobs.append(raw_job(
            source="custom", slug=re.sub(r"[^a-z0-9]+", "-", fund["name"].lower()).strip("-"),
            company=fund["name"], raw_id=str(idx) + "-" + str(abs(hash(href)) % 10**8),
            title=title[:200], url=href, apply_url=href,
            description=context, posted_at="", updated_at="",
            location_candidates=[context[:300], title],
            career_url=fund.get("career_url", ""),
        ))
    return jobs, ("ok" if jobs else "empty"), f"Playwright {url}: {len(jobs)} 条链接"


# ---------------------------------------------------------------------------
# 适配层：官网自有 JSON API（无需浏览器，零成本，可进每日自动更新）
# ---------------------------------------------------------------------------

def fetch_polymer_api(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    Polymer Capital：官网前端 bundle 里硬编码的公开接口。
    POST https://careers-api.polymerpraxis.com/Jobs  （GET 会 405，POST {} 返回 33 条 JSON）
    申请链接由 Workable 的 entityName + shortcode 拼出，实测 HTTP 200。
    """
    url = fund.get("api_url") or "https://careers-api.polymerpraxis.com/Jobs"
    raw = _request(url, opts, method="POST", data=b"{}")
    try:
        records = json.loads(raw.decode("utf-8", "replace"))
    except ValueError as exc:
        raise ScraperError(f"Polymer /Jobs 返回非 JSON: {url}") from exc
    if not isinstance(records, list):
        raise ScraperError("Polymer /Jobs 结构变化：期望数组")

    jobs: List[Dict] = []
    for item in records:
        if not isinstance(item, dict):
            continue
        loc = item.get("location") or {}
        candidates = [str(loc.get(k) or "") for k in ("country", "city", "region", "countryCode")]
        for extra in item.get("locations") or []:
            if isinstance(extra, dict):
                candidates.extend(str(extra.get(k) or "") for k in ("country", "city", "region"))

        shortcode = str(item.get("shortcode") or "")
        entity = str(item.get("entityName") or "polymer-capital")
        apply_url = (
            f"https://apply.workable.com/{entity}/j/{shortcode}/"
            if shortcode else fund.get("career_url", "")
        )
        deps = item.get("department") or []
        department = ", ".join(str(x) for x in deps) if isinstance(deps, list) else str(deps)
        published = str(item.get("published") or "")[:10]

        jobs.append(raw_job(
            source="polymer_api", slug=entity, company=fund["name"],
            raw_id=str(item.get("id") or shortcode),
            title=str(item.get("title") or ""), url=apply_url, apply_url=apply_url,
            department=department,
            description=" ".join(filter(None, [department, str(item.get("type") or "")])),
            posted_at=published, updated_at=published,
            location_candidates=unique(candidates), career_url=fund.get("career_url", ""),
        ))
    return jobs, ("ok" if jobs else "empty"), f"Polymer API: {len(jobs)} 条原始职位"


def fetch_eightfold_api(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    Millennium：Eightfold 招聘站的公开 JSON 接口（页内 <code id="smartApplyData">
    同源数据）。每页固定 10 条，num 参数无效，但 start 分页有效，location 会改变
    count（HK 35 / SG 16）。按地点逐页翻完即可覆盖全部 HK/SG 岗位。
    """
    base = fund.get("api_url") or "https://career.mlp.com/api/apply/v2/jobs"
    domain = str(fund.get("domain") or "mlp.com")
    locations = fund.get("location_queries") or ["Hong Kong", "Singapore"]
    page_size, max_pages = 10, int(fund.get("max_pages", 8))
    collected: Dict[str, Dict] = {}
    problems: List[str] = []

    for loc in locations:
        for page in range(max_pages):
            query = urlencode({"domain": domain, "start": page * page_size,
                               "num": page_size, "location": loc})
            try:
                raw = _request(base + "?" + query, opts, retries=0)
                data = json.loads(raw.decode("utf-8", "replace"))
            except (ScraperError, ValueError) as exc:
                problems.append(f"{loc}/p{page}: {type(exc).__name__}")
                break
            positions = data.get("positions") or [] if isinstance(data, dict) else []
            if not positions:
                break
            for pos in positions:
                if not isinstance(pos, dict):
                    continue
                jid = str(pos.get("id") or pos.get("display_job_id") or "")
                if jid and jid not in collected:
                    collected[jid] = pos
            try:
                total = int(data.get("count") or 0)
            except (TypeError, ValueError):
                total = 0
            if total and (page + 1) * page_size >= total:
                break
            time.sleep(0.25)  # 轻限速，别把对方站点打疼

    def full_description(pos: Dict[str, Any]) -> str:
        """列表接口的 job_description 恒为空；完整 JD 需按 id 请求详情接口。

        详情端点与列表同源：/api/apply/v2/jobs/<id>?domain=<domain>，返回
        job_description 的 HTML 正文（实测 ~2.8k 字符）。逐条轻限速，失败时
        静默降级为空，不能因为一个详情页 404 拖垮整个数据源。
        """
        inline = html_to_text(pos.get("job_description") or "")
        if inline:
            return inline
        pid = str(pos.get("id") or "")
        if not pid:
            return ""
        url = base.rstrip("/") + "/" + pid + "?" + urlencode({"domain": domain})
        try:
            detail = _json_get(url, opts, retries=0)
            return html_to_text(detail.get("job_description") or "") if isinstance(detail, dict) else ""
        except (ScraperError, ValueError) as exc:
            LOG.debug("Eightfold 详情 %s 不可用: %s", pid, exc)
            return ""
        finally:
            time.sleep(0.2)  # 轻限速：一次全量约 30 个详情请求

    jobs: List[Dict] = []
    with_description = 0
    for jid, pos in collected.items():
        desc = full_description(pos)
        if desc:
            with_description += 1
        candidates = [str(pos.get("location") or "")]
        candidates.extend(str(x) for x in (pos.get("locations") or []))
        link = str(pos.get("canonicalPositionUrl") or
                   f"https://mlp.eightfold.ai/careers/job/{jid}")
        jobs.append(raw_job(
            source="eightfold", slug=domain, company=fund["name"], raw_id=jid,
            title=str(pos.get("name") or pos.get("posting_name") or ""),
            url=link, apply_url=link,
            department=", ".join(unique([
                str(pos.get("department") or ""), str(pos.get("business_unit") or ""),
            ])),
            description=desc,
            posted_at=epoch_to_iso(pos.get("t_create")),
            updated_at=epoch_to_iso(pos.get("t_update")),
            location_candidates=unique(candidates), career_url=fund.get("career_url", ""),
        ))
    if not jobs:
        raise ScraperError("Eightfold 全部查询无结果: " + "; ".join(problems[:3]))
    return jobs, "ok", (f"Eightfold {domain}: {len(jobs)} 条原始职位，"
                        f"{with_description} 条带完整 JD")


def fetch_phenom_api(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    Susquehanna (SIG)：Phenom 招聘站的公开 /api/jobs。
    实测 location=Hong Kong&limit=100 返回 33 条 HK 岗；location=Singapore 当前 0 条。
    字段在 jobs[].data 里：title / city / country / posted_date / apply_url / description。
    """
    base = fund.get("api_url") or "https://careers.sig.com/api/jobs"
    locations = fund.get("location_queries") or ["Hong Kong", "Singapore"]
    jobs: List[Dict] = []
    problems: List[str] = []

    for loc in locations:
        url = base + "?" + urlencode({"location": loc, "limit": int(fund.get("page_size", 100))})
        try:
            raw = _request(url, opts, retries=0)
            data = json.loads(raw.decode("utf-8", "replace"))
        except (ScraperError, ValueError) as exc:
            problems.append(f"{loc}: {type(exc).__name__}")
            continue
        rows = data.get("jobs") or [] if isinstance(data, dict) else []
        for row in rows:
            item = row.get("data", row) if isinstance(row, dict) else {}
            if not item or not isinstance(item, dict):
                continue
            jid = str(item.get("slug") or item.get("req_id") or "")
            link = str(item.get("apply_url") or f"https://careers.sig.com/jobs/{jid}")
            cats = item.get("categories")
            department = ", ".join(str(x) for x in cats) if isinstance(cats, list) else str(cats or "")
            # SIG 把 JD 拆成 description（职责/介绍）与 qualifications（要求）
            # 两个字段，原站两部分都显示。合并后才算完整 JD。
            desc = html_to_text(item.get("description") or "")
            quals = html_to_text(item.get("qualifications") or "")
            if quals and quals[:80] not in desc:
                desc = (desc + "\n\n" + quals).strip()
            jobs.append(raw_job(
                source="phenom", slug=str(fund.get("ats_slug") or "sig"),
                company=fund["name"], raw_id=jid, title=str(item.get("title") or ""),
                url=link, apply_url=link, department=department,
                description=desc,
                posted_at=str(item.get("posted_date") or "")[:10],
                updated_at=str(item.get("update_date") or "")[:10],
                location_candidates=unique([
                    str(item.get(k) or "") for k in ("city", "state", "country", "location_name")
                ]),
                career_url=fund.get("career_url", ""),
            ))
    if not jobs and problems:
        raise ScraperError("Phenom API 全部失败: " + "; ".join(problems))
    return jobs, ("ok" if jobs else "empty"), f"Phenom SIG: {len(jobs)} 条原始职位"


def fetch_optiver_api(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    Optiver：官网 /en/api/v1/jobs 返回 items[]，但实测 totalCount=160 时只给前 16 条，
    page/limit/offset/office 等参数均被忽略 —— 数据不完整，默认不启用。
    保留适配器，等找到真正的分页参数（或改用 Playwright 遍历筛选页）再打开。
    """
    url = fund.get("api_url") or "https://www.optiver.com/en/api/v1/jobs"
    data = _json_get(url, opts)
    items = data.get("items") or [] if isinstance(data, dict) else []
    total = data.get("totalCount", "?") if isinstance(data, dict) else "?"
    jobs: List[Dict] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        href = str(item.get("href") or "")
        link = urljoin("https://www.optiver.com", href) if href else fund.get("career_url", "")
        jobs.append(raw_job(
            source="optiver_api", slug="optiver", company=fund["name"],
            raw_id=str(item.get("componentID") or abs(hash(href)) % 10**10),
            title=str(item.get("title") or ""), url=link, apply_url=link,
            department=str(item.get("domain") or ""),
            description=" ".join(filter(None, [str(item.get("experience") or ""),
                                              str(item.get("domain") or "")])),
            posted_at="", updated_at="",
            location_candidates=unique([str(item.get("location") or "")]),
            career_url=fund.get("career_url", ""),
        ))
    return jobs, ("ok" if jobs else "empty"), f"Optiver API: {len(jobs)}/{total} 条原始职位"


# ---------------------------------------------------------------------------
# 适配层：聚合类数据源（eFC 已启用；LinkedIn guest 仍默认 disabled）
# ---------------------------------------------------------------------------

def fetch_efc_api(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    eFinancialCareers：Angular SSR 页内 ng-state 暴露的 job-search-ui JSON 接口。
    实测关键参数：q=<关键词>&location=<城市>&page=<n>&pageSize=<n>；
    countryCode2 与 q 同用时会串入法国/美国岗位，location 才可靠。
    已按需求纳入 scrape.yml 日常自动更新。为降低对站点的压力：retries=0、
    翻页间 sleep 10s（crawl-delay）、单源异常由 collect_fund() 隔离，
    eFC 失败不影响其余官方源。
    注意：主站 robots.txt 仍 Disallow /search、/v1，若出现 403/429 应停用。
    """
    base = (fund.get("api_url") or
            "https://job-search-ui.efinancialcareers.com/v1/efc/jobs/search")
    site = fund.get("site_url") or "https://www.efinancialcareers.com"
    queries = fund.get("queries") or []
    max_pages = int(fund.get("max_pages", 2))
    page_size = int(fund.get("page_size", 20))
    jobs: List[Dict] = []
    problems: List[str] = []

    for query in queries:
        keyword = str(query.get("keyword") or "")
        location = str(query.get("location") or "")
        for page in range(1, max_pages + 1):
            params = urlencode({"q": keyword, "location": location, "page": page,
                                "pageSize": page_size, "culture": "en"})
            try:
                raw = _request(base + "?" + params, opts, retries=0)
                data = json.loads(raw.decode("utf-8", "replace"))
            except (ScraperError, ValueError) as exc:
                problems.append(f"{keyword}/{location}/p{page}: {type(exc).__name__}")
                break
            rows = data.get("data") or [] if isinstance(data, dict) else []
            if not rows:
                break
            for item in rows:
                if not isinstance(item, dict):
                    continue
                jl = item.get("jobLocation") or {}
                detail = str(item.get("detailsPageUrl") or "")
                link = detail if detail.startswith("http") else (
                    site + ("" if detail.startswith("/") else "/") + detail
                )
                jobs.append(raw_job(
                    source="eFC", slug="efc", company="eFinancialCareers",
                    company_override=str(item.get("companyName") or
                                         item.get("clientBrandName") or ""),
                    raw_id=str(item.get("id") or item.get("jobId") or ""),
                    title=str(item.get("title") or ""), url=link, apply_url=link,
                    department=str(item.get("positionType") or item.get("employmentType") or ""),
                    # `summary` 是固定 ~500 字的搜索结果摘要（会在半句话处截断）；
                    # `description` 才是完整 JD（实测 5k+ 字符）。优先取完整版，
                    # 只有老接口没有 description 时才退回 summary。
                    description=html_to_text(item.get("description") or item.get("summary") or ""),
                    posted_at=str(item.get("postedDate") or "")[:10], updated_at="",
                    location_candidates=unique([
                        str(jl.get(k) or "") for k in
                        ("displayName", "city", "state", "country", "region")
                    ]),
                    career_url=site,
                ))
            if len(rows) < page_size:
                break
            time.sleep(float(fund.get("delay_seconds", 2.0)))  # 主站 robots 要求 crawl-delay: 10
    if not jobs and problems:
        raise ScraperError("eFC API 全部失败: " + "; ".join(problems[:3]))
    return jobs, ("ok" if jobs else "empty"), f"eFC API: {len(jobs)} 条原始职位"


def fetch_linkedin_guest(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    """
    LinkedIn guest jobs 端点：每页 10 条 HTML 卡片，无需登录、无需 JS。
    实测连续 6 页均 200；但 robots.txt 明确 Disallow: /jobs-guest/，
    且 LinkedIn ToS 禁止自动抓取，随时可能 999 / IP 封禁 —— 默认 disabled。
    仅供人工一次性补数据，切勿放进 GitHub Actions 定时任务。
    """
    base = (fund.get("api_url") or
            "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search")
    queries = fund.get("queries") or []
    max_pages = int(fund.get("max_pages", 3))
    jobs: List[Dict] = []
    problems: List[str] = []

    for query in queries:
        keyword = str(query.get("keyword") or "")
        location = str(query.get("location") or "")
        for page in range(max_pages):
            params = urlencode({"keywords": keyword, "location": location,
                                "start": page * 10})
            try:
                body = _request(
                    base + "?" + params, opts, retries=0,
                    extra_headers={"Accept": "text/html,application/xhtml+xml"},
                ).decode("utf-8", "replace")
            except ScraperError as exc:
                problems.append(f"{keyword}/{location}/p{page}: {exc}")
                break
            cards = re.findall(r"<li>.*?</li>", body, flags=re.S)
            if not cards:
                break
            for card in cards:
                title = capture(r'class="base-search-card__title"[^>]*>(.*?)</h3>', card)
                company = capture(r'class="hidden-nested-link"[^>]*>(.*?)</a>', card)
                loc = capture(r'class="job-search-card__location"[^>]*>(.*?)</span>', card)
                posted = capture(r"<time[^>]*datetime=\"([^\"]+)\"", card)
                urn = capture(r'data-entity-urn="urn:li:jobPosting:(\d+)"', card)
                href = capture(r'class="base-card__full-link[^>]*href="([^"]+)"', card)
                if not title or not href:
                    continue
                jobs.append(raw_job(
                    source="linkedin_guest", slug="linkedin", company="LinkedIn",
                    company_override=company, raw_id=urn or str(abs(hash(href)) % 10**12),
                    title=title, url=href, apply_url=href, department="",
                    description="", posted_at=posted, updated_at=posted,
                    location_candidates=unique([loc, title, company]),
                    career_url="https://www.linkedin.com/jobs/",
                ))
            if len(cards) < 10:
                break
            time.sleep(float(fund.get("delay_seconds", 1.5)))
    if not jobs and problems:
        raise ScraperError("LinkedIn guest 全部失败: " + "; ".join(problems[:3]))
    return jobs, ("ok" if jobs else "empty"), f"LinkedIn guest: {len(jobs)} 条原始卡片"


# ---------------------------------------------------------------------------
# 适配层：Apify LinkedIn（按结果付费，默认 disabled）
#
# 设计要点
#   1. 只走 Apify REST API，不装 apify-client，保持纯标准库零依赖。
#   2. 同步端点 POST /v2/actors/{actorId}/run-sync-get-dataset-items
#      超过 300s 返回 408，因此 query timeout=240 + socket timeout=300。
#   3. retries=0：POST 会启动一次付费 run，自动重试 = 重复扣费。
#   4. 三层硬上限：Actor input 的 limit / API 的 limit（返回条数）/
#      API 的 maxItems（计费条数）+ maxTotalChargeUsd（单次 run 金额）。
#   5. 结果 source="LinkedIn"，company_override=真实雇主名。
#   6. 该源产出的结果写到独立缓存文件，由 run() 在「全量抓取」时合并，
#      这样每天 4 次的免费官方源 workflow 不会重复调用付费 Actor。
# ---------------------------------------------------------------------------

APIFY_DEFAULT_ACTOR = "valig~linkedin-jobs-scraper"
APIFY_CACHE_FILE = "data/linkedin_apify_cache.json"
APIFY_CACHE_TTL_HOURS = 72


def apify_actor_input(fund: Dict[str, Any], query: Dict[str, Any], actor_id: str) -> Dict[str, Any]:
    """把统一 queries 翻译成两个 Actor 各自的 input 字段。"""
    keyword = str(query.get("keyword") or "")
    location = str(query.get("location") or "")
    per_query = int(fund.get("limit_per_query", 50))
    if "curious_coder" in actor_id:
        # curious_coder 的 datePosted 是 past24Hours / pastWeek / pastMonth，
        # limitPerSource 才是每次搜索的上限；scrapeCompany 默认 True 会为每条
        # 岗位额外发请求，本项目用不到公司详情，关掉省钱省时间。
        return {
            "keywords": keyword,
            "location": location,
            "datePosted": fund.get("cc_date_posted", "past24Hours"),
            "limitPerSource": per_query,
            "scrapeCompany": False,
        }
    payload: Dict[str, Any] = {
        "keywords": keyword,
        "location": location,
        "datePosted": fund.get("date_posted", "r86400"),
        "limit": per_query,
    }
    if fund.get("title_include"):
        payload["titleInclude"] = list(fund["title_include"])
    if fund.get("title_exclude"):
        payload["titleExclude"] = list(fund["title_exclude"])
    return payload


def apify_item_to_raw(item: Dict[str, Any]) -> Dict[str, Any]:
    """兼容两个 Actor 的输出字段；valig 的 url 已是干净 canonical URL。"""
    url = str(item.get("url") or item.get("link") or "")
    title = str(item.get("title") or "")
    raw_id = str(item.get("id") or "")
    if not raw_id and url:
        found = re.search(r"(\d{6,})", url)
        raw_id = found.group(1) if found else str(abs(hash(url)) % 10**12)
    description = str(item.get("description") or item.get("descriptionText") or "")
    # descriptionHtml 里没有纯文本字段时才降级，用现有 html_to_text 清洗
    if not description and item.get("descriptionHtml"):
        description = html_to_text(str(item["descriptionHtml"]))
    posted = str(item.get("postedDate") or item.get("postedAt") or "")[:10]
    return raw_job(
        source="LinkedIn",                    # 前端卡片显示来源 = LinkedIn
        slug="apify",
        company="LinkedIn",
        company_override=str(item.get("companyName") or ""),  # 显示真实雇主
        raw_id=raw_id,
        title=title,
        url=url,
        apply_url=url,
        department="",
        description=description,
        posted_at=posted,
        updated_at=posted,
        location_candidates=unique([str(item.get("location") or ""), title]),
        career_url="https://www.linkedin.com/jobs/",
    )


def fetch_apify_linkedin(fund: Dict[str, Any], opts: "RunOptions") -> Tuple[List[Dict], str, str]:
    token = os.environ.get("APIFY_API_TOKEN", "").strip()
    if not token:
        return [], "needs_apify", (
            "缺少 APIFY_API_TOKEN：请到 Apify Console > Integrations 生成，"
            "并配置为 GitHub Actions Repository Secret（切勿写进代码）"
        )

    actor_id = fund.get("actor_id") or APIFY_DEFAULT_ACTOR
    endpoint = f"https://api.apify.com/v2/actors/{actor_id}/run-sync-get-dataset-items"
    per_query = int(fund.get("limit_per_query", 50))
    max_items = int(fund.get("max_items_per_run") or per_query)
    max_charge = float(fund.get("max_total_charge_usd", 0.03))
    run_timeout = int(fund.get("timeout_seconds", 240))
    jobs: List[Dict] = []
    problems: List[str] = []

    for query in fund.get("queries") or []:
        actor_input = apify_actor_input(fund, query, actor_id)
        params = urlencode({
            "format": "json",
            "clean": "true",
            "timeout": run_timeout,
            "limit": max_items,             # 最多返回多少条 dataset items
            "maxItems": max_items,          # 最多为多少条结果计费
            "maxTotalChargeUsd": max_charge,
            "restartOnError": "false",
        })
        try:
            raw = _request(
                endpoint + "?" + params,
                opts,
                retries=0,                  # 绝不自动重试付费 run
                method="POST",
                data=json.dumps(actor_input).encode("utf-8"),
                extra_headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout_override=float(run_timeout + 60),
            )
            items = json.loads(raw.decode("utf-8", "replace"))
        except (ScraperError, ValueError, UnicodeDecodeError) as exc:
            problems.append(f"{query.get('keyword')}/{query.get('location')}: {exc}")
            continue
        if isinstance(items, dict):
            items = items.get("items") or items.get("data") or []
        if not isinstance(items, list):
            problems.append(f"{query.get('keyword')}: 响应不是数组")
            continue
        for item in items:
            if isinstance(item, dict) and item.get("title"):
                jobs.append(apify_item_to_raw(item))

    if not jobs and problems:
        raise ScraperError("Apify LinkedIn 全部失败: " + "; ".join(problems[:2]))
    note = (f"Apify {actor_id}: {len(jobs)} 条原始职位 / "
            f"{len(fund.get('queries') or [])} 次 run / 失败 {len(problems)}")
    return jobs, ("ok" if jobs else "empty"), note


SCRAPERS = {
    "greenhouse": fetch_greenhouse,
    "lever": fetch_lever,
    "freshteam": fetch_freshteam,
    "custom": fetch_custom,
    "polymer_api": fetch_polymer_api,
    "eightfold_api": fetch_eightfold_api,
    "phenom_api": fetch_phenom_api,
    "optiver_api": fetch_optiver_api,
    "efc_api": fetch_efc_api,
    "linkedin_guest": fetch_linkedin_guest,
    "apify_linkedin": fetch_apify_linkedin,
}


# ---------------------------------------------------------------------------
# 领域层：地点过滤 / quant 过滤 / 打标签
# ---------------------------------------------------------------------------

def match_locations(candidates: Sequence[str]) -> List[str]:
    text = " " + " ".join(str(c) for c in candidates).lower() + " "
    return [
        name for name, patterns in TARGET_LOCATIONS.items()
        if any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)
    ]


def quant_match(title: str) -> Tuple[int, List[str]]:
    """只匹配职位标题，返回 (命中数, 命中词)。描述不参与，避免误收 HR / 会计岗。"""
    text = " " + (title or "").lower() + " "
    hits: List[str] = []
    for pattern in ROLE_KEYWORDS:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if m and m.group(0).strip() not in hits:
            hits.append(m.group(0).strip())
    if not hits:
        return 0, []

    strong = any(re.search(p, text, flags=re.IGNORECASE) for p in STRONG_QUANT_KEYWORDS)
    if not strong and any(re.search(p, text, flags=re.IGNORECASE) for p in EXCLUDE_TITLE_KEYWORDS):
        return 0, []
    return len(hits), hits


def build_tags(title: str, description: str) -> List[str]:
    # 完整 JD 可能 5k+ 字，技能关键词常在 Requirements 段（后半部分），
    # 不能再只看前 1200 字。
    text = (title + " " + description).lower()
    return [kw for kw in SKILL_KEYWORDS if kw in text][:6]


def finalize_job(
    raw: Dict[str, Any], fund: Dict[str, Any], opts: "RunOptions"
) -> Tuple[Optional[Dict[str, Any]], str]:
    """把内部格式转成对外契约；被过滤时返回原因，便于统计。"""
    # 1) 地点过滤
    found = match_locations(raw["location_candidates"])
    if not found and fund.get("assume_location"):
        found = [fund["assume_location"]]
    selected = [loc for loc in found if loc in opts.locations]
    if not selected:
        return None, "location"

    # 2) Quant 关键词过滤
    score, hits = quant_match(raw["title"])
    if opts.quant_only and score < opts.min_quant_score:
        return None, "quant"

    description = (raw["description"] or "").strip()
    # 安全护栏（正常 JD 不会触顶）。万一触顶，也尽量收在句子边界，
    # 不要在半句话上硬切——用户展开时看到的就是这段文本。
    if len(description) > opts.description_limit:
        cut = description[: opts.description_limit]
        boundary = max(cut.rfind(". "), cut.rfind("。"), cut.rfind("\n"))
        if boundary > opts.description_limit * 0.6:
            cut = cut[: boundary + 1]
        description = cut.rstrip() + "…"

    job_id = f"{raw['source']}:{raw['slug']}:{raw['raw_id']}"
    # 聚合源（LinkedIn / eFC）必须显示真实雇主，否则卡片会全写成 "LinkedIn"
    company = raw.get("company_override") or fund["name"]
    job = {
        "id": job_id,
        "title": raw["title"],
        "company": company,
        # 用未截断的原始 JD 判定：猎头措辞信号常出现在摘要之外。
        "employer_type": classify_employer(company, raw["source"], raw["description"]),
        "location": ", ".join(selected),
        "locations": selected,
        "city_key": "|".join(x.lower().replace(" ", "_") for x in selected),
        "department": raw["department"],
        "description": description,
        "url": raw["url"],
        "apply_url": raw["apply_url"] or raw["url"],
        "career_url": raw["career_url"],
        "source": raw["source"],
        "ats_slug": raw["slug"],
        "posted_at": raw["posted_at"],
        "updated_at": raw["updated_at"],
        "fetched_at": utcnow(),
        "quant_score": score,
        "matched_keywords": hits,
        "tags": build_tags(raw["title"], raw["description"]),
    }
    return job, "ok"


# ---------------------------------------------------------------------------
# 编排层
# ---------------------------------------------------------------------------

def iso_age_hours(value: Any) -> float:
    """ISO 时间戳距今多少小时；解析不了视为已过期。"""
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600.0
    except (ValueError, TypeError):
        return float("inf")


def load_apify_cache() -> List[Dict[str, Any]]:
    """
    读取独立 workflow 每天更新一次的 LinkedIn 缓存。

    缓存文件就是 `main_scraper.py --fund "LinkedIn via Apify" --output <cache>`
    产出的标准 payload，其中 job 已经过 finalize_job() 的地点 + quant 过滤；
    这里只做新鲜度和字段体检，去重统一在 run() 里做。
    这样 4 次/日的免费官方源 workflow 不必重复调用付费 Actor。
    """
    path = PROJECT_ROOT / APIFY_CACHE_FILE
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError) as exc:
        LOG.warning("Apify 缓存不可读，跳过合并: %s", exc)
        return []
    stamp = payload.get("generated_at") or payload.get("fetched_at") or ""
    age = iso_age_hours(stamp)
    if age > APIFY_CACHE_TTL_HOURS:
        LOG.warning("Apify 缓存过期（%.1fh > %dh），跳过合并", age, APIFY_CACHE_TTL_HOURS)
        return []
    out = [
        job for job in (payload.get("jobs") or [])
        if isinstance(job, dict)
        and job.get("source") == "LinkedIn"
        and job.get("id") and job.get("title") and job.get("url")
        and str(job["url"]).startswith("https://")
    ]
    LOG.info("合并 Apify LinkedIn 缓存: %d 条（%.1fh 前生成）", len(out), age)
    return out


@dataclass
class RunOptions:
    output: Path
    timeout: int
    retries: int
    workers: int
    user_agent: str
    with_content: bool
    quant_only: bool
    min_quant_score: int
    description_limit: int
    locations: List[str]
    dry_run: bool = False


def validate_funds(funds: Sequence[Dict[str, Any]]) -> None:
    """配置校验：启动时 fail fast，别等跑到一半才发现 slug 是空的。"""
    errors = []
    for fund in funds:
        name = fund.get("name", "<unnamed>")
        stype = fund.get("scraper_type")
        if stype not in SCRAPERS:
            errors.append(f"{name}: 未知 scraper_type={stype!r}")
        if stype in ("greenhouse", "lever", "freshteam") and not fund.get("ats_slug"):
            errors.append(f"{name}: scraper_type={stype} 必须提供 ats_slug")
        if stype in ("efc_api", "linkedin_guest", "apify_linkedin") and not fund.get("queries"):
            errors.append(f"{name}: scraper_type={stype} 必须提供 queries 列表")
        if stype in ("eightfold_api", "phenom_api") and not fund.get("location_queries"):
            errors.append(f"{name}: scraper_type={stype} 必须提供 location_queries")
        if not fund.get("career_url"):
            errors.append(f"{name}: 缺少 career_url")
    if errors:
        raise SystemExit("配置校验失败:\n  - " + "\n  - ".join(errors))


def collect_fund(fund: Dict[str, Any], opts: RunOptions) -> Tuple[Dict, List[Dict], str, str]:
    """单家基金失败不影响全局；返回 (fund, raw_jobs, status, note)。"""
    try:
        jobs, status, note = SCRAPERS[fund["scraper_type"]](fund, opts)
        LOG.info("[%s] %s -> %s", status.upper(), fund["name"], note)
        return fund, jobs, status, note
    except Exception as exc:  # 单家隔离，绝不静默吞掉
        LOG.warning("[ERROR] %s -> %s: %s", fund["name"], type(exc).__name__, exc)
        return fund, [], "error", f"{type(exc).__name__}: {exc}"


def run(opts: RunOptions, funds: Sequence[Dict[str, Any]], *, merge_cache: bool = True) -> Dict[str, Any]:
    started = time.time()
    fetched_at = utcnow()
    stats: Dict[str, Any] = {
        "funds_configured": len(FUNDS),
        "funds_enabled": len(funds),
        "funds_ok": 0, "funds_empty": 0, "funds_error": 0,
        "funds_needs_playwright": 0,
        "raw_jobs": 0, "dropped_location": 0, "dropped_quant": 0,
        "duplicates_removed": 0, "dedupe_candidates": 0,
        "total_jobs": 0, "apify_cache_jobs": 0, "apify_cache_kept": 0,
        "by_source": {}, "by_location": {}, "by_employer_type": {},
    }
    errors: List[Dict[str, str]] = []
    # 先全量收集、最后统一去重。不能边收边丢：as_completed 的完成顺序是
    # 随机的，「先到先得」会让保留哪条记录变得不可预测，也可能把官方源丢掉。
    candidates: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max(1, opts.workers)) as pool:
        futures = {pool.submit(collect_fund, fund, opts): fund for fund in funds}
        for future in as_completed(futures):
            fund, raws, status, note = future.result()
            if status == "ok":
                stats["funds_ok"] += 1
            elif status == "empty":
                stats["funds_empty"] += 1
            elif status == "needs_playwright":
                stats["funds_needs_playwright"] += 1
            else:
                stats["funds_error"] += 1
                errors.append({"fund": fund["name"], "url": fund.get("career_url", ""), "reason": note})

            for raw in raws:
                stats["raw_jobs"] += 1
                job, reason = finalize_job(raw, fund, opts)
                if job is None:
                    stats["dropped_location" if reason == "location" else "dropped_quant"] += 1
                    continue
                job["fetched_at"] = fetched_at
                candidates.append(job)

    # 付费 Actor 每天只跑 1 次，产出缓存；这里让 4 次/日的免费全量抓取
    # 都带上这份数据，且不会重复触发付费 run。--fund 单源模式不合并，
    # 否则生成缓存时会把旧缓存又叠一遍。缓存同样进入统一去重池。
    if merge_cache:
        for job in load_apify_cache():
            if not set(job.get("locations") or []) & set(opts.locations):
                stats["dropped_location"] += 1
                continue
            if opts.quant_only and quant_match(job.get("title", ""))[0] < opts.min_quant_score:
                stats["dropped_quant"] += 1
                continue
            candidates.append(job)
            stats["apify_cache_jobs"] += 1

    stats["dedupe_candidates"] = len(candidates)
    kept, dedupe_report = deduplicate(candidates)
    stats["duplicates_removed"] = dedupe_report["removed_total"]
    stats["apify_cache_kept"] = sum(1 for j in kept if j.get("source") == "LinkedIn")
    dedupe_report["apify_cache_kept"] = stats["apify_cache_kept"]

    for job in kept:
        stats["by_source"][job["source"]] = stats["by_source"].get(job["source"], 0) + 1
        et = job.get("employer_type", "direct")
        stats["by_employer_type"][et] = stats["by_employer_type"].get(et, 0) + 1
        for loc in job["locations"]:
            stats["by_location"][loc] = stats["by_location"].get(loc, 0) + 1

    stats["total_jobs"] = len(kept)
    stats["elapsed_seconds"] = round(time.time() - started, 2)
    stats["dedupe"] = dedupe_report

    LOG.info(
        "去重: %d 候选 -> %d 保留 (移除 %d) | URL %d / 身份 %d / id %d | "
        "官方优先 %d / 完整度优先 %d",
        dedupe_report["candidates"], dedupe_report["kept"],
        dedupe_report["removed_total"], dedupe_report["by_url"],
        dedupe_report["by_identity"], dedupe_report["by_id"],
        dedupe_report["official_source_preferred"],
        dedupe_report["completeness_preferred"],
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "fetched_at": fetched_at,          # 兼容现有前端
        "generated_at": fetched_at,
        "total_jobs": len(kept),           # 兼容现有前端
        "filters": {
            "locations": opts.locations,
            "quant_only": opts.quant_only,
            "min_quant_score": opts.min_quant_score,
            "keyword_count": len(ROLE_KEYWORDS),
            "legacy_quant_keywords": len(QUANT_KEYWORDS),
        },
        "dedupe": dedupe_report,           # 顶层，方便直接查看 / 门禁读取
        "stats": stats,
        "errors": errors,
        "jobs": kept,
    }


def write_output(payload: Dict[str, Any], path: Path) -> None:
    """原子写：先写 .tmp 再 os.replace，避免前端读到半截 JSON。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {"ts": utcnow(), "level": record.levelname, "msg": record.getMessage(),
             "logger": record.name},
            ensure_ascii=False,
        )


def parse_locations(value: str) -> List[str]:
    aliases = {"hk": "Hong Kong", "hongkong": "Hong Kong", "sg": "Singapore", "sin": "Singapore"}
    wanted = []
    for part in (value or "").split(","):
        key = part.strip()
        if not key:
            continue
        resolved = aliases.get(key.lower()) or next(
            (name for name in TARGET_LOCATIONS if name.lower() == key.lower()), None
        )
        if resolved and resolved not in wanted:
            wanted.append(resolved)
        elif not resolved:
            raise SystemExit(f"未知地点 {key!r}，可选: {', '.join(TARGET_LOCATIONS)}")
    return wanted or list(TARGET_LOCATIONS)


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="HK/SG Quant Jobs 抓取器（纯标准库，零依赖）",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    default_output = PROJECT_ROOT / SETTINGS["output_file"]
    parser.add_argument("--output", default=str(default_output), help="jobs.json 输出路径")
    parser.add_argument("--fund", action="append", default=[],
                        help="只跑指定数据源（名称子串，可重复；显式指定时可跑 enabled=False 的手动源）")
    parser.add_argument("--locations", default=",".join(TARGET_LOCATIONS), help="逗号分隔，如 'Hong Kong' 或 'SG'")
    parser.add_argument("--no-content", action="store_true",
                        help="调试用：不拉 Greenhouse content（会同时丢失 offices 地点字段，可能漏职位）")
    parser.add_argument("--all-jobs", action="store_true", help="关闭 quant 关键词过滤（调试用）")
    parser.add_argument("--min-score", type=int, default=SETTINGS["min_quant_score"], help="最低关键词命中数")
    parser.add_argument("--description-limit", type=int, default=SETTINGS["description_limit"],
                        help="单条描述纯文本上限（安全护栏，默认 12000）")
    parser.add_argument("--workers", type=int, default=SETTINGS["workers"], help="并发基金数")
    parser.add_argument("--timeout", type=int, default=SETTINGS["timeout_seconds"], help="单次请求超时秒数")
    parser.add_argument("--dry-run", action="store_true", help="不写文件，只打印结果预览")
    parser.add_argument("--json-logs", action="store_true", help="结构化 JSON 日志")
    parser.add_argument("--verbose", "-v", action="store_true", help="DEBUG 日志")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter() if args.json_logs else logging.Formatter("%(levelname)-7s %(message)s"))
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, handlers=[handler], force=True)

    funds = [f for f in FUNDS if f.get("enabled", True)]
    if args.fund:
        wanted = [x.lower() for x in args.fund]
        # 显式 --fund 属于人工一次性运行，允许调用 enabled=False 的源
        # （eFC / LinkedIn / Optiver 这类只做手动补数据、不进定时任务）。
        funds = [f for f in FUNDS if any(w in f["name"].lower() for w in wanted)]
        for fund in funds:
            if not fund.get("enabled", True):
                LOG.warning("手动模式启用默认关闭的数据源: %s（切勿用于 CI）", fund["name"])
    if not funds:
        LOG.error("没有可抓取的基金，请检查 --fund 或 enabled 配置")
        return 1
    validate_funds(funds)

    opts = RunOptions(
        output=Path(args.output).resolve(),
        timeout=args.timeout,
        retries=SETTINGS["retries"],
        workers=args.workers,
        user_agent=SETTINGS["user_agent"],
        with_content=SETTINGS["with_content"] and not args.no_content,
        quant_only=SETTINGS["quant_only"] and not args.all_jobs,
        min_quant_score=args.min_score,
        description_limit=args.description_limit,
        locations=parse_locations(args.locations),
        dry_run=args.dry_run,
    )

    LOG.info("开始抓取：%d 家基金 | 地点 %s | quant_only=%s | 并发 %d",
             len(funds), "/".join(opts.locations), opts.quant_only, opts.workers)
    if not opts.with_content:
        LOG.warning("--no-content 模式：Greenhouse 不返回 offices/departments，地点过滤会偏严、结果偏少")
    payload = run(opts, funds, merge_cache=not args.fund)
    stats = payload["stats"]

    dd = payload["dedupe"]
    LOG.info(
        "完成：原始 %d -> 地点剔除 %d / 关键词剔除 %d -> 去重前 %d 条",
        stats["raw_jobs"], stats["dropped_location"], stats["dropped_quant"],
        dd["candidates"],
    )
    LOG.info(
        "去重：%d -> %d 条（移除 %d；URL %d / 身份 %d / id %d；官方优先 %d，完整度优先 %d）",
        dd["candidates"], dd["kept"], dd["removed_total"],
        dd["by_url"], dd["by_identity"], dd["by_id"],
        dd["official_source_preferred"], dd["completeness_preferred"],
    )
    LOG.info("最终 %d 条（%.1fs）", stats["total_jobs"], stats["elapsed_seconds"])
    LOG.info("来源分布: %s", stats["by_source"])
    LOG.info("雇主类型: %s", stats["by_employer_type"])
    LOG.info("地点分布: %s", stats["by_location"])
    LOG.info("API 可用 %d 家 | 需 Playwright %d 家 | 失败 %d 家",
             stats["funds_ok"] + stats["funds_empty"],
             stats["funds_needs_playwright"], stats["funds_error"])

    if args.dry_run:
        for job in payload["jobs"][:10]:
            LOG.info("  %-42s | %-18s | %-20s | %s",
                     job["title"][:42], job["company"][:18], job["location"], job["source"])
        return 0

    write_output(payload, opts.output)
    LOG.info("已写入 %s", opts.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
