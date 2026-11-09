# -*- coding: utf-8 -*-
"""
HK/SG Quant Jobs - 配置与数据契约 (funds_config.py)
====================================================

【项目目标】
个人零成本职位聚合站：只收录香港 / 新加坡地区、量化相关的对冲基金 /
自营交易公司职位，数据优先来自公司官方招聘渠道（含官方 ATS 托管页）。

【数据契约 - Fund 字段】
    name             str   展示名（前端 company 字段）
    career_url       str   官方招聘入口，人工核实过的可访问 URL
    scraper_type     str   greenhouse | lever | freshteam | custom
                          | polymer_api | eightfold_api | phenom_api
                          | optiver_api | efc_api | linkedin_guest
                          | apify_linkedin
    ats_slug         str   公开 ATS board 标识（greenhouse / lever 必填）
    ats_board_url    str   公开职位板地址，供人工核对
    api_url          str   自有 JSON API 端点（*_api / linkedin_guest 可选，代码有默认值）
    location_queries list  eightfold_api / phenom_api 的地区查询词
    queries          list  聚合源的 [{"keyword":..., "location":...}] 查询组合
    max_pages        int   聚合源 / 分页 API 的最大页数
    actor_id         str   apify_linkedin 专用，如 valig~linkedin-jobs-scraper
    limit_per_query  int   apify_linkedin 单次 run 最多抓多少条
    max_items_per_run int  apify_linkedin 单次 run 的计费 / 返回硬上限
    max_total_charge_usd float  单次 run 的金额上限（防跑飞）
    timeout_seconds  int   apify_linkedin 同步 run 超时（< 300，API 上限）
    locations        list  该基金预期覆盖地区，仅作审计说明，不参与过滤
    enabled          bool  False 时不抓取（保留在名单里）
    assume_location  str   custom 抓不到地点时的兜底地点（可选）
    playwright       dict  custom 专用配置，见 PLAYWRIGHT_TEMPLATE
    notes            str   实测结论 / 已知限制 / 合规提示
    verified_at      str   最后一次人工核实日期 (YYYY-MM-DD)

【数据契约 - Job 字段补充】
    employer_type    str   "direct" = 雇主直招 | "agency" = 猎头 / 第三方代招
                          由 classify_employer() 写入，统计见 stats.by_employer_type。
                          判定规则集中在下面第 3b 节（名单 / 关键词 / JD 措辞），
                          改名单即可纠偏，不必改抓取器或前端。

【数据源分级】
    A 官方公开 JSON（每日 4 次自动更新）：greenhouse / lever / polymer_api /
      eightfold_api / phenom_api
    B 聚合站公开 JSON：efc_api 已启用（随 scrape.yml 每日 4 次更新）。
      q+location 过滤准确，保留 crawl-delay 10s / retries=0 / 单源失败隔离；
      主站 robots.txt 对 /search、/v1 有 Disallow，若出现 403/429 请改回 False。
      其余默认关闭：optiver_api（数据不全）、linkedin_guest（robots / ToS 风险，
      只允许人工低频一次性运行）
    C 需 Playwright 或暂不可行：custom（Citadel / Two Sigma / Balyasny 等）
    D 付费按需（Apify）：apify_linkedin，每天最多 1 次、走独立 workflow，
      结果先落 data/linkedin_apify_cache.json 再由全量抓取合并进 jobs.json；
      免费额度耗尽 / 缺 token 时自动 fail-open，不影响 A 类官方源。

【验收标准】
    1. python scrapers/main_scraper.py 无需 pip install 即可产出 data/jobs.json
    2. 每条职位必须命中 TARGET_LOCATIONS 之一且命中 QUANT 关键词
    3. 单家基金失败不影响其它基金，失败原因写入 jobs.json 的 errors
    4. 自动剔除实习岗（EXCLUDE_LEVEL_KEYWORDS，标题 + JD 全文）；
       Graduate / Campus 岗位刻意保留、由人工判断，避免误删正职岗

【核实日期】2026-11-06（所有 URL 均实际发起过 HTTP 请求验证）
"""

VERIFIED_AT = "2026-11-06"

# ---------------------------------------------------------------------------
# 1. 运行参数（集中配置，爬虫启动时校验，不散落在代码里）
# ---------------------------------------------------------------------------
SETTINGS = {
    "output_file": "data/jobs.json",
    "timeout_seconds": 20,
    "retries": 2,                 # 仅对 5xx / 429 / 网络错误重试
    "workers": 6,                 # 并发基金数，别打太猛
    "user_agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    # 必须保持 True：实测 Greenhouse 仅在 ?content=true 时才返回 offices /
    # departments 字段，关掉会丢失地点信息（本次实测少收 11 个 HK/SG 职位）。
    # 想提速请调小 workers 或减少基金数，不要用 --no-content 当常规模式。
    "with_content": True,         # Greenhouse 附带描述 + offices + departments
    "quant_only": True,
    "min_quant_score": 1,
    # 前端卡片展开要显示接近原文的 JD，所以这里只是防异常数据的安全护栏，
    # 不是摘要长度。实测完整 JD：eFC ~5k、Greenhouse ~2-8k、SIG ~7k 字符。
    # 600 字会把几乎所有官方 JD 砍在半句话上，不要再调回去。
    "description_limit": 12000,   # 单条描述纯文本上限（正常职位不会触顶）
}

# ---------------------------------------------------------------------------
# 2. 地点过滤：别名用正则词边界匹配，避免 "sg" 命中 "design"
# ---------------------------------------------------------------------------
TARGET_LOCATIONS = {
    "Hong Kong": [
        r"\bhong\s*kong\b", r"\bhk\b", r"\bhksar\b", r"\bkowloon\b",
        r"\bcauseway\s*bay\b", r"\bcentral,?\s*hk\b",
    ],
    "Singapore": [
        r"\bsingapore\b", r"\bsg\b", r"\bshenton\s*way\b",
        r"\bmarina\s*bay\b", r"\braffles\s*place\b",
    ],
}

# ---------------------------------------------------------------------------
# 3. Quant 关键词过滤
#    判定只看「职位标题」：职位描述里几乎都会出现 python / trading / research，
#    拿描述做收录判断会把 HR、行政、会计岗一并收进来（实测 Jane Street 的
#    Accounting Coordinator 描述里就有这些词）。描述只用于生成 tags。
# ---------------------------------------------------------------------------
ROLE_KEYWORDS = [
    # --- 强信号：量化 / 投研本体 ---
    r"\bquant(?:itative|s)?\b", r"\bqr\b", r"\bresearcher\b", r"\bresearch\b",
    r"\btrader\b", r"\btrading\b", r"\bsystematic\b", r"\balpha\b",
    r"\bportfolio\s*manager\b", r"\bstrategist\b", r"\bstrategies\b",
    r"\bstatistical\s*arbitrage\b", r"\bstat\s*arb\b",
    r"\bhft\b", r"\bhigh[\s-]frequency\b", r"\blow[\s-]latency\b",
    r"\bkdb(?:\+|\s*/\s*q)?\b", r"\bmarket\s*making\b", r"\bexecution\b",
    r"\bpricing\b", r"\bderivatives\b", r"\bdelta\s*one\b",
    r"\bdata\s*scientist\b", r"\bdata\s*science\b",
    r"\bmachine\s*learning\b", r"\bapplied\s*ml\b", r"\bml\s*engineer\b",
    r"\bai\s*research\b", r"\bmodel(?:l)?ing\b", r"\boptimization\b",
    # --- 技术信号：对冲基金里的工程岗算 quant-adjacent ---
    r"\bsoftware\s*engineer\b", r"\bdeveloper\b", r"\bengineer(?:ing)?\b",
    r"\bdata\s*engineer\b", r"\bplatform\s*engineer\b", r"\bdevops\b",
    r"\bsre\b", r"\breliability\s*engineer\b",
    r"\bc\+\+\b", r"\bpython\b", r"\brust\b", r"\bgolang\b", r"\bjava\b",
    r"\bfpga\b", r"\bnetwork\s*engineer\b",
]

# 命中这些即无视 EXCLUDE_TITLE_KEYWORDS（例如 "Quantitative Recruiter" 仍收录）
STRONG_QUANT_KEYWORDS = [
    r"\bquant(?:itative)?\b", r"\bqr\b", r"\btrader\b", r"\btrading\b",
    r"\bsystematic\b", r"\balpha\b", r"\bstatistical\s*arbitrage\b",
    r"\bhft\b", r"\bkdb\b", r"\bportfolio\s*manager\b",
]

# 明显非技术岗：仅在未命中强信号时用于剔除
EXCLUDE_TITLE_KEYWORDS = [
    r"\brecruit(?:er|ing|ment)\b", r"\btalent\s*acquisition\b",
    r"\bhuman\s*resources\b", r"\bhr\b", r"\bexecutive\s*assistant\b",
    r"\boffice\s*manager\b", r"\bbarista\b", r"\baccount(?:ing|ant)\b",
    r"\bpayroll\b", r"\blegal\s*counsel\b", r"\bcompliance\s*officer\b",
    r"\bmarketing\b", r"\bcommunications\b", r"\bsales\b",
    r"\bbusiness\s*development\b", r"\bfacilities\b", r"\breceptionist\b",
    r"\bux\s*researcher\b", r"\bproduct\s*designer\b", r"\bgraphic\b",
    r"\bcontent\s*writer\b", r"\bevent\s*coordinator\b",
    r"\bav\s*engineer\b", r"\bcybersecurity\b", r"\bhelpdesk\b",
    r"\bdesktop\s*support\b", r"\bit\s*support\b",
]

# ---------------------------------------------------------------------------
# 3a. 实习职位排除层（与 quant 收录规则正交）
#     只自动剔除「实习岗」。Graduate / Campus 等应届 / 校招岗刻意留在数据里，
#     由人工在页面上判断：正职 JD 常把 graduate 当学历要求写，例如
#     "Graduate degree (Master or above)"、"Graduate & Postgraduate study"、
#     "talented graduates will be considered"、"fresh graduates or up to 2
#     years' experience"。见词就删会误删有经验岗位（2026-11-09 复核误删 6 条）。
#
#     在 finalize_job() 里、quant 过滤之后执行：即使标题同时命中 quant 和
#     intern 也照样剔除。标题 + 原始 JD 全文一起判定，大小写不敏感 ——
#     不少机构把实习岗标题写成 "Software Engineer"，只在正文里说
#     "as an intern ... during the internship"。
#
#     必须用词边界（\b）：裸子串 intern 会命中 international / internal
#     （实测 412 处），这些并不是实习岗。改这份名单即可调整口径，
#     不必改抓取器或前端。
# ---------------------------------------------------------------------------
EXCLUDE_LEVEL_KEYWORDS = [
    r"\bintern\b", r"\binterns\b",
    r"\binternship\b", r"\binternships\b",
]

# 仅用于生成展示标签，不参与收录判定
SKILL_KEYWORDS = [
    "python", "c++", "rust", "java", "kdb", "q/kdb", "sql", "pytorch",
    "tensorflow", "machine learning", "deep learning", "llm", "cuda",
    "linux", "fpga", "low latency", "time series", "stochastic", "monte carlo",
    "options", "futures", "equities", "fx", "crypto", "backtesting",
]

# 兼容旧的 `from funds_config import QUANT_KEYWORDS` 用法（纯字符串版）
QUANT_KEYWORDS = [
    "quant", "quantitative", "researcher", "research", "trader", "trading",
    "systematic", "alpha", "portfolio manager", "strategist",
    "data scientist", "machine learning", "software engineer", "developer",
    "engineer", "c++", "python", "kdb", "hft", "low latency", "execution",
]

# ---------------------------------------------------------------------------
# 3b. 雇主类型：直招 vs 猎头（唯一事实来源）
#     eFinancialCareers 这类聚合站里，company 字段是「发布广告的公司」，
#     不一定是真正的雇主。抓到职位后由 main_scraper.classify_employer()
#     为每条写入 employer_type = "direct" | "agency"，前端据此显示 Agency
#     徽章并提供筛选。这里只放配置，判定逻辑在 scrapers/main_scraper.py。
#
#     维护方式：发现漏标 / 误标的公司，直接改下面两个名单即可，无需改代码。
#     名单里的名字按原始公司名写，比对时会统一做 normalize_company()
#     归一化（小写、去标点、去 Ltd / LP / Group 等法律后缀）。
# ---------------------------------------------------------------------------

# 已知第三方招聘 / 猎头公司。判定优先级最高。
RECRUITER_AGENCIES = [
    # 公司名本身即标明招聘业务
    "Gravitas Recruitment Group",
    "SilverStrand Executive Search Limited",
    "Michael Page International Singapore",
    "TEKsystems (Allegis Group Singapore Pte Ltd)",
    # 量化 / 金融垂直猎头
    "BAH Partners",
    "Oxford Knight",
    "Pinpoint Asia",
    "Sanderson-iKas",
    "Selby Jennings",
    "Larson Maddox",
    "Webbe International",
    "Aptitude Asia",
    "Alan Partners",
    "Westbury Partners",       # boutique executive search, westbury-partners.com
    "Principle Partners",      # buy-side executive search, principle-partners.com
    "Eka Finance",             # ekafinance.com: "global quantitative search"
    "NLS",
    "Tribus",
    # 复核项：JD 全程用第三人称描述未具名雇主（"A global systematic hedge
    # fund is hiring..."），且与 Investor Relations 这类岗位混在一起发布，
    # 判定为代招。若日后确认它其实是直雇机构，直接删掉这一行即可。
    "DARMAX GLOBAL",
]

# 新出现的猎头若公司名带这些词，即使不在名单里也会被标记。
# 刻意保持窄口径：不含 "partners" / "consulting" / "search" 单词，
# 避免把 PDT Partners、Jane Street 这类直雇机构误判成猎头。
RECRUITER_NAME_PATTERNS = [
    r"\brecruit(?:ment|er|ers|ing)?\b",
    r"\bstaffing\b",
    r"\bheadhunt(?:er|ers|ing)?\b",
    r"\bexecutive\s+search\b",
    r"\btalent\s+(?:solutions|acquisition)\b",
    r"\bemployment\s+agency\b",
]

# 兜底保护：这些公司即使命中上面的关键词也一律算直招。
DIRECT_EMPLOYER_OVERRIDES = [
    "Balyasny Asset Management LP",
    "GIC Private Limited",
    "CITIC Securities International Company Limited",
    "United Overseas Bank",
    "AIA Singapore Private Limited",
]

# JD 措辞信号（只在聚合站来源上生效，避免银行 describing their own clients
# 时被误判）。实测在现有数据里 8 家公司命中，0 家直雇机构误伤。
AGENCY_DESCRIPTION_SIGNALS = [
    r"\bour\s+client\b",
    r"\bon\s+behalf\s+of\s+(?:a|an|our|the)\b",
    r"\bwe(?:'re|\s+are)\s+(?:working\s+with|partnered\s+with)\s+(?:a|an|our)\b",
]
AGENCY_SIGNAL_SOURCES = {"eFC", "LinkedIn", "linkedin_guest"}

# ---------------------------------------------------------------------------
# 4. 基金名单
#    所有 greenhouse / lever 条目的 ats_slug 均于 2026-11-06 实测返回 200
#    且有真实职位数据；*_api 条目均为本轮逆向官网得到的公开 JSON 端点；
#    custom 条目为需 Playwright / 浏览器渲染或暂不可行的官网。
#    enabled=False 的条目不会进入 GitHub Actions 定时更新。
# ---------------------------------------------------------------------------

# Greenhouse 官方职位板 URL 生成器（每个都已验证 200）
def _gh_board(slug: str) -> str:
    return f"https://job-boards.greenhouse.io/{slug}"


FUNDS = [
    # ===================== Greenhouse 公开 API（实测可用） =====================
    {
        "name": "Point72",
        "career_url": "https://careers.point72.com/",
        "scraper_type": "greenhouse",
        "ats_slug": "point72",
        "ats_board_url": "https://boards.greenhouse.io/point72",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "官网 careers 页含 Greenhouse 组件；实测 197 岗，HK 20 / SG 10。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Schonfeld Strategic Advisors",
        "career_url": "https://www.schonfeld.com/careers/",
        "scraper_type": "greenhouse",
        "ats_slug": "schonfeld",
        "ats_board_url": _gh_board("schonfeld"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "官网 careers 页直接嵌 Greenhouse；实测 67 岗，HK 13 / SG 4。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "ExodusPoint Capital Management",
        "career_url": _gh_board("exoduspoint"),
        "scraper_type": "greenhouse",
        "ats_slug": "exoduspoint",
        "ats_board_url": _gh_board("exoduspoint"),
        "locations": ["Hong Kong"],
        "enabled": True,
        "notes": "官网 /careers 返回 404，官方职位板托管在 Greenhouse；实测 3 岗，含 HK 1。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Jane Street",
        "career_url": "https://www.janestreet.com/join-jane-street/",
        "scraper_type": "greenhouse",
        "ats_slug": "janestreet",
        "ats_board_url": "https://www.janestreet.com/join-jane-street/apply/",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "absolute_url 指向公司自有域名，但后端是 Greenhouse；实测 219 岗，HK 55 / SG 14。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "IMC Trading",
        "career_url": _gh_board("imc"),
        "scraper_type": "greenhouse",
        "ats_slug": "imc",
        "ats_board_url": "https://job-boards.eu.greenhouse.io/imc",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "欧洲区使用 eu.greenhouse.io；实测 162 岗，HK 17。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Tower Research Capital",
        "career_url": _gh_board("towerresearchcapital"),
        "scraper_type": "greenhouse",
        "ats_slug": "towerresearchcapital",
        "ats_board_url": _gh_board("towerresearchcapital"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "职位链接回跳 tower-research.com；实测 91 岗，HK 14 / SG 21。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Jump Trading",
        "career_url": _gh_board("jumptrading"),
        "scraper_type": "greenhouse",
        "ats_slug": "jumptrading",
        "ats_board_url": _gh_board("jumptrading"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 92 岗，HK 21 / SG 15。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Squarepoint Capital",
        "career_url": _gh_board("squarepointcapital"),
        "scraper_type": "greenhouse",
        "ats_slug": "squarepointcapital",
        "ats_board_url": _gh_board("squarepointcapital"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 90 岗，HK 16 / SG 46，是新加坡岗位最密的来源之一。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "PDT Partners",
        "career_url": _gh_board("pdtpartners"),
        "scraper_type": "greenhouse",
        "ats_slug": "pdtpartners",
        "ats_board_url": _gh_board("pdtpartners"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 12 岗，当前无 HK/SG 岗；保留以便开岗自动收录。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Winton Group",
        "career_url": _gh_board("winton"),
        "scraper_type": "greenhouse",
        "ats_slug": "winton",
        "ats_board_url": "https://job-boards.eu.greenhouse.io/winton",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 7 岗，当前无 HK/SG 岗。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "AQR Capital Management",
        "career_url": "https://careers.aqr.com/",
        "scraper_type": "greenhouse",
        "ats_slug": "aqr",
        "ats_board_url": _gh_board("aqr"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "自有域名 careers.aqr.com 由 Greenhouse 驱动；实测 55 岗，当前无 HK/SG 岗。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Akuna Capital",
        "career_url": _gh_board("akunacapital"),
        "scraper_type": "greenhouse",
        "ats_slug": "akunacapital",
        "ats_board_url": _gh_board("akunacapital"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 32 岗，SG 1。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Graviton Research Capital",
        "career_url": _gh_board("gravitonresearchcapital"),
        "scraper_type": "greenhouse",
        "ats_slug": "gravitonresearchcapital",
        "ats_board_url": _gh_board("gravitonresearchcapital"),
        "locations": ["Singapore"],
        "enabled": True,
        "notes": "新加坡高频量化；实测 21 岗，SG 9。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Quadrature Capital",
        "career_url": _gh_board("quadraturecapital"),
        "scraper_type": "greenhouse",
        "ats_slug": "quadraturecapital",
        "ats_board_url": _gh_board("quadraturecapital"),
        "locations": ["Singapore"],
        "enabled": True,
        "notes": "实测 3 岗，SG 1。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "DV Trading",
        "career_url": _gh_board("dvtrading"),
        "scraper_type": "greenhouse",
        "ats_slug": "dvtrading",
        "ats_board_url": _gh_board("dvtrading"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "自营交易；实测 73 岗，HK 12 / SG 5。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Flow Traders",
        "career_url": _gh_board("flowtraders"),
        "scraper_type": "greenhouse",
        "ats_slug": "flowtraders",
        "ats_board_url": _gh_board("flowtraders"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "做市商；实测 43 岗，HK 11。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Virtu Financial",
        "career_url": _gh_board("virtu"),
        "scraper_type": "greenhouse",
        "ats_slug": "virtu",
        "ats_board_url": _gh_board("virtu"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 48 岗，HK 1 / SG 13。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Jump Crypto",
        "career_url": _gh_board("jumpcrypto"),
        "scraper_type": "greenhouse",
        "ats_slug": "jumpcrypto",
        "ats_board_url": _gh_board("jumpcrypto"),
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "实测 2 岗，当前无 HK/SG 岗。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "WorldQuant",
        "career_url": _gh_board("worldquant"),
        "scraper_type": "greenhouse",
        "ats_slug": "worldquant",
        "ats_board_url": "https://www.worldquant.com/careers/",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮新增。官方 Greenhouse board 实测 87 岗、12 个 HK/SG 岗（BRAIN Researcher / Quant Developer / Portfolio Manager 等）。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "XTX Markets",
        "career_url": "https://job-boards.greenhouse.io/xtxmarketstechnologies",
        "scraper_type": "greenhouse",
        "ats_slug": "xtxmarketstechnologies",
        "ats_board_url": "https://job-boards.greenhouse.io/xtxmarketstechnologies",
        "locations": ["Singapore"],
        "enabled": True,
        "notes": "本轮新增。官网 careers 页明确指向该 Greenhouse board；实测 7 岗，其中 2 个 Singapore, Central。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Man Group",
        "career_url": "https://job-boards.eu.greenhouse.io/mangroup",
        "scraper_type": "greenhouse",
        "ats_slug": "mangroup",
        "ats_board_url": "https://www.man.com/careers",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮新增。官网 careers 用 eu.greenhouse.io；实测 71 岗，地点全在 London / Sofia / NY / Shanghai，当前无 HK/SG 岗，保留以便开岗自动收录。",
        "verified_at": VERIFIED_AT,
    },

    # ===================== Lever 公开 API（实测可用） =====================
    {
        "name": "Amber Group",
        "career_url": "https://jobs.lever.co/ambergroup",
        "scraper_type": "lever",
        "ats_slug": "ambergroup",
        "ats_board_url": "https://jobs.lever.co/ambergroup",
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "数字资产做市 / 自营交易，含 Quant Intern、Quant Researcher；实测 3 岗，HK 2 / SG 1。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Nium (Lever 分支验证样例)",
        "career_url": "https://jobs.lever.co/nium",
        "scraper_type": "lever",
        "ats_slug": "nium",
        "ats_board_url": "https://jobs.lever.co/nium",
        "locations": ["Singapore"],
        "enabled": False,
        "notes": "支付公司而非对冲基金，默认关闭；仅用于验证 Lever 解析分支仍有第二个真实数据源。",
        "verified_at": VERIFIED_AT,
    },

    # ============ 官网自有公开 JSON API（本轮新增，零依赖，可进自动更新） ============
    {
        "name": "Polymer Capital Management",
        "career_url": "https://careers.polymercapital.com/",
        "scraper_type": "polymer_api",
        "api_url": "https://careers-api.polymerpraxis.com/Jobs",
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮从 custom 升级。前端 bundle 中硬编码 POST /Jobs（GET 405）；实测 33 岗、11 个 HK/SG 岗，无反爬。申请链接用 Workable entityName+shortcode 拼接，实测 200。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Millennium Management",
        "career_url": "https://career.mlp.com/careers",
        "scraper_type": "eightfold_api",
        "api_url": "https://career.mlp.com/api/apply/v2/jobs",
        "domain": "mlp.com",
        "location_queries": ["Hong Kong", "Singapore"],
        "max_pages": 8,
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮从 custom 升级。Eightfold 公开 JSON：num 参数无效但 start 分页有效；location=Hong Kong 返回 count=35、Singapore count=16，逐页可覆盖全部 HK/SG 岗。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Susquehanna International Group (SIG)",
        "career_url": "https://careers.sig.com/jobs",
        "scraper_type": "phenom_api",
        "api_url": "https://careers.sig.com/api/jobs",
        "ats_slug": "sig",
        "location_queries": ["Hong Kong", "Singapore"],
        "page_size": 100,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮新增。Phenom 招聘站公开 /api/jobs?location=...&limit=100；实测 HK 33 岗（Quant Research / Pricing Developer 等），SG 当前 0 岗。字段含 apply_url 与 posted_date。",
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Optiver (incomplete API)",
        "career_url": "https://www.optiver.com/join-us/jobs/",
        "scraper_type": "optiver_api",
        "api_url": "https://www.optiver.com/en/api/v1/jobs",
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": False,
        "notes": "技术可行但数据不全：totalCount=160 只返回前 16 条，page/limit/offset/office/pageNumber/skip 全部被忽略。仅 1 个 Singapore 岗可见，故默认关闭；待找到真实分页参数或改用 Playwright 再开启。",
        "verified_at": VERIFIED_AT,
    },

    # ========== 聚合类数据源 ==========
    # eFC 已正式启用并纳入 scrape.yml 日常自动更新（2026-11-06 起）。
    # 仍保留保守请求策略；linkedin_guest 依旧默认关闭、勿进 CI。
    {
        "name": "eFinancialCareers (HK/SG)",
        "career_url": "https://www.efinancialcareers.com/",
        "scraper_type": "efc_api",
        "api_url": "https://job-search-ui.efinancialcareers.com/v1/efc/jobs/search",
        "site_url": "https://www.efinancialcareers.com",
        "queries": [
            {"keyword": "quant", "location": "Hong Kong"},
            {"keyword": "quant", "location": "Singapore"},
            {"keyword": "quantitative researcher", "location": "Hong Kong"},
            {"keyword": "quantitative researcher", "location": "Singapore"},
        ],
        "max_pages": 2,
        "page_size": 20,
        "delay_seconds": 10.0,      # 主站 robots.txt: crawl-delay: 10，务必保留
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,            # 已按需求正式启用，随 scrape.yml 每日 4 次更新
        "notes": (
            "已启用并纳入日常自动更新。Angular SSR 的 ng-state 暴露 job-search-ui JSON，"
            "q+location 过滤准确（实测 HK 295 / SG 200 条）；queries 只保留 quant / "
            "quantitative researcher 两类关键词，最终仍由 finalize_job() 做地点 + quant "
            "二次过滤、canonical_url 去重。缓解措施：delay_seconds=10（遵守 crawl-delay）、"
            "retries=0、单源失败隔离，eFC 挂掉不影响其余官方源。"
            "风险：主站 robots.txt Disallow /search、/v1，User-agent: GPTBot Disallow /，"
            "且含银行 / 猎头贴；若出现 403/429 或收到合规要求，把 enabled 改回 False 即可。"
        ),
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "LinkedIn Public Jobs (HK/SG)",
        "career_url": "https://www.linkedin.com/jobs/",
        "scraper_type": "linkedin_guest",
        "api_url": "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search",
        "queries": [
            {"keyword": "quant", "location": "Hong Kong"},
            {"keyword": "quant", "location": "Singapore"},
            {"keyword": "quantitative researcher", "location": "Hong Kong"},
            {"keyword": "quantitative researcher", "location": "Singapore"},
        ],
        "max_pages": 3,
        "delay_seconds": 1.5,
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": False,
        "notes": "技术可行：guest 端点无需登录 / JS，每页 10 张 HTML 卡片，实测连续 6 页均 200，含真实公司名。但 robots.txt 明确 Disallow: /jobs-guest/，且违反 LinkedIn ToS，随时可能 999 / IP 封禁。仅允许人工一次性补数据，严禁加入定时更新。",
        "verified_at": VERIFIED_AT,
    },

    # ========== 付费按需源（Apify；与 4 次/日免费 workflow 完全隔离） ==========
    # 每日最多 1 次，由 .github/workflows/linkedin-apify.yml 触发；
    # 输出 data/linkedin_apify_cache.json，再由全量抓取合并进 data/jobs.json。
    # 需要环境变量 APIFY_API_TOKEN（GitHub Actions Secret），本地不配置则跳过。
    {
        "name": "LinkedIn via Apify (HK/SG)",
        "career_url": "https://www.linkedin.com/jobs/",
        "scraper_type": "apify_linkedin",
        # Free 套餐：valig $0.40/1000 results + $0.001/start；
        # curious_coder Free 是 $2.00/1000（页面标的 $1.00 是 Starter 价），
        # 同样 $5 额度前者约 12,500 条、后者仅约 2,500 条。
        # 想换 Actor 只改这一行即可，adapter 会自动切换 input 字段。
        "actor_id": "valig~linkedin-jobs-scraper",
        "queries": [
            {"keyword": "quant", "location": "Hong Kong"},
            {"keyword": "quant", "location": "Singapore"},
            {"keyword": "quantitative researcher", "location": "Hong Kong"},
            {"keyword": "quantitative researcher", "location": "Singapore"},
        ],
        "limit_per_query": 50,        # 4 次 run × 上限 50 = 每日最多 200 条原始结果
        "date_posted": "r86400",      # valig：过去 24 小时（r604800=周 / r2592000=月）
        "cc_date_posted": "past24Hours",  # 换 curious_coder 时用的同义值
        "title_include": [],          # 留空：交给项目自己的 quant_match() 判定，避免漏岗
        "title_exclude": ["Recruiter", "Talent Acquisition", "Account Manager", "Sales"],
        "max_items_per_run": 50,      # API limit（返回）+ maxItems（计费）双上限
        "max_total_charge_usd": 0.03, # 单次 run 金额熔断；4 次 run 最坏 $0.12/天
        "timeout_seconds": 240,       # 同步端点 300s 上限，留 60s 给 HTTP 往返
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": False,
        "notes": (
            "Apify Store Actor，按 dataset 结果计费，无需 LinkedIn 登录 / cookie。"
            "选 valig：Free $0.40/1000 + $0.001/start，输出 url 为干净 canonical 链接"
            "（curious_coder 的 link 带 refId/trackingId/position，需 canonical_url 剥离）；"
            "原生支持 location + datePosted + titleInclude/titleExclude + limit。"
            "按 4 次 run × 50 条 ≈ 200 条/日估算约 $0.084/日、$2.5/月，Free $5 够用。"
            "风险：额度耗尽返回 402、LinkedIn AI Search 改版、第三方 Actor 维护中断、"
            "猎头/银行岗混入（由 finalize_job 的地点+quant 过滤兜底）。"
            "严禁并入 scrape.yml 的 4 次/日定时任务；只用 linkedin-apify.yml 手动 / 每日 1 次。"
        ),
        "verified_at": VERIFIED_AT,
    },

    # ===================== custom：需要 Playwright / 浏览器渲染 =====================
    {
        "name": "Citadel",
        "career_url": "https://www.citadel.com/careers/",
        "scraper_type": "custom",
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮复核：普通 GET 403；headless Chrome 等待 15s 仍停在 Cloudflare 'Just a moment...'，职位标记为 0。无公开 ATS（Greenhouse/Lever/SmartRecruiters/Ashby 均无有效 board）。普通 Playwright 不能稳定通过，不建议投入；改用官方职位提醒或人工补录。",
        "playwright": {"enabled": False, "job_link_selector": "a[href*='job'], a[href*='position'], a[href*='careers']", "extra_wait_ms": 3000},
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Citadel Securities",
        "career_url": "https://www.citadelsecurities.com/careers/",
        "scraper_type": "custom",
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮复核：与 Citadel 同族，headless Chrome 同样停在 Cloudflare 挑战页；无公开 ATS API。结论同 Citadel。",
        "playwright": {"enabled": False, "job_link_selector": "a[href*='job'], a[href*='position']", "extra_wait_ms": 3000},
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Two Sigma",
        "career_url": "https://careers.twosigma.com/careers",
        "scraper_type": "custom",
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮复核：Avature 表单驱动（jobSort / listFilterMode / search 等 hidden input），SearchJobsData/ 各种 GET 只返回 266-279 字节空壳，/careers/OpenRoles 的 GET 与 POST 均返回同一 109653 字节外壳。可行方案：Playwright 填表提交后等结果表格；或抓页面 JS 逆向 POST 参数。",
        "playwright": {"enabled": False, "wait_until": "networkidle", "wait_for_selector": "a[href*='Job']", "job_link_selector": "a[href*='Job'], a[href*='job']", "extra_wait_ms": 2500},
        "verified_at": VERIFIED_AT,
    },
    {
        "name": "Balyasny Asset Management",
        "career_url": "https://www.bamfunds.com/careers",
        "scraper_type": "custom",
        "ats_slug": None,
        "locations": ["Hong Kong", "Singapore"],
        "enabled": True,
        "notes": "本轮复核：Next.js App Router 营销页（123680 字节），无 __NEXT_DATA__、无 JSON-LD、JobPosting 出现 0 次，headless Chrome 也不渲染职位列表。唯一外链是 Salesforce 门户 https://bambusdev.my.site.com/s/，未发现公开职位 JSON。建议人工补录或订阅职位提醒。",
        "playwright": {"enabled": False, "job_link_selector": "a[href*='career'], a[href*='job'], a[href*='position']", "extra_wait_ms": 2000},
        "verified_at": VERIFIED_AT,
    },
]

# ---------------------------------------------------------------------------
# 5. Playwright 模板（custom 页面接入方式）
#    把某条 custom 基金的 playwright.enabled 改成 True 并填好选择器即可生效；
#    无需改 main_scraper.py。首次使用需：
#        pip install playwright && playwright install chromium
# ---------------------------------------------------------------------------
PLAYWRIGHT_TEMPLATE = {
    "enabled": False,             # 默认关闭，保证零依赖可直接跑
    "url": "",                    # 留空则用 career_url
    "wait_until": "domcontentloaded",
    "wait_for_selector": "",      # 等职位列表渲染完成，例如 "a[href*='job']"
    "job_link_selector": "a[href*='job'], a[href*='position']",
    "extra_wait_ms": 2000,
    "max_jobs": 100,
}

# Freshteam 目前没有可用的目标基金，保留端点探测清单以便后续接入。
# 新增示例：
#   {
#       "name": "Some Fund",
#       "career_url": "https://somefund.freshteam.com/",
#       "scraper_type": "freshteam",
#       "ats_slug": "somefund",
#       "enabled": True,
#   }
FRESHTEAM_ENDPOINTS = [
    "/api/jobs",
    "/api/v1/jobs",
    "/jobs.json",
    "/api/public/jobs",
]

# ---------------------------------------------------------------------------
# 6. 本轮「其他重要对冲基金」探测审计（2026-11-06，均未纳入 FUNDS）
#    结论口径：官网可访问 + 有没有公开 JSON + 当前有没有 HK/SG 岗。
# ---------------------------------------------------------------------------
#   Hudson River Trading      403（Akamai 类风控），未发现公开 ATS
#   G-Research                WordPress 站点，无职位 JSON
#   DRW / D. E. Shaw          自研招聘页，无公开 JSON API
#   Quantedge / Trexquant     官网静态页，无职位 JSON
#   Qube Research / Capula    连接失败 / 502，未拿到职位数据
#   BlueCrest                 SmartRecruiters board 存在但只有 1 个美国装机岗
#   Marshall Wace             Greenhouse board 存在但 0 岗
#   Walleye / Optiver(GH)     Greenhouse board 存在但 0 岗
#   Systematica               PinpointHQ /jobs.json 可用，但 4 岗全在 London
#   Man Group                 已纳入（Greenhouse 71 岗，当前无 HK/SG，保留自动收录）
#   Citadel / CitadelSec      Cloudflare 挑战，headless Chrome 也过不去
#   Two Sigma / Balyasny      见上方 custom 条目备注
#
#   想继续扩充时优先顺序：先找官网 careers 页 → grep ATS 域名 → 试公开 JSON；
#   只有同时满足「官方 + 公开 JSON + HK/SG 有岗」才把 enabled 改成 True。
