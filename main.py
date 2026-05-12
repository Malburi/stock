from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import asyncio
import time
import pandas as pd
import requests
import xml.etree.ElementTree as ET
from urllib.parse import quote
import os, json, hashlib

_stock_db: list[dict] = []

# ── VAPID 키 관리 ─────────────────────────────────────────────────
VAPID_KEYS_FILE = ".vapid_keys"

def _load_or_create_vapid_keys() -> tuple[str, str]:
    """Returns (public_key_urlsafe_b64, private_key_pem)"""
    import base64 as _b64
    pub = os.environ.get("VAPID_PUBLIC_KEY")
    priv_env = os.environ.get("VAPID_PRIVATE_KEY")
    if pub and priv_env:
        # env var에 \n 리터럴이 있을 경우 실제 줄바꿈으로 변환
        priv = priv_env.replace("\\n", "\n")
        return pub, priv
    if os.path.exists(VAPID_KEYS_FILE):
        with open(VAPID_KEYS_FILE) as f:
            d = json.load(f)
            return d["public"], d["private"]
    from py_vapid import Vapid
    from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
    v = Vapid()
    v.generate_keys()
    pub_raw = v._public_key.public_bytes(Encoding.X962, PublicFormat.UncompressedPoint)
    pub = _b64.urlsafe_b64encode(pub_raw).rstrip(b"=").decode()
    priv = v.private_pem().decode()
    # env var용: 줄바꿈을 \n 리터럴로 변환
    priv_escaped = priv.replace("\n", "\\n")
    with open(VAPID_KEYS_FILE, "w") as f:
        json.dump({"public": pub, "private": priv}, f)
    print("[VAPID] 새 키 생성됨. Render 환경변수에 아래 두 값을 등록하세요:")
    print(f"  VAPID_PUBLIC_KEY = {pub}")
    print(f"  VAPID_PRIVATE_KEY = {priv_escaped}")
    return pub, priv

_vapid_public: str = ""
_vapid_private: str = ""

# ── Push 구독 저장소 ──────────────────────────────────────────────
# {endpoint_hash: {"sub": {...}, "alerts": {code: {"target": int|None, "stopLoss": int|None, "bigMove": bool}}}}
_push_subs: dict[str, dict] = {}
_fired: set = set()  # "hash:code:type:YYYYMMDD"

def _load_listing(market: str) -> list[dict]:
    df = fdr.StockListing(market)
    if df is None or df.empty:
        return []
    # 컬럼명 정규화 (버전마다 다를 수 있음)
    df.columns = [c.strip() for c in df.columns]
    code_col = next((c for c in df.columns if c in ("Symbol", "Code", "종목코드")), None)
    name_col = next((c for c in df.columns if c in ("Name", "종목명")), None)
    if not code_col or not name_col:
        print(f"[{market}] 컬럼 인식 실패. 실제 컬럼: {df.columns.tolist()}")
        return []
    return [
        {"c": str(row[code_col]), "n": str(row[name_col]), "m": market}
        for _, row in df.iterrows()
        if pd.notna(row.get(code_col)) and pd.notna(row.get(name_col))
        and str(row[code_col]).strip()
    ]

async def _alert_worker():
    while True:
        await asyncio.sleep(60)
        if not _push_subs:
            continue
        codes = {code for v in _push_subs.values() for code in v["alerts"]}
        if not codes:
            continue

        loop = asyncio.get_event_loop()
        prices: dict[str, dict] = {}
        for code in codes:
            try:
                data = await loop.run_in_executor(None, lambda c=code: _fetch_naver_current(c))
                prices[code] = data
            except Exception:
                pass

        today = datetime.now().strftime("%Y%m%d")
        for ep_hash, info in list(_push_subs.items()):
            stock_names = {s["c"]: s["n"] for s in _stock_db}
            for code, rule in info["alerts"].items():
                cp = prices.get(code)
                if not cp:
                    continue
                price = cp["close"]
                change_pct = cp["changePct"]
                name = stock_names.get(code, code)
                _check_and_push(ep_hash, info["sub"], code, name, rule, price, change_pct, today)


def _check_and_push(ep_hash, sub, code, name, rule, price, change_pct, today):
    from pywebpush import webpush, WebPushException

    def send(tag, title, body):
        key = f"{ep_hash}:{code}:{tag}:{today}"
        if key in _fired:
            return
        _fired.add(key)
        try:
            webpush(
                subscription_info=sub,
                data=json.dumps({"title": title, "body": body, "tag": tag}),
                vapid_private_key=_vapid_private,
                vapid_claims={"sub": "mailto:stock-alert@example.com"},
            )
        except WebPushException as e:
            if "410" in str(e) or "404" in str(e):
                _push_subs.pop(ep_hash, None)

    target = rule.get("target")
    stop_loss = rule.get("stopLoss")
    big_move = rule.get("bigMove", False)

    if target and price >= target:
        send("target", f"🎯 목표가 도달 — {name}", f"현재가 ₩{price:,} (목표 ₩{target:,})")
    if stop_loss and price <= stop_loss:
        send("stopLoss", f"⚠️ 손절가 도달 — {name}", f"현재가 ₩{price:,} (손절 ₩{stop_loss:,})")
    if big_move and abs(change_pct) >= 5:
        sign = "+" if change_pct > 0 else ""
        send("bigMove", f"📊 급등락 경보 — {name}", f"현재가 ₩{price:,}  {sign}{change_pct:.2f}%")


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _stock_db, _vapid_public, _vapid_private
    print("KRX 전종목 목록 로딩 중...")
    loop = asyncio.get_event_loop()
    try:
        kospi, kosdaq = await asyncio.gather(
            loop.run_in_executor(None, lambda: _load_listing("KOSPI")),
            loop.run_in_executor(None, lambda: _load_listing("KOSDAQ")),
        )
        _stock_db = kospi + kosdaq
        print(f"종목 목록 로딩 완료: KOSPI {len(kospi)}개 + KOSDAQ {len(kosdaq)}개 = {len(_stock_db)}개")
    except Exception as e:
        print(f"종목 목록 로드 실패: {e}")

    try:
        _vapid_public, _vapid_private = _load_or_create_vapid_keys()
        print(f"[VAPID] 공개키: {_vapid_public[:20]}...")
    except Exception as e:
        print(f"[VAPID] 키 로드 실패: {e}")

    task = asyncio.create_task(_alert_worker())
    yield
    task.cancel()


app = FastAPI(title="한국 주식 트래커", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

# 서버 메모리 캐시 (30분 TTL)
_cache: dict = {}
CACHE_TTL = 1800

# 기간 → 조회할 캘린더일 수 (거래일 여유분 포함)
PERIOD_CAL_DAYS = {"1M": 45, "3M": 110, "6M": 210, "1Y": 400}

_current_cache: dict = {}
CURRENT_CACHE_TTL = 30  # 30초

_news_cache: dict = {}
NEWS_CACHE_TTL = 600  # 10분

_fundamentals_cache: dict = {}
FUNDAMENTALS_CACHE_TTL = 600  # 10분

_index_cache: dict = {}
INDEX_CACHE_TTL = 300  # 5분

NAVER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) AppleWebKit/605.1.15"
}

def _fetch_naver_current(code: str) -> dict:
    res = requests.get(
        f"https://m.stock.naver.com/api/stock/{code}/basic",
        headers=NAVER_HEADERS,
        timeout=5,
    )
    res.raise_for_status()
    j = res.json()

    def clean(s):
        return str(s).replace(",", "").replace("+", "").strip()

    close = j.get("closePrice") or j.get("stockPrice") or "0"
    change = j.get("compareToPreviousClosePrice", "0")
    change_pct = j.get("fluctuationsRatio", "0")
    # 부호 처리: 네이버는 상승 시 양수, 하락 시 음수로 반환
    try:
        change_int = int(clean(change))
    except:
        change_int = 0
    try:
        change_f = float(clean(change_pct))
    except:
        change_f = 0.0

    return {
        "close": int(clean(close)),
        "change": change_int,
        "changePct": change_f,
    }


@app.get("/api/current/{code}")
async def get_current_price(code: str):
    if code in _current_cache:
        ts, data = _current_cache[code]
        if time.time() - ts < CURRENT_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, lambda: _fetch_naver_current(code))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"현재가 조회 실패: {e}")
    _current_cache[code] = (time.time(), data)
    return data


def _fetch_naver_fundamentals(code: str) -> dict:
    import re
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        return {"per": None, "pbr": None, "marcap": None, "div": None}

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/",
    }
    res = requests.get(
        f"https://finance.naver.com/item/sise.naver?code={code}",
        headers=headers,
        timeout=5,
    )
    res.raise_for_status()
    res.encoding = "euc-kr"
    soup = BeautifulSoup(res.text, "html.parser")

    def clean(s):
        return s.replace(",", "").replace("+", "").strip() if s else ""

    result = {"per": None, "pbr": None, "marcap": None, "div": None}

    # PER
    per_th = soup.find("th", string="PER")
    if per_th:
        td = per_th.find_next_sibling("td")
        if td:
            try:
                result["per"] = float(clean((td.find("em") or td).get_text(strip=True)))
            except Exception:
                pass

    # PBR
    for th in soup.find_all("th"):
        if th.get_text(strip=True).startswith("PBR"):
            td = th.find_next_sibling("td")
            if td:
                try:
                    result["pbr"] = float(clean((td.find("em") or td).get_text(strip=True)))
                except Exception:
                    pass
            break

    # 시가총액 (억 단위)
    for th in soup.find_all("th"):
        if "시가총액" in th.get_text():
            td = th.find_next_sibling("td")
            if td:
                m = re.search(r"([\d,]+)억", td.get_text(strip=True))
                if m:
                    result["marcap"] = int(m.group(1).replace(",", ""))
                    break

    # 배당수익률
    for th in soup.find_all("th"):
        if "배당수익률" in th.get_text():
            td = th.find_next_sibling("td")
            if td:
                m = re.search(r"([\d.]+)%", td.get_text(strip=True))
                if m:
                    result["div"] = float(m.group(1))
            break

    return result


@app.get("/api/financials/{code}")
async def get_financials(code: str):
    if code in _fundamentals_cache:
        ts, data = _fundamentals_cache[code]
        if time.time() - ts < FUNDAMENTALS_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, lambda: _fetch_naver_fundamentals(code))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"재무 지표 조회 실패: {e}")
    _fundamentals_cache[code] = (time.time(), data)
    return data


def _fetch_index_data() -> dict:
    result = {}
    for naver_code, key in [("KOSPI", "kospi"), ("KOSDAQ", "kosdaq")]:
        try:
            res = requests.get(
                f"https://m.stock.naver.com/api/index/{naver_code}/basic",
                headers=NAVER_HEADERS,
                timeout=5,
            )
            res.raise_for_status()
            j = res.json()

            def clean(s):
                return str(s).replace(",", "").replace("+", "").strip()

            value = round(float(clean(j.get("closePrice", "0"))), 2)
            change = round(float(clean(j.get("compareToPreviousClosePrice", "0"))), 2)
            change_pct = round(float(clean(j.get("fluctuationsRatio", "0"))), 2)
            result[key] = {"value": value, "change": change, "changePct": change_pct}
        except Exception:
            result[key] = None
    return result


@app.get("/api/index")
async def get_market_index():
    if "data" in _index_cache:
        ts, data = _index_cache["data"]
        if time.time() - ts < INDEX_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, _fetch_index_data)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"지수 조회 실패: {e}")
    _index_cache["data"] = (time.time(), data)
    return data


@app.get("/api/push/key")
async def get_vapid_key():
    if not _vapid_public:
        raise HTTPException(status_code=503, detail="VAPID 키 미설정")
    return {"publicKey": _vapid_public}


@app.post("/api/push/subscribe")
async def push_subscribe(request: Request):
    body = await request.json()
    sub = body.get("subscription")
    alerts = body.get("alerts", {})
    if not sub or not sub.get("endpoint"):
        raise HTTPException(status_code=400, detail="subscription 필드 누락")
    ep_hash = hashlib.sha256(sub["endpoint"].encode()).hexdigest()[:16]
    _push_subs[ep_hash] = {"sub": sub, "alerts": alerts}
    return {"ok": True, "hash": ep_hash}


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    body = await request.json()
    endpoint = body.get("endpoint", "")
    ep_hash = hashlib.sha256(endpoint.encode()).hexdigest()[:16]
    _push_subs.pop(ep_hash, None)
    return {"ok": True}


@app.get("/api/news/{code}")
async def get_news(code: str, name: str = ""):
    if code in _news_cache:
        ts, data = _news_cache[code]
        if time.time() - ts < NEWS_CACHE_TTL:
            return data

    # 종목명 결정 (파라미터 우선, 없으면 DB 조회)
    stock_name = name or next((s["n"] for s in _stock_db if s["c"] == code), code)
    query = quote(f"{stock_name} 주식")
    url = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko"

    loop = asyncio.get_event_loop()
    try:
        res = await loop.run_in_executor(None, lambda: requests.get(
            url, headers=NAVER_HEADERS, timeout=8
        ))
        res.raise_for_status()
        root = ET.fromstring(res.content)
        news = []
        for item in root.findall(".//item")[:6]:
            source_el = item.find("source")
            pub = item.findtext("pubDate", "")
            # "Mon, 11 May 2026 08:17:02 GMT" → "05/11"
            try:
                from email.utils import parsedate
                d = parsedate(pub)
                date_str = f"{d[1]:02d}/{d[2]:02d}" if d else ""
            except:
                date_str = ""
            news.append({
                "title": item.findtext("title", "").strip(),
                "link":  item.findtext("link", "").strip(),
                "source": source_el.text if source_el is not None else "",
                "date": date_str,
            })
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    _news_cache[code] = (time.time(), news)
    return news


@app.get("/api/stocks/search")
def search_stocks(q: str = ""):
    q = q.strip()
    if not q:
        return []
    ql = q.lower()
    results = [s for s in _stock_db if ql in s["n"].lower() or q in s["c"]]
    return results[:50]


@app.get("/api/prices/{code}")
def get_prices(code: str, period: str = "1M"):
    if period not in PERIOD_CAL_DAYS:
        period = "1M"

    cache_key = f"{code}:{period}"
    if cache_key in _cache:
        ts, data = _cache[cache_key]
        if time.time() - ts < CACHE_TTL:
            return data

    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=PERIOD_CAL_DAYS[period])

    try:
        df = fdr.DataReader(code, start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"데이터 조회 오류: {e}")

    if df is None or df.empty:
        raise HTTPException(status_code=404, detail="데이터가 없습니다. 종목코드를 확인하세요.")

    df = df.copy()
    df["change"] = df["Close"].diff().fillna(0).round(0).astype(int)

    prices = [
        {
            "date": date.strftime("%Y-%m-%d"),
            "close": int(row["Close"]),
            "change": int(row["change"]),
            "changePct": round(float(row.get("Change", 0)) * 100, 2),
        }
        for date, row in df.iterrows()
    ]
    prices.reverse()  # 최신 → 과거 순

    result = {"prices": prices, "source": "KRX (FinanceDataReader)"}
    _cache[cache_key] = (time.time(), result)
    return result


# 정적 파일은 마지막에 마운트 (API 라우트보다 후순위)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
