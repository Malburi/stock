from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
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
from pykrx import stock as pykrx_stock
# .env 로드 (python-dotenv 없어도 동작, 있으면 env var 자동 주입)
try:
    from dotenv import load_dotenv; load_dotenv()
except ImportError:
    pass

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
_price_snapshots: list = []  # [{ts, data:{code:{close,changePct,n,m}}}] 최대 3개 (5분 간격)

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


def _fetch_price_snapshot() -> dict:
    """sise_rise(등락률 상위) 스냅샷 — 가격 급등 감지용."""
    items = _fetch_surge_naver()  # KOSPI+KOSDAQ 등락률 상위 50개
    return {
        s["c"]: {"close": s["close"], "changePct": s["changePct"], "n": s["n"], "m": s["m"]}
        for s in items
    }


async def _surge_alert_worker():
    """5분마다 등락률 스냅샷 → 5분/10분 전 대비 급등 감지 → 푸시.
    기준: 5분 내 등락률 3%p 이상 추가 상승.
    """
    global _price_snapshots
    await asyncio.sleep(120)
    while True:
        try:
            has_subs = any(v.get("surgeAlert") for v in _push_subs.values())
            if has_subs:
                loop = asyncio.get_event_loop()
                snap_data = await loop.run_in_executor(None, _fetch_price_snapshot)
                if snap_data:
                    _price_snapshots.append({"ts": time.time(), "data": snap_data})
                    if len(_price_snapshots) > 3:
                        _price_snapshots.pop(0)

                    if len(_price_snapshots) >= 2:
                        curr  = _price_snapshots[-1]["data"]
                        prev5 = _price_snapshots[-2]["data"]
                        prev10 = _price_snapshots[-3]["data"] if len(_price_snapshots) >= 3 else None

                        surging = []
                        for code, cur in curr.items():
                            if code not in prev5 or cur["changePct"] <= 0:
                                continue
                            delta5 = cur["changePct"] - prev5[code]["changePct"]
                            if delta5 < 3.0:  # 5분 내 3%p 이상 상승
                                continue
                            prev5_pct = prev5[code]["changePct"]
                            prev10_pct = prev10[code]["changePct"] if (prev10 and code in prev10) else prev5_pct
                            surging.append((code, cur, delta5, prev5_pct, prev10_pct))

                        surging.sort(key=lambda x: x[2], reverse=True)

                        if surging:
                            from pywebpush import webpush, WebPushException
                            for ep_hash, info in list(_push_subs.items()):
                                if not info.get("surgeAlert"):
                                    continue
                                for code, s_info, delta5, prev5_pct, prev10_pct in surging[:3]:
                                    try:
                                        webpush(
                                            subscription_info=info["sub"],
                                            data=json.dumps({
                                                "title": f"🚀 주가 급등 — {s_info['n']}",
                                                "body": (
                                                    f"₩{s_info['close']:,}  현재 +{s_info['changePct']:.1f}%\n"
                                                    f"10분전 {prev10_pct:+.1f}% → 5분전 {prev5_pct:+.1f}% → 지금 +{s_info['changePct']:.1f}%"
                                                ),
                                                "tag": f"surge:{code}:{int(time.time()//300)}",
                                            }),
                                            vapid_private_key=_vapid_private,
                                            vapid_claims={"sub": "mailto:stock-alert@example.com"},
                                        )
                                    except WebPushException as e:
                                        if "410" in str(e) or "404" in str(e):
                                            _push_subs.pop(ep_hash, None)
                                    except Exception:
                                        pass
        except Exception as e:
            print(f"[surge_alert] 오류: {e}")
        await asyncio.sleep(300)


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
    surge_task = asyncio.create_task(_surge_alert_worker())
    yield
    task.cancel()
    surge_task.cancel()


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
NEWS_CACHE_TTL = 120  # 2분

_fundamentals_cache: dict = {}
FUNDAMENTALS_CACHE_TTL = 600  # 10분

_index_cache: dict = {}
INDEX_CACHE_TTL = 300  # 5분

_ai_cache: dict = {}
AI_CACHE_TTL = 3600  # 1시간

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

_investor_cache: dict = {}
INVESTOR_CACHE_TTL = 300  # 5분

_ranking_cache: dict = {}
RANKING_CACHE_TTL = 600   # 10분
_surge_cache: dict = {}
SURGE_CACHE_TTL = 300     # 5분
_themes_cache: dict = {}
THEMES_CACHE_TTL = 300    # 5분
_earnings_cache: dict = {}
EARNINGS_CACHE_TTL = 3600 # 1시간
_theme_detail_cache: dict = {}
THEME_DETAIL_CACHE_TTL = 300  # 5분

DART_API_KEY = os.environ.get("DART_API_KEY", "")

# ── KIS OpenAPI 설정 ──────────────────────────────────────────────
KIS_APP_KEY    = os.environ.get("KIS_APP_KEY", "")
KIS_APP_SECRET = os.environ.get("KIS_APP_SECRET", "")
KIS_BASE_URL   = "https://openapi.koreainvestment.com:9443"

_KIS_TOKEN_FILE = ".kis_token"
_kis_token: dict = {"token": "", "expires_at": 0}  # 메모리 캐시 (재시작 시 파일에서 복원)

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


def _fetch_fundamentals(code: str) -> dict:
    """KIS inquire-price 우선, 실패 시 Naver fallback."""
    import re
    from bs4 import BeautifulSoup

    result = {
        "per": None, "pbr": None, "marcap": None, "div": None,
        "high52": None, "low52": None, "foreign_ratio": None,
    }

    # ── 1. KIS inquire-price (PER, PBR, 시가총액, 52주 고저, 외국인 소진율) ──
    _key = os.environ.get("KIS_APP_KEY") or KIS_APP_KEY
    _sec = os.environ.get("KIS_APP_SECRET") or KIS_APP_SECRET
    if _key and _sec:
        try:
            token = _get_kis_token()
            r = requests.get(
                f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
                headers={"authorization": f"Bearer {token}", "appkey": KIS_APP_KEY,
                         "appsecret": KIS_APP_SECRET, "tr_id": "FHKST01010100", "custtype": "P"},
                timeout=8,
            )
            if r.ok:
                o = r.json().get("output") or {}
                def _f(k):
                    try: return float(o[k]) or None
                    except: return None
                def _i(k):
                    try: return int(o[k]) or None
                    except: return None
                result["per"]           = _f("per")
                result["pbr"]           = _f("pbr")
                result["high52"]        = _i("w52_hgpr")
                result["low52"]         = _i("w52_lwpr")
                result["foreign_ratio"] = _f("hts_frgn_ehrt")
                avls = _f("hts_avls")  # 억원 단위
                if avls: result["marcap"] = round(avls)
                result["vol"]           = _i("acml_vol")        # 누적 거래량 (주)
                result["tr_pbmn"]       = _i("acml_tr_pbmn")   # 누적 거래대금 (백만원)
        except Exception:
            pass

    # ── 2. Naver fallback (배당수익률, KIS 실패 시 나머지 필드) ────────
    naver_needed = result["per"] is None or result["div"] is None
    if naver_needed:
        try:
            pc_headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                          "Referer": "https://finance.naver.com/"}
            r2 = requests.get(f"https://finance.naver.com/item/sise.naver?code={code}",
                              headers=pc_headers, timeout=5)
            r2.encoding = "euc-kr"
            soup = BeautifulSoup(r2.text, "html.parser")
            for th in soup.find_all("th"):
                t = th.get_text(strip=True)
                td = th.find_next_sibling("td")
                if not td: continue
                td_text = td.get_text(strip=True)
                if "배당수익률" in t:
                    m = re.search(r"([\d.]+)%", td_text)
                    if m: result["div"] = float(m.group(1))
                elif result["per"] is None and "PER" in t:
                    try: result["per"] = float(re.sub(r"[^\d.]", "", td_text)) or None
                    except: pass
                elif result["high52"] is None and ("52주 최고" in t or t == "52주최고"):
                    m = re.search(r"([\d,]+)", td_text)
                    if m: result["high52"] = int(m.group(1).replace(",", ""))
                elif result["low52"] is None and ("52주 최저" in t or t == "52주최저"):
                    m = re.search(r"([\d,]+)", td_text)
                    if m: result["low52"] = int(m.group(1).replace(",", ""))
                elif result["foreign_ratio"] is None and "소진율" in t:
                    m = re.search(r"([\d.]+)%", td_text)
                    if m: result["foreign_ratio"] = float(m.group(1))
        except Exception:
            pass

    return result


@app.get("/api/financials/{code}")
async def get_financials(code: str):
    if code in _fundamentals_cache:
        ts, data = _fundamentals_cache[code]
        if time.time() - ts < FUNDAMENTALS_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, lambda: _fetch_fundamentals(code))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"재무 지표 조회 실패: {e}")
    _fundamentals_cache[code] = (time.time(), data)
    return data


def _get_kis_token() -> str:
    """KIS OAuth 토큰 반환. 파일 캐시로 서버 재시작 시에도 재활용 (분당 1회 발급 제한 우회)."""
    app_key = os.environ.get("KIS_APP_KEY") or KIS_APP_KEY
    app_secret = os.environ.get("KIS_APP_SECRET") or KIS_APP_SECRET
    if not app_key or not app_secret:
        raise RuntimeError("KIS_APP_KEY / KIS_APP_SECRET 환경변수 미설정")
    # 1) 메모리 캐시 유효
    if time.time() < _kis_token["expires_at"] - 300 and _kis_token["token"]:
        return _kis_token["token"]
    # 2) 파일 캐시 복원
    if os.path.exists(_KIS_TOKEN_FILE):
        try:
            with open(_KIS_TOKEN_FILE) as f:
                saved = json.load(f)
            if time.time() < saved.get("expires_at", 0) - 300:
                _kis_token["token"] = saved["token"]
                _kis_token["expires_at"] = saved["expires_at"]
                return _kis_token["token"]
        except Exception:
            pass
    # 3) 새 토큰 발급
    r = requests.post(
        f"{KIS_BASE_URL}/oauth2/tokenP",
        json={"grant_type": "client_credentials", "appkey": app_key, "appsecret": app_secret},
        headers={"content-type": "application/json"},
        timeout=10,
    )
    r.raise_for_status()
    j = r.json()
    if "access_token" not in j:
        raise RuntimeError(f"KIS 토큰 발급 실패: {j}")
    _kis_token["token"] = j["access_token"]
    _kis_token["expires_at"] = time.time() + j.get("expires_in", 86400)
    try:
        with open(_KIS_TOKEN_FILE, "w") as f:
            json.dump({"token": _kis_token["token"], "expires_at": _kis_token["expires_at"]}, f)
    except Exception:
        pass
    return _kis_token["token"]


def _fetch_kis_investor(code: str) -> dict:
    token = _get_kis_token()
    r = requests.get(
        f"{KIS_BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-investor",
        params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
        headers={
            "content-type": "application/json",
            "authorization": f"Bearer {token}",
            "appkey": KIS_APP_KEY,
            "appsecret": KIS_APP_SECRET,
            "tr_id": "FHKST01010900",
            "custtype": "P",
        },
        timeout=10,
    )
    j = r.json() if r.ok else {}
    if j.get("rt_cd") not in (None, "0"):
        return {}
    rows = j.get("output") or j.get("output1") or []
    if not rows:
        return {}
    # output[0] = 당일(또는 최근 영업일)
    today = rows[0]
    def to_bil(s):
        # KIS tr_pbmn 단위: 백만원 → 억원 변환
        try: return round(int(s) / 100, 1)
        except: return None
    return {
        "date": today.get("stck_bsop_date", ""),
        "개인":  {"net": to_bil(today.get("prsn_ntby_tr_pbmn", "0"))},
        "기관":  {"net": to_bil(today.get("orgn_ntby_tr_pbmn", "0"))},
        "외국인":{"net": to_bil(today.get("frgn_ntby_tr_pbmn", "0"))},
    }





@app.get("/api/kis/check")
async def kis_check():
    """KIS 환경변수 로드 여부 확인 (키 값은 노출하지 않음)"""
    key = os.environ.get("KIS_APP_KEY", "")
    sec = os.environ.get("KIS_APP_SECRET", "")
    return {
        "KIS_APP_KEY_set": bool(key),
        "KIS_APP_KEY_len": len(key),
        "KIS_APP_SECRET_set": bool(sec),
        "KIS_APP_SECRET_len": len(sec),
        "module_key_set": bool(KIS_APP_KEY),
    }


@app.get("/api/investor/{code}")
async def get_investor(code: str):
    app_key = os.environ.get("KIS_APP_KEY", "") or KIS_APP_KEY
    app_secret = os.environ.get("KIS_APP_SECRET", "") or KIS_APP_SECRET
    if not app_key or not app_secret:
        raise HTTPException(status_code=503, detail="KIS API 키 미설정")
    if code in _investor_cache:
        ts, data = _investor_cache[code]
        if time.time() - ts < INVESTOR_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        data = await loop.run_in_executor(None, lambda: _fetch_kis_investor(code))
    except Exception as e:
        print(f"[investor] {code} 조회 실패: {e}")
        return {}  # UI에서 섹션 숨김 처리
    _investor_cache[code] = (time.time(), data)
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
    surge_alert = body.get("surgeAlert", False)
    _push_subs[ep_hash] = {"sub": sub, "alerts": alerts, "surgeAlert": surge_alert}
    return {"ok": True, "hash": ep_hash}


@app.post("/api/push/unsubscribe")
async def push_unsubscribe(request: Request):
    body = await request.json()
    endpoint = body.get("endpoint", "")
    ep_hash = hashlib.sha256(endpoint.encode()).hexdigest()[:16]
    _push_subs.pop(ep_hash, None)
    return {"ok": True}


def _build_gemini_prompt(stock_name: str, code: str, current: dict, fund: dict, investor: dict, news: list) -> str:
    lines = [f"종목: {stock_name} ({code})\n"]

    if current:
        lines.append(f"현재가: {current.get('close', '-'):,}원  등락: {current.get('change', 0):+,}원 ({current.get('changePct', 0):+.2f}%)")

    if fund:
        per  = f"{fund['per']:.2f}배"  if fund.get('per')  else '-'
        pbr  = f"{fund['pbr']:.2f}배"  if fund.get('pbr')  else '-'
        div  = f"{fund['div']:.2f}%"   if fund.get('div')  else '-'
        h52  = f"{fund['high52']:,}원"  if fund.get('high52') else '-'
        l52  = f"{fund['low52']:,}원"   if fund.get('low52')  else '-'
        fr   = f"{fund['foreign_ratio']:.2f}%" if fund.get('foreign_ratio') else '-'
        marc = f"{fund['marcap']:,}억원" if fund.get('marcap') else '-'
        lines.append(f"PER: {per}  PBR: {pbr}  배당률: {div}")
        lines.append(f"시가총액: {marc}  52주 최고: {h52}  52주 최저: {l52}  외국인 비중: {fr}")

    if investor and investor.get('개인'):
        rows = []
        for who in ['개인', '기관', '외국인']:
            net = investor.get(who, {}).get('net')
            if net is not None:
                rows.append(f"{who} {net:+.0f}억")
        lines.append("당일 수급: " + "  ".join(rows))

    if news:
        headlines = "  /  ".join(n['title'] for n in news[:4])
        lines.append(f"최신 뉴스: {headlines}")

    lines.append("""
위 데이터를 바탕으로 단기(1~4주) 관점의 매매 의견을 아래 JSON 형식으로만 답하세요. 다른 텍스트는 절대 출력하지 마세요.

{
  "verdict": "매수관심 또는 중립 또는 매도고려",
  "summary": "핵심 한 줄 요약 (30자 이내)",
  "reasons": ["근거1", "근거2", "근거3"],
  "risk": "주요 리스크 한 줄"
}""")
    return "\n".join(lines)


@app.get("/api/ai/opinion/{code}")
async def get_ai_opinion(code: str, force: bool = False):
    api_key = os.environ.get("GEMINI_API_KEY") or GEMINI_API_KEY
    if not api_key:
        raise HTTPException(status_code=503, detail="GEMINI_API_KEY 미설정")

    if not force and code in _ai_cache:
        ts, data = _ai_cache[code]
        if time.time() - ts < AI_CACHE_TTL:
            return data

    loop = asyncio.get_event_loop()
    stock_name = next((s["n"] for s in _stock_db if s["c"] == code), code)

    current, fund = await asyncio.gather(
        loop.run_in_executor(None, lambda: _fetch_naver_current(code)),
        loop.run_in_executor(None, lambda: _fetch_fundamentals(code)),
        return_exceptions=True,
    )
    if isinstance(current, Exception): current = {}
    if isinstance(fund, Exception):    fund = {}

    # 뉴스·수급은 캐시 우선
    news_data = _news_cache.get(code, (0, []))[1]
    inv_data  = _investor_cache.get(code, (0, {}))[1]

    prompt = _build_gemini_prompt(stock_name, code, current, fund, inv_data, news_data)

    try:
        from google import genai
        client = genai.Client(api_key=api_key)
        MODELS = ["models/gemini-2.5-flash", "models/gemini-2.0-flash", "models/gemini-flash-latest"]
        last_err = None
        resp = None
        for model in MODELS:
            for attempt in range(3):
                try:
                    resp = await loop.run_in_executor(
                        None, lambda m=model: client.models.generate_content(model=m, contents=prompt)
                    )
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    msg = str(e)
                    if "503" in msg or "429" in msg:
                        await asyncio.sleep(2 ** attempt)
                        continue
                    break
            if resp:
                break
        if not resp:
            raise last_err
        raw  = resp.text.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        data = json.loads(raw)
    except Exception as e:
        print(f"[AI] Gemini 오류: {type(e).__name__}: {e}")
        raise HTTPException(status_code=500, detail=f"Gemini 호출 실패: {type(e).__name__}: {e}")

    result = {**data, "generatedAt": time.time(), "stockName": stock_name}
    _ai_cache[code] = (time.time(), result)
    return result


@app.get("/api/stream/prices")
async def stream_prices(codes: str = ""):
    """SSE: 5초마다 관심종목 현재가 일괄 푸시."""
    code_list = [c.strip() for c in codes.split(",") if c.strip()][:20]

    async def generate():
        loop = asyncio.get_event_loop()
        while True:
            if code_list:
                tasks = [
                    loop.run_in_executor(None, lambda c=code: _fetch_naver_current(c))
                    for code in code_list
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                now = time.time()
                prices = {}
                for code, result in zip(code_list, results):
                    if not isinstance(result, Exception):
                        prices[code] = result
                        _current_cache[code] = (now, result)  # SSE 결과를 캐시에 저장
                    elif code in _current_cache:
                        ts, cached = _current_cache[code]
                        if now - ts < 60:  # 네이버 실패 시 최대 60초 캐시 폴백
                            prices[code] = cached
                if prices:
                    yield f"data: {json.dumps(prices)}\n\n"
                else:
                    yield ": keepalive\n\n"
            else:
                yield ": keepalive\n\n"
            await asyncio.sleep(5)

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"},
    )


def _dedupe_news(items: list, time_window_min: int = 90, text_threshold: float = 0.12) -> list:
    """시간 근접(90분) + 2-gram 유사도로 같은 사건 중복 기사 제거."""
    import re as _re
    from datetime import datetime

    def parse_dt(s: str):
        for fmt in ("%Y.%m.%d %H:%M", "%Y.%m.%d"):
            try:
                return datetime.strptime(s, fmt)
            except Exception:
                pass
        return None

    def bigrams(text: str) -> set:
        t = _re.sub(r"[^\w]", "", text)
        return {t[i:i+2] for i in range(len(t) - 1)}

    def sim(t1: str, t2: str) -> float:
        g1, g2 = bigrams(t1), bigrams(t2)
        if not g1 or not g2:
            return 0.0
        return len(g1 & g2) / len(g1 | g2)

    kept = []
    for item in items:
        dt = parse_dt(item.get("date", ""))
        dup = False
        for k in kept:
            s = sim(item["title"], k["title"])
            if s >= 0.35:          # 텍스트만으로 명백한 중복
                dup = True; break
            k_dt = parse_dt(k.get("date", ""))
            if dt and k_dt:
                diff = abs((dt - k_dt).total_seconds() / 60)
                if diff <= time_window_min and s >= text_threshold:
                    dup = True; break   # 같은 시간대 + 약한 유사도 → 동일 사건
        if not dup:
            kept.append(item)
    return kept


@app.get("/api/news/{code}")
async def get_news(code: str, name: str = ""):
    if code in _news_cache:
        ts, data = _news_cache[code]
        if time.time() - ts < NEWS_CACHE_TTL:
            return data

    from bs4 import BeautifulSoup
    loop = asyncio.get_event_loop()
    news = []
    pc_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://finance.naver.com/",
    }

    # ── 1. 네이버 금융 종목 뉴스 (1차) ───────────────────────────
    try:
        res = await loop.run_in_executor(None, lambda: requests.get(
            f"https://finance.naver.com/item/news_news.naver?code={code}&page=1",
            headers=pc_headers, timeout=6,
        ))
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")
        for row in soup.select("table.type5 tr"):
            a    = row.select_one("td.title a")
            date = row.select_one("td.date")
            press= row.select_one("td.info")
            if not a:
                continue
            href = a.get("href", "")
            full_link = f"https://finance.naver.com{href}" if href.startswith("/") else href
            news.append({
                "title":  a.get_text(strip=True),
                "link":   full_link,
                "source": press.get_text(strip=True) if press else "",
                "date":   date.get_text(strip=True) if date else "",
            })
            if len(news) >= 20:
                break
    except Exception:
        pass

    # ── 2. Google News RSS fallback ───────────────────────────────
    if not news:
        try:
            stock_name = name or next((s["n"] for s in _stock_db if s["c"] == code), code)
            query = quote(f"{stock_name} 주식")
            url   = f"https://news.google.com/rss/search?q={query}&hl=ko&gl=KR&ceid=KR:ko&tbs=qdr:d"
            res   = await loop.run_in_executor(None, lambda: requests.get(url, headers=NAVER_HEADERS, timeout=8))
            res.raise_for_status()
            root  = ET.fromstring(res.content)
            from email.utils import parsedate
            for item in root.findall(".//item")[:8]:
                source_el = item.find("source")
                pub = item.findtext("pubDate", "")
                try:
                    d = parsedate(pub)
                    date_str = f"{d[1]:02d}/{d[2]:02d}" if d else ""
                except:
                    date_str = ""
                news.append({
                    "title":  item.findtext("title", "").strip(),
                    "link":   item.findtext("link",  "").strip(),
                    "source": source_el.text if source_el is not None else "",
                    "date":   date_str,
                })
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    news = _dedupe_news(news)[:6]
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


def _naver_clean(s: str) -> str:
    return s.replace(",", "").replace("+", "").replace("%", "").strip()


def _fetch_ranking_naver(sosok: str, by: str) -> list:
    """Naver 시총/거래량 TOP30 스크래핑.
    sise_market_sum columns(0-based, with rank col):
      0:순위 1:종목명(link) 2:현재가 3:전일비 4:등락률 5:액면가
      6:시가총액(억) 7:상장주식수 8:외국인비율 9:거래량 10:PER 11:ROE
    sise_quant columns:
      0:순위 1:종목명(link) 2:현재가 3:전일비 4:등락률
      5:거래량 6:전일거래량 7:? 8:? 9:?
    """
    from bs4 import BeautifulSoup
    ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if by == "marcap":
        url = f"https://finance.naver.com/sise/sise_market_sum.naver?sosok={sosok}"
    else:
        url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}"
    res = requests.get(url, headers=ua, timeout=10)
    res.encoding = "euc-kr"
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.select_one("table.type_2")
    if not table:
        return []
    market = "KOSPI" if sosok == "0" else "KOSDAQ"
    items = []
    for row in table.select("tr"):
        a_tag = row.select_one("td a[href*='code=']")
        if not a_tag:
            continue
        code = a_tag["href"].split("code=")[-1].split("&")[0].zfill(6)
        tds = row.select("td")
        try:
            if by == "marcap":
                close = int(_naver_clean(tds[2].get_text())) if len(tds) > 2 else 0
                change_pct = float(_naver_clean(tds[4].get_text()) or "0") if len(tds) > 4 else 0.0
                marcap_raw = _naver_clean(tds[6].get_text()) if len(tds) > 6 else "0"
                marcap_int = int(marcap_raw) if marcap_raw.isdigit() or (marcap_raw.lstrip('-').isdigit()) else 0
                volume = int(_naver_clean(tds[9].get_text()) or "0") if len(tds) > 9 else 0
                marcap_str = f"{marcap_int:,}억"
            else:
                close = int(_naver_clean(tds[2].get_text())) if len(tds) > 2 else 0
                change_pct = float(_naver_clean(tds[4].get_text()) or "0") if len(tds) > 4 else 0.0
                volume = int(_naver_clean(tds[5].get_text()) or "0") if len(tds) > 5 else 0
                prev_vol = int(_naver_clean(tds[6].get_text()) or "0") if len(tds) > 6 else 0
                marcap_int = 0
                marcap_str = f"{volume:,}주"
        except (ValueError, IndexError):
            continue
        if not close:
            continue
        items.append({
            "c": code,
            "n": a_tag.get_text(strip=True),
            "m": market,
            "close": close,
            "changePct": change_pct,
            "volume": volume if by == "volume" else 0,
            "marcap": marcap_int,
            "marcapStr": marcap_str,
        })
    return items[:30]


@app.get("/api/discover/ranking")
async def discover_ranking(market: str = "KOSPI", by: str = "marcap"):
    """시총/거래량 TOP30. market=KOSPI|KOSDAQ, by=marcap|volume"""
    market = market.upper()
    if market not in ("KOSPI", "KOSDAQ"):
        raise HTTPException(status_code=400, detail="market은 KOSPI 또는 KOSDAQ")
    if by not in ("marcap", "volume"):
        raise HTTPException(status_code=400, detail="by는 marcap 또는 volume")

    cache_key = f"{market}:{by}"
    if cache_key in _ranking_cache:
        ts, data = _ranking_cache[cache_key]
        if time.time() - ts < RANKING_CACHE_TTL:
            return data

    sosok = "0" if market == "KOSPI" else "1"
    loop = asyncio.get_event_loop()
    try:
        items = await loop.run_in_executor(None, lambda: _fetch_ranking_naver(sosok, by))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스크래핑 실패: {e}")
    if not items:
        raise HTTPException(status_code=503, detail="데이터를 가져올 수 없습니다")

    out = {"market": market, "by": by, "items": items}
    _ranking_cache[cache_key] = (time.time(), out)
    return out


def _fetch_surge_naver(sosok: str = "both", by: str = "price") -> list:
    """급등주 스크래핑.
    by=price → sise_rise (등락률 상위): td[4]=등락률, td[5]=거래량, td[8]=전일거래량
    by=volume → sise_quant (거래량 상위): td[4]=등락률, td[5]=거래량, td[6]=전일거래량
    """
    from bs4 import BeautifulSoup
    ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    sosok_list = ["0", "1"] if sosok == "both" else [sosok]
    items = []
    for s in sosok_list:
        try:
            if by == "price":
                url = f"https://finance.naver.com/sise/sise_rise.naver?sosok={s}"
            else:
                url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={s}"
            res = requests.get(url, headers=ua, timeout=8)
            res.encoding = "euc-kr"
            soup = BeautifulSoup(res.text, "html.parser")
            table = soup.select_one("table.type_2")
            if not table:
                continue
            market = "KOSPI" if s == "0" else "KOSDAQ"
            for row in table.select("tr"):
                a_tag = row.select_one("td a[href*='code=']")
                if not a_tag:
                    continue
                code = a_tag["href"].split("code=")[-1].split("&")[0].zfill(6)
                tds = row.select("td")
                try:
                    close = int(_naver_clean(tds[2].get_text())) if len(tds) > 2 else 0
                    change_pct = float(_naver_clean(tds[4].get_text()) or "0") if len(tds) > 4 else 0.0
                    volume = int(_naver_clean(tds[5].get_text()) or "0") if len(tds) > 5 else 0
                    prev_vol_idx = 8 if by == "price" else 6
                    prev_volume = int(_naver_clean(tds[prev_vol_idx].get_text()) or "0") if len(tds) > prev_vol_idx else 0
                except (ValueError, IndexError):
                    continue
                if not close:
                    continue
                ratio = round(volume / prev_volume * 100, 1) if prev_volume > 0 else 0
                items.append({
                    "c": code, "n": a_tag.get_text(strip=True), "m": market,
                    "close": close, "changePct": change_pct,
                    "volume": volume, "prevVolume": prev_volume, "volumeRatio": ratio,
                })
        except Exception:
            continue
    if by == "price":
        items.sort(key=lambda x: x["changePct"], reverse=True)
    else:
        items.sort(key=lambda x: x["volume"], reverse=True)
    return items[:50]


@app.get("/api/discover/surge")
async def discover_surge(market: str = "BOTH", by: str = "price"):
    """급등주. market=KOSPI|KOSDAQ|BOTH, by=price|volume"""
    market = market.upper()
    if market not in ("KOSPI", "KOSDAQ", "BOTH"):
        market = "BOTH"
    if by not in ("price", "volume"):
        by = "price"
    sosok = {"KOSPI": "0", "KOSDAQ": "1", "BOTH": "both"}[market]
    cache_key = f"surge:{market}:{by}"
    if cache_key in _surge_cache:
        ts, data = _surge_cache[cache_key]
        if time.time() - ts < SURGE_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        items = await loop.run_in_executor(None, lambda: _fetch_surge_naver(sosok, by))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"스크래핑 실패: {e}")
    out = {"items": items, "market": market, "by": by}
    _surge_cache[cache_key] = (time.time(), out)
    return out


def _fetch_themes_naver() -> list:
    """Naver 테마 페이지 스크래핑.
    columns: 0:테마명(link) 1:전일대비 2:최근3일등락률
             3:상승수 4:하락수 5:보합수 6:대표종목1 7:대표종목2
    """
    from bs4 import BeautifulSoup
    ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    try:
        res = requests.get("https://finance.naver.com/sise/theme.naver", headers=ua, timeout=8)
        res.encoding = "euc-kr"
        soup = BeautifulSoup(res.text, "html.parser")
        table = soup.select_one("table.type_1")
        if not table:
            return []
        themes = []
        for row in table.select("tr"):
            tds = row.select("td")
            if len(tds) < 2:
                continue
            a_tag = tds[0].select_one("a")
            if not a_tag:
                continue
            href = a_tag.get("href", "")
            theme_no = href.split("no=")[-1] if "no=" in href else ""
            try:
                change_pct = float(_naver_clean(tds[1].get_text()) or "0")
            except ValueError:
                change_pct = 0.0
            # 대표종목 (tds[6], tds[7])
            rep1 = tds[6].get_text(strip=True) if len(tds) > 6 else ""
            rep2 = tds[7].get_text(strip=True) if len(tds) > 7 else ""
            top_stocks = ", ".join(s for s in [rep1, rep2] if s)
            themes.append({
                "name": a_tag.get_text(strip=True),
                "no": theme_no,
                "changePct": change_pct,
                "topStocks": top_stocks,
                "url": f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={theme_no}",
            })
        themes.sort(key=lambda x: x["changePct"], reverse=True)
        return themes[:30]
    except Exception:
        return []


@app.get("/api/discover/themes")
async def discover_themes():
    """당일 테마주 트렌드 (Naver 테마 페이지)."""
    if "themes" in _themes_cache:
        ts, data = _themes_cache["themes"]
        if time.time() - ts < THEMES_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    themes = await loop.run_in_executor(None, _fetch_themes_naver)
    out = {"items": themes}
    _themes_cache["themes"] = (time.time(), out)
    return out


def _fetch_theme_stocks(no: str) -> list:
    """Naver 테마 상세 페이지에서 소속 종목 스크래핑.
    type_5 columns: 0:종목명 1:차트링크 2:현재가 3:전일비 4:등락률 5:매수호가 6:매도호가
    """
    from bs4 import BeautifulSoup
    ua = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    url = f"https://finance.naver.com/sise/sise_group_detail.naver?type=theme&no={no}"
    res = requests.get(url, headers=ua, timeout=10)
    res.encoding = "euc-kr"
    soup = BeautifulSoup(res.text, "html.parser")
    table = soup.select_one("table.type_5")
    if not table:
        return []
    items = []
    for row in table.select("tr"):
        # td[0]의 첫 번째 코드 링크만 사용 (td[1] 차트 링크 제외)
        first_td = row.select_one("td")
        if not first_td:
            continue
        a_tag = first_td.select_one("a[href*='code=']")
        if not a_tag:
            continue
        code = a_tag["href"].split("code=")[-1].split("&")[0].zfill(6)
        tds = row.select("td")
        try:
            close = int(_naver_clean(tds[2].get_text())) if len(tds) > 2 else 0
            change_pct = float(_naver_clean(tds[4].get_text()) or "0") if len(tds) > 4 else 0.0
        except (ValueError, IndexError):
            continue
        if not close:
            continue
        db_entry = next((s for s in _stock_db if s["c"] == code), None)
        market = db_entry["m"] if db_entry else "KOSPI"
        items.append({
            "c": code,
            "n": a_tag.get_text(strip=True),
            "m": market,
            "close": close,
            "changePct": change_pct,
        })
    return items


@app.get("/api/discover/theme/{no}")
async def discover_theme_detail(no: str):
    """테마 소속 종목 조회."""
    if no in _theme_detail_cache:
        ts, data = _theme_detail_cache[no]
        if time.time() - ts < THEME_DETAIL_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        items = await loop.run_in_executor(None, lambda: _fetch_theme_stocks(no))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"테마 종목 조회 실패: {e}")
    out = {"no": no, "items": items}
    _theme_detail_cache[no] = (time.time(), out)
    return out


def _classify_dart(title: str) -> str:
    t = title
    if any(k in t for k in ("잠정실적", "손익구조", "매출액또는손익")): return "잠정실적"
    if "주주총회" in t: return "주주총회"
    if "유상증자" in t: return "유상증자"
    if "무상증자" in t: return "무상증자"
    if "자기주식취득" in t: return "자사주매입"
    if "자기주식처분" in t: return "자사주처분"
    if "합병" in t: return "합병"
    if "분할" in t: return "분할"
    if "전환사채" in t: return "전환사채"
    if "신주인수권부사채" in t or "BW" in t: return "신주인수권"
    if "배당" in t: return "배당"
    if "최대주주" in t: return "최대주주변경"
    return "주요공시"


def _fetch_earnings_dart(dart_key: str) -> list:
    """DART 주요사항보고서(E타입) — 잠정실적·주주총회·유증·자사주 등 투자 핵심 공시.
    주주총회 소집공고는 회의 2~4주 전 사전 공시되므로 30일치 조회로 예정 이벤트 파악 가능.
    """
    today = datetime.now()
    bgn = (today - timedelta(days=30)).strftime("%Y%m%d")
    end = today.strftime("%Y%m%d")
    name_map = {s["c"]: s["n"] for s in _stock_db}
    market_map = {s["c"]: s["m"] for s in _stock_db}
    items = []
    seen = set()
    try:
        params = {
            "crtfc_key": dart_key,
            "bgn_de": bgn,
            "end_de": end,
            "pblntf_ty": "E",   # 주요사항보고서
            "page_count": 100,
        }
        res = requests.get("https://opendart.fss.or.kr/api/list.json", params=params, timeout=10)
        res.raise_for_status()
        j = res.json()
        if j.get("status") == "000":
            for d in j.get("list", []):
                rcpt = d.get("rcept_no", "")
                if rcpt in seen:
                    continue
                seen.add(rcpt)
                code = str(d.get("stock_code", "")).zfill(6)
                if not code or code == "000000":
                    continue
                title = d.get("report_nm", "")
                items.append({
                    "date": d.get("rcept_dt", ""),
                    "c": code,
                    "n": d.get("corp_name", name_map.get(code, code)),
                    "m": market_map.get(code, ""),
                    "title": title,
                    "tag": _classify_dart(title),
                    "url": f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcpt}",
                })
    except Exception:
        pass
    items.sort(key=lambda x: x["date"], reverse=True)
    return items[:80]


@app.get("/api/discover/earnings")
async def discover_earnings():
    """사업/반기/분기보고서 최근 제출 목록 (DART API 필요)."""
    key = os.environ.get("DART_API_KEY") or DART_API_KEY
    if not key:
        raise HTTPException(status_code=503, detail="DART_API_KEY 미설정 — dart.fss.or.kr에서 무료 발급 후 환경변수에 추가하세요")
    if "earnings" in _earnings_cache:
        ts, data = _earnings_cache["earnings"]
        if time.time() - ts < EARNINGS_CACHE_TTL:
            return data
    loop = asyncio.get_event_loop()
    try:
        items = await loop.run_in_executor(None, lambda: _fetch_earnings_dart(key))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DART API 호출 실패: {e}")
    out = {"items": items}
    _earnings_cache["earnings"] = (time.time(), out)
    return out


# 정적 파일은 마지막에 마운트 (API 라우트보다 후순위)
app.mount("/", StaticFiles(directory="static", html=True), name="static")
