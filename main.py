from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import FinanceDataReader as fdr
from datetime import datetime, timedelta
import asyncio
import time
import pandas as pd
import requests

_stock_db: list[dict] = []

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

@asynccontextmanager
async def lifespan(app: FastAPI):
    global _stock_db
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

    yield


app = FastAPI(title="한국 주식 트래커", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET"], allow_headers=["*"])

# 서버 메모리 캐시 (30분 TTL)
_cache: dict = {}
CACHE_TTL = 1800

# 기간 → 조회할 캘린더일 수 (거래일 여유분 포함)
PERIOD_CAL_DAYS = {"1M": 45, "3M": 110, "6M": 210, "1Y": 400}

_current_cache: dict = {}
CURRENT_CACHE_TTL = 30  # 30초

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
