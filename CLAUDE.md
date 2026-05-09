# CLAUDE.md

이 파일은 이 저장소에서 작업하는 Claude Code(claude.ai/code)에게 안내를 제공합니다.

## 프로젝트 개요

한국 주식(KOSPI & KOSDAQ)을 추적하는 FastAPI 웹 애플리케이션입니다. pykrx를 통해 KRX(한국거래소)에서 직접 25일치 종가 이력을 가져옵니다. API 키 불필요, 응답속도 1~3초.

## 앱 실행 방법

```bash
pip install -r requirements.txt
uvicorn main:app --reload
# → http://localhost:8000
```

빌드 단계 없음. `static/index.html`이 CDN(React, Recharts, Tailwind)으로 동작합니다.

## 아키텍처

**백엔드 (`main.py`):** FastAPI 앱. 주요 엔드포인트:
- `GET /api/stocks` — MASTER 전체 목록 반환
- `GET /api/stocks/search?q=` — 종목명·코드 검색
- `GET /api/prices/{code}` — pykrx로 KRX에서 최근 25거래일 종가 조회. 서버 메모리에 30분 TTL 캐시.

pykrx는 60 캘린더일 범위를 조회한 뒤 `.tail(25)`로 최근 거래일만 추출합니다. 등락률·전일대비는 `종가.diff()`로 직접 계산합니다.

**프론트엔드 (`static/index.html`):** 빌드 없는 React (CDN + Babel Standalone). 세 화면 — `'list'` → `'search'` → `'detail'` — 을 `screen` 상태로 전환합니다. 종가 데이터는 `localStorage`에 30분 클라이언트 캐시합니다 (키: `price:{code}`). 관심목록은 `stocks:watchlist:v3`로 영속합니다.

`stock_tracker.jsx`는 이전 버전(Claude API 직접 호출 방식)으로, 현재 사용하지 않습니다.

## 주요 특성

- 종목 코드는 6자리 문자열입니다 (예: 삼성전자 `'005930'`).
- 모든 UI 텍스트는 한국어입니다.
- pykrx 첫 호출 시 KRX 티커 정보를 내려받아 수 초 걸릴 수 있습니다. 이후 호출은 빠릅니다.
