import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import { Plus, Trash2, Search, TrendingUp, TrendingDown, Loader2, X, RefreshCw, ArrowLeft, AlertCircle } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

// ─── KRX 전종목 마스터 (코스피 + 코스닥) ─────────────────────
// 출처: KRX 정보데이터시스템 (주요 종목 수록)
const MASTER = [
  // ── KOSPI ──────────────────────────────────────────────────
  {c:'005930',n:'삼성전자',m:'KOSPI'},
  {c:'000660',n:'SK하이닉스',m:'KOSPI'},
  {c:'373220',n:'LG에너지솔루션',m:'KOSPI'},
  {c:'207940',n:'삼성바이오로직스',m:'KOSPI'},
  {c:'005935',n:'삼성전자우',m:'KOSPI'},
  {c:'005380',n:'현대차',m:'KOSPI'},
  {c:'000270',n:'기아',m:'KOSPI'},
  {c:'068270',n:'셀트리온',m:'KOSPI'},
  {c:'005490',n:'POSCO홀딩스',m:'KOSPI'},
  {c:'035420',n:'NAVER',m:'KOSPI'},
  {c:'105560',n:'KB금융',m:'KOSPI'},
  {c:'055550',n:'신한지주',m:'KOSPI'},
  {c:'012330',n:'현대모비스',m:'KOSPI'},
  {c:'028260',n:'삼성물산',m:'KOSPI'},
  {c:'329180',n:'HD현대중공업',m:'KOSPI'},
  {c:'267260',n:'HD현대일렉트릭',m:'KOSPI'},
  {c:'009540',n:'HD한국조선해양',m:'KOSPI'},
  {c:'267250',n:'HD현대',m:'KOSPI'},
  {c:'010620',n:'HD현대미포',m:'KOSPI'},
  {c:'042660',n:'한화오션',m:'KOSPI'},
  {c:'010130',n:'고려아연',m:'KOSPI'},
  {c:'034730',n:'SK',m:'KOSPI'},
  {c:'015760',n:'한국전력',m:'KOSPI'},
  {c:'051910',n:'LG화학',m:'KOSPI'},
  {c:'006400',n:'삼성SDI',m:'KOSPI'},
  {c:'003670',n:'포스코퓨처엠',m:'KOSPI'},
  {c:'011200',n:'HMM',m:'KOSPI'},
  {c:'086790',n:'하나금융지주',m:'KOSPI'},
  {c:'316140',n:'우리금융지주',m:'KOSPI'},
  {c:'035720',n:'카카오',m:'KOSPI'},
  {c:'352820',n:'하이브',m:'KOSPI'},
  {c:'017670',n:'SK텔레콤',m:'KOSPI'},
  {c:'030200',n:'KT',m:'KOSPI'},
  {c:'032830',n:'삼성생명',m:'KOSPI'},
  {c:'003550',n:'LG',m:'KOSPI'},
  {c:'066570',n:'LG전자',m:'KOSPI'},
  {c:'009150',n:'삼성전기',m:'KOSPI'},
  {c:'018260',n:'삼성에스디에스',m:'KOSPI'},
  {c:'010950',n:'S-Oil',m:'KOSPI'},
  {c:'096770',n:'SK이노베이션',m:'KOSPI'},
  {c:'034020',n:'두산에너빌리티',m:'KOSPI'},
  {c:'241560',n:'두산밥캣',m:'KOSPI'},
  {c:'000810',n:'삼성화재',m:'KOSPI'},
  {c:'024110',n:'기업은행',m:'KOSPI'},
  {c:'004020',n:'현대제철',m:'KOSPI'},
  {c:'010140',n:'삼성중공업',m:'KOSPI'},
  {c:'047810',n:'한국항공우주',m:'KOSPI'},
  {c:'012450',n:'한화에어로스페이스',m:'KOSPI'},
  {c:'272210',n:'한화시스템',m:'KOSPI'},
  {c:'064350',n:'현대로템',m:'KOSPI'},
  {c:'005830',n:'DB손해보험',m:'KOSPI'},
  {c:'000720',n:'현대건설',m:'KOSPI'},
  {c:'047040',n:'대우건설',m:'KOSPI'},
  {c:'375500',n:'DL이앤씨',m:'KOSPI'},
  {c:'028050',n:'삼성E&A',m:'KOSPI'},
  {c:'011170',n:'롯데케미칼',m:'KOSPI'},
  {c:'011070',n:'LG이노텍',m:'KOSPI'},
  {c:'032640',n:'LG유플러스',m:'KOSPI'},
  {c:'139480',n:'이마트',m:'KOSPI'},
  {c:'023530',n:'롯데쇼핑',m:'KOSPI'},
  {c:'097950',n:'CJ제일제당',m:'KOSPI'},
  {c:'271560',n:'오리온',m:'KOSPI'},
  {c:'004990',n:'롯데지주',m:'KOSPI'},
  {c:'161390',n:'한국타이어앤테크놀로지',m:'KOSPI'},
  {c:'000150',n:'두산',m:'KOSPI'},
  {c:'402340',n:'SK스퀘어',m:'KOSPI'},
  {c:'128940',n:'한미약품',m:'KOSPI'},
  {c:'009830',n:'한화솔루션',m:'KOSPI'},
  {c:'003490',n:'대한항공',m:'KOSPI'},
  {c:'180640',n:'한진칼',m:'KOSPI'},
  {c:'079550',n:'LIG넥스원',m:'KOSPI'},
  {c:'361610',n:'SK아이이테크놀로지',m:'KOSPI'},
  {c:'326030',n:'SK바이오팜',m:'KOSPI'},
  {c:'302440',n:'SK바이오사이언스',m:'KOSPI'},
  {c:'001040',n:'CJ',m:'KOSPI'},
  {c:'035250',n:'강원랜드',m:'KOSPI'},
  {c:'021240',n:'코웨이',m:'KOSPI'},
  {c:'007310',n:'오뚜기',m:'KOSPI'},
  {c:'004370',n:'농심',m:'KOSPI'},
  {c:'033780',n:'KT&G',m:'KOSPI'},
  {c:'090430',n:'아모레퍼시픽',m:'KOSPI'},
  {c:'161890',n:'한국콜마',m:'KOSPI'},
  {c:'051900',n:'LG생활건강',m:'KOSPI'},
  {c:'006800',n:'미래에셋증권',m:'KOSPI'},
  {c:'016360',n:'삼성증권',m:'KOSPI'},
  {c:'005940',n:'NH투자증권',m:'KOSPI'},
  {c:'008770',n:'호텔신라',m:'KOSPI'},
  {c:'069960',n:'현대백화점',m:'KOSPI'},
  {c:'004170',n:'신세계',m:'KOSPI'},
  {c:'009240',n:'한샘',m:'KOSPI'},
  {c:'001800',n:'오리온홀딩스',m:'KOSPI'},
  {c:'010060',n:'OCI홀딩스',m:'KOSPI'},
  {c:'000100',n:'유한양행',m:'KOSPI'},
  {c:'185750',n:'종근당',m:'KOSPI'},
  {c:'003230',n:'삼양식품',m:'KOSPI'},
  {c:'036460',n:'한국가스공사',m:'KOSPI'},
  {c:'030000',n:'제일기획',m:'KOSPI'},
  {c:'000670',n:'영풍',m:'KOSPI'},
  {c:'001430',n:'세아베스틸지주',m:'KOSPI'},
  {c:'002380',n:'KCC',m:'KOSPI'},
  {c:'006360',n:'GS건설',m:'KOSPI'},
  {c:'078930',n:'GS',m:'KOSPI'},
  {c:'071050',n:'한국금융지주',m:'KOSPI'},
  {c:'086280',n:'현대글로비스',m:'KOSPI'},
  {c:'015020',n:'이랜드리테일',m:'KOSPI'},
  {c:'003600',n:'SK케미칼',m:'KOSPI'},
  {c:'011790',n:'SKC',m:'KOSPI'},
  {c:'025540',n:'한국단자공업',m:'KOSPI'},
  {c:'000210',n:'DL',m:'KOSPI'},
  {c:'093370',n:'후성',m:'KOSPI'},
  {c:'004800',n:'효성',m:'KOSPI'},
  {c:'298050',n:'효성첨단소재',m:'KOSPI'},
  {c:'298040',n:'효성중공업',m:'KOSPI'},
  {c:'092780',n:'동양생명',m:'KOSPI'},
  {c:'138930',n:'BNK금융지주',m:'KOSPI'},
  {c:'175330',n:'JB금융지주',m:'KOSPI'},
  {c:'029780',n:'삼성카드',m:'KOSPI'},
  {c:'071055',n:'한국금융지주우',m:'KOSPI'},
  {c:'001570',n:'금양',m:'KOSPI'},
  {c:'014820',n:'동원시스템즈',m:'KOSPI'},
  {c:'001040',n:'CJ',m:'KOSPI'},
  {c:'000080',n:'하이트진로',m:'KOSPI'},
  {c:'000020',n:'동화약품',m:'KOSPI'},
  {c:'002790',n:'아모레G',m:'KOSPI'},
  {c:'008350',n:'남선알미늄',m:'KOSPI'},
  {c:'001680',n:'대상',m:'KOSPI'},
  {c:'007070',n:'GS리테일',m:'KOSPI'},
  {c:'011150',n:'CJ씨푸드',m:'KOSPI'},
  // ── KOSDAQ ─────────────────────────────────────────────────
  {c:'247540',n:'에코프로비엠',m:'KOSDAQ'},
  {c:'086520',n:'에코프로',m:'KOSDAQ'},
  {c:'091990',n:'셀트리온헬스케어',m:'KOSDAQ'},
  {c:'196170',n:'알테오젠',m:'KOSDAQ'},
  {c:'028300',n:'HLB',m:'KOSDAQ'},
  {c:'068760',n:'셀트리온제약',m:'KOSDAQ'},
  {c:'141080',n:'리가켐바이오',m:'KOSDAQ'},
  {c:'263750',n:'펄어비스',m:'KOSDAQ'},
  {c:'293490',n:'카카오게임즈',m:'KOSDAQ'},
  {c:'112040',n:'위메이드',m:'KOSDAQ'},
  {c:'194480',n:'데브시스터즈',m:'KOSDAQ'},
  {c:'041510',n:'SM',m:'KOSDAQ'},
  {c:'122870',n:'와이지엔터테인먼트',m:'KOSDAQ'},
  {c:'035900',n:'JYP Ent.',m:'KOSDAQ'},
  {c:'035760',n:'CJ ENM',m:'KOSDAQ'},
  {c:'067310',n:'하나마이크론',m:'KOSDAQ'},
  {c:'058470',n:'리노공업',m:'KOSDAQ'},
  {c:'240810',n:'원익IPS',m:'KOSDAQ'},
  {c:'357780',n:'솔브레인',m:'KOSDAQ'},
  {c:'403870',n:'HPSP',m:'KOSDAQ'},
  {c:'042700',n:'한미반도체',m:'KOSDAQ'},
  {c:'039030',n:'이오테크닉스',m:'KOSDAQ'},
  {c:'277810',n:'레인보우로보틱스',m:'KOSDAQ'},
  {c:'376300',n:'디어유',m:'KOSDAQ'},
  {c:'214150',n:'클래시스',m:'KOSDAQ'},
  {c:'236200',n:'에스앤에스텍',m:'KOSDAQ'},
  {c:'348370',n:'엔켐',m:'KOSDAQ'},
  {c:'178920',n:'PI첨단소재',m:'KOSDAQ'},
  {c:'084370',n:'유진테크',m:'KOSDAQ'},
  {c:'095340',n:'ISC',m:'KOSDAQ'},
  {c:'036930',n:'주성엔지니어링',m:'KOSDAQ'},
  {c:'145020',n:'휴젤',m:'KOSDAQ'},
  {c:'298380',n:'에이비엘바이오',m:'KOSDAQ'},
  {c:'328130',n:'루닛',m:'KOSDAQ'},
  {c:'253450',n:'스튜디오드래곤',m:'KOSDAQ'},
  {c:'097520',n:'엠씨넥스',m:'KOSDAQ'},
  {c:'131970',n:'두산테스나',m:'KOSDAQ'},
  {c:'108860',n:'셀바스AI',m:'KOSDAQ'},
  {c:'950140',n:'잉글우드랩',m:'KOSDAQ'},
  {c:'214940',n:'에스티팜',m:'KOSDAQ'},
  {c:'145720',n:'덴티움',m:'KOSDAQ'},
  {c:'041960',n:'블루베리NFT',m:'KOSDAQ'},
  {c:'089030',n:'테크윙',m:'KOSDAQ'},
  {c:'119860',n:'커넥트웨이브',m:'KOSDAQ'},
  {c:'122990',n:'와이솔',m:'KOSDAQ'},
  {c:'078600',n:'대주전자재료',m:'KOSDAQ'},
  {c:'096530',n:'씨젠',m:'KOSDAQ'},
  {c:'054540',n:'삼영전자',m:'KOSDAQ'},
  {c:'033160',n:'엠케이트렌드',m:'KOSDAQ'},
  {c:'053800',n:'안랩',m:'KOSDAQ'},
  {c:'060280',n:'큐렉소',m:'KOSDAQ'},
  {c:'950220',n:'나노씨엠에스',m:'KOSDAQ'},
  {c:'060310',n:'3S',m:'KOSDAQ'},
  {c:'950160',n:'코오롱티슈진',m:'KOSDAQ'},
  {c:'950130',n:'엑세스바이오',m:'KOSDAQ'},
  {c:'066970',n:'엘앤에프',m:'KOSDAQ'},
  {c:'007390',n:'네이처셀',m:'KOSDAQ'},
  {c:'036540',n:'SCI평가정보',m:'KOSDAQ'},
  {c:'065350',n:'신성델타테크',m:'KOSDAQ'},
  {c:'048260',n:'오스템임플란트',m:'KOSDAQ'},
  {c:'045300',n:'성우하이텍',m:'KOSDAQ'},
  {c:'101000',n:'오텍',m:'KOSDAQ'},
  {c:'064090',n:'웨이브일렉트로',m:'KOSDAQ'},
  {c:'064760',n:'티씨케이',m:'KOSDAQ'},
  {c:'039610',n:'화성산업',m:'KOSDAQ'},
  {c:'013030',n:'하이록코리아',m:'KOSDAQ'},
  {c:'950210',n:'프레스티지바이오파마',m:'KOSDAQ'},
  {c:'199800',n:'툴젠',m:'KOSDAQ'},
  {c:'347860',n:'알체라',m:'KOSDAQ'},
  {c:'317530',n:'캐리소프트',m:'KOSDAQ'},
  {c:'348150',n:'에스바이오메딕스',m:'KOSDAQ'},
  {c:'950170',n:'JTC',m:'KOSDAQ'},
  {c:'226340',n:'본느',m:'KOSDAQ'},
  {c:'039200',n:'오스코텍',m:'KOSDAQ'},
  {c:'083790',n:'크리스탈지노믹스',m:'KOSDAQ'},
  {c:'950150',n:'코오롱생명과학',m:'KOSDAQ'},
];

// ─── 상수 ──────────────────────────────────────────────────────
const STORAGE_KEY = 'stocks:watchlist:v3';
const CACHE_PREFIX = 'stocks:price:';
const CACHE_TTL = 30 * 60 * 1000;

// ─── 유틸 ──────────────────────────────────────────────────────
const fp = (v) => v == null || isNaN(v) ? '-' : new Intl.NumberFormat('ko-KR').format(Math.round(v));
const fChange = (change, pct) => {
  if (change == null) return '';
  const s = change > 0 ? '+' : '';
  return `${s}${fp(change)} (${s}${Number(pct).toFixed(2)}%)`;
};
const cc = (v) => v > 0 ? 'text-red-500' : v < 0 ? 'text-blue-500' : 'text-stone-400';

// ─── JSON 추출 ──────────────────────────────────────────────────
const extractJSON = (text) => {
  if (!text) return null;
  const cb = text.match(/```(?:json)?\s*(\{[\s\S]*?\})\s*```/);
  if (cb) { try { return JSON.parse(cb[1]); } catch (e) {} }
  const cands = [];
  for (let i = 0; i < text.length; i++) {
    if (text[i] !== '{') continue;
    let d = 0, inS = false, esc = false;
    for (let j = i; j < text.length; j++) {
      const c = text[j];
      if (esc) { esc = false; continue; }
      if (c === '\\') { esc = true; continue; }
      if (c === '"') { inS = !inS; continue; }
      if (inS) continue;
      if (c === '{') d++;
      else if (c === '}') { d--; if (d === 0) { cands.push(text.slice(i, j + 1)); break; } }
    }
  }
  cands.sort((a, b) => {
    const s = (x) => (x.includes('"prices"') ? 10000 : 0) + x.length;
    return s(b) - s(a);
  });
  for (const c of cands) { try { const p = JSON.parse(c); if (p && typeof p === 'object') return p; } catch (e) {} }
  return null;
};

// ─── API: 종가 조회 ────────────────────────────────────────────
const fetchPrices = async (stock) => {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      model: 'claude-sonnet-4-20250514',
      max_tokens: 4000,
      messages: [{
        role: 'user',
        content: `네이버 금융(finance.naver.com)에서 종목코드 ${stock.c} (${stock.n})의 일별 시세를 검색해서 최근 거래일 25일치 종가 데이터를 가져와줘.

조사 후 아래 JSON만 출력. 설명/분석/코드블록 없이 JSON만.

{"prices":[{"date":"2026-05-09","close":75000,"change":-500,"changePct":-0.66}],"source":"네이버 금융"}

규칙:
- prices: 최근일부터 과거순 최대 25개 거래일
- close: 원 단위 정수, change: 전일대비(양수/음수), changePct: 등락률%(소수점2자리)
- source: 출처명, JSON 외 다른 텍스트 금지`
      }],
      tools: [{ type: 'web_search_20250305', name: 'web_search' }],
    }),
  });
  if (!res.ok) throw new Error(`API 오류 (${res.status})`);
  const data = await res.json();
  const text = data.content.filter((b) => b.type === 'text').map((b) => b.text).join('\n');
  if (!text.trim()) throw new Error('빈 응답. 다시 시도해주세요.');
  const parsed = extractJSON(text);
  if (!parsed) throw new Error(`JSON 파싱 실패: "${text.slice(0, 150).replace(/\s+/g,' ')}"`);
  if (!parsed.prices?.length) throw new Error('종가 데이터가 비어있습니다.');
  parsed.prices = parsed.prices
    .filter((p) => p?.date && p.close != null)
    .map((p) => ({ date: String(p.date), close: Number(p.close), change: Number(p.change)||0, changePct: Number(p.changePct)||0 }))
    .filter((p) => !isNaN(p.close));
  if (!parsed.prices.length) throw new Error('유효한 종가 데이터가 없습니다.');
  return parsed;
};

// ══════════════════════════════════════════════════════════════
export default function StockTracker() {
  const [watchlist, setWatchlist] = useState([]);
  const [ready, setReady] = useState(false);
  const [screen, setScreen] = useState('list'); // 'list' | 'search' | 'detail'
  const [query, setQuery] = useState('');
  const [selected, setSelected] = useState(null);
  const [priceData, setPriceData] = useState(null);
  const [priceSource, setPriceSource] = useState('');
  const [fetchedAt, setFetchedAt] = useState(null);
  const [priceLoading, setPriceLoading] = useState(false);
  const [priceError, setPriceError] = useState('');
  const searchRef = useRef(null);

  // 로컬 즉시 검색 결과 (useMemo로 동기 계산)
  const searchResults = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return [];
    const watched = new Set(watchlist.map((s) => s.c));
    return MASTER.filter((s) =>
      s.n.toLowerCase().includes(q) || s.c.includes(q)
    ).map((s) => ({ ...s, added: watched.has(s.c) })).slice(0, 30);
  }, [query, watchlist]);

  // 초기 로드
  useEffect(() => {
    (async () => {
      try {
        const r = await window.storage.get(STORAGE_KEY);
        if (r?.value) setWatchlist(JSON.parse(r.value));
      } catch (e) {}
      setReady(true);
    })();
  }, []);

  // 검색 화면 오픈 시 포커스 + 초기화
  useEffect(() => {
    if (screen === 'search') {
      setQuery('');
      setTimeout(() => searchRef.current?.focus(), 80);
    }
  }, [screen]);

  // 관심종목 저장
  const save = async (list) => {
    setWatchlist(list);
    try { await window.storage.set(STORAGE_KEY, JSON.stringify(list)); } catch (e) {}
  };

  const addStock = async (s) => {
    if (watchlist.some((w) => w.c === s.c)) return;
    await save([...watchlist, { c: s.c, n: s.n, m: s.m }]);
  };

  const removeStock = async (code) => {
    await save(watchlist.filter((s) => s.c !== code));
    if (selected?.c === code) { setSelected(null); setScreen('list'); }
  };

  // 종가 조회
  const loadPrices = useCallback(async (stock, force = false) => {
    setPriceLoading(true); setPriceData(null); setPriceError(''); setPriceSource(''); setFetchedAt(null);
    const key = `${CACHE_PREFIX}${stock.c}`;
    if (!force) {
      try {
        const cached = await window.storage.get(key);
        if (cached?.value) {
          const { data, ts } = JSON.parse(cached.value);
          if (Date.now() - ts < CACHE_TTL) {
            setPriceData(data.prices); setPriceSource(data.source||''); setFetchedAt(ts);
            setPriceLoading(false); return;
          }
        }
      } catch (e) {}
    }
    try {
      const data = await fetchPrices(stock);
      const now = Date.now();
      setPriceData(data.prices); setPriceSource(data.source||''); setFetchedAt(now);
      try { await window.storage.set(key, JSON.stringify({ data, ts: now })); } catch (e) {}
    } catch (err) {
      setPriceError(err.message);
    } finally {
      setPriceLoading(false);
    }
  }, []);

  const openDetail = (stock) => {
    setSelected(stock); setPriceData(null); setPriceError('');
    setScreen('detail'); loadPrices(stock);
  };

  const chartData = priceData ? [...priceData].reverse() : [];
  const latest = priceData?.[0];

  const BASE = { fontFamily: 'system-ui, -apple-system, sans-serif' };
  const STYLE = <style>{`body{-webkit-tap-highlight-color:transparent}.mono{font-family:'JetBrains Mono',monospace}`}</style>;

  // ── 검색 화면 ────────────────────────────────────────────────
  if (screen === 'search') return (
    <div className="min-h-screen bg-white flex flex-col" style={BASE}>
      {STYLE}
      <header className="sticky top-0 z-10 bg-white border-b-2 border-stone-900 flex items-center gap-2 px-3 py-3">
        <button onClick={() => setScreen('list')} className="p-2 rounded active:bg-stone-100"><X size={22}/></button>
        <Search size={18} className="text-stone-400 shrink-0"/>
        <input
          ref={searchRef}
          type="text" value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="종목명 또는 종목코드"
          className="flex-1 outline-none text-base py-1"
        />
        {query && <button onClick={() => setQuery('')} className="p-1"><X size={16} className="text-stone-400"/></button>}
      </header>

      <div className="flex-1 overflow-y-auto">
        {!query.trim() && (
          <div className="p-10 text-center text-stone-400 text-sm leading-relaxed">
            종목명 또는 코드를 입력하세요<br/>
            <span className="text-xs text-stone-300">입력 즉시 검색됩니다</span>
          </div>
        )}

        {query.trim() && searchResults.length === 0 && (
          <div className="p-10 text-center text-stone-400 text-sm">
            '<b>{query}</b>' 검색 결과 없음
          </div>
        )}

        {searchResults.length > 0 && (
          <>
            <div className="px-4 py-2 mono text-[11px] text-stone-400 border-b border-stone-100">
              {searchResults.length}개 결과
            </div>
            <ul>
              {searchResults.map((s) => (
                <li key={s.c} className="border-b border-stone-100">
                  <div className="flex items-center px-4 py-4 gap-3">
                    <div className="flex-1 min-w-0">
                      <div className="font-bold">{s.n}</div>
                      <div className="mono text-xs text-stone-500 mt-0.5">{s.c} · {s.m}</div>
                    </div>
                    {s.added
                      ? <span className="mono text-xs text-stone-400 border border-stone-200 px-3 py-1.5 shrink-0">등록됨</span>
                      : <button
                          onClick={() => addStock(s)}
                          className="flex items-center gap-1 text-xs font-bold bg-stone-900 text-white px-3 py-1.5 active:bg-stone-700 shrink-0"
                        ><Plus size={13}/> 추가</button>
                    }
                  </div>
                </li>
              ))}
            </ul>
          </>
        )}
      </div>
    </div>
  );

  // ── 상세 화면 ────────────────────────────────────────────────
  if (screen === 'detail' && selected) return (
    <div className="min-h-screen bg-white" style={BASE}>
      {STYLE}
      <header className="sticky top-0 z-10 bg-white border-b-2 border-stone-900">
        <div className="px-3 py-3 flex items-center gap-2">
          <button onClick={() => setScreen('list')} className="p-2 rounded active:bg-stone-100"><ArrowLeft size={22}/></button>
          <div className="flex-1 min-w-0">
            <div className="font-bold text-base truncate">{selected.n}</div>
            <div className="mono text-xs text-stone-500">{selected.c} · {selected.m}</div>
          </div>
          <button onClick={() => loadPrices(selected, true)} disabled={priceLoading} className="p-2 rounded active:bg-stone-100 disabled:opacity-40">
            <RefreshCw size={20} className={priceLoading ? 'animate-spin' : ''}/>
          </button>
        </div>
      </header>

      <div className="p-4 space-y-4">
        {priceLoading && (
          <div className="border-2 border-stone-200 p-12 text-center">
            <Loader2 className="animate-spin mx-auto text-stone-400 mb-3" size={32}/>
            <div className="text-sm font-bold text-stone-500">시세 조회 중...</div>
            <div className="text-xs text-stone-400 mt-1">웹 검색 중 (10~20초)</div>
          </div>
        )}

        {priceError && !priceLoading && (
          <div className="bg-red-50 border-2 border-red-500 p-4">
            <div className="flex items-center gap-2 text-red-700 font-bold text-sm mb-1"><AlertCircle size={16}/> 조회 실패</div>
            <div className="text-xs text-red-600 leading-relaxed break-all">{priceError}</div>
            <button onClick={() => loadPrices(selected, true)} className="mt-3 text-xs font-bold bg-red-600 text-white px-3 py-2 active:bg-red-700">다시 시도</button>
          </div>
        )}

        {latest && !priceLoading && (
          <>
            <div className="bg-stone-900 text-white p-5">
              <div className="text-xs text-stone-400 tracking-widest mb-2">LATEST CLOSE</div>
              <div className="mono text-4xl font-black mb-2">₩{fp(latest.close)}</div>
              <div className={`mono text-sm font-bold flex items-center gap-1 ${cc(latest.change)}`}>
                {latest.change > 0 ? <TrendingUp size={14}/> : latest.change < 0 ? <TrendingDown size={14}/> : null}
                {fChange(latest.change, latest.changePct)}
              </div>
              <div className="mono text-xs text-stone-500 mt-3">{latest.date}</div>
            </div>

            {chartData.length > 1 && (
              <div className="border-2 border-stone-900 p-4">
                <div className="text-xs font-bold tracking-widest text-stone-500 mb-3">종가 추이 · {chartData.length}일</div>
                <ResponsiveContainer width="100%" height={200}>
                  <LineChart data={chartData} margin={{ top:5, right:5, left:-20, bottom:0 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e7e5e4"/>
                    <XAxis dataKey="date" tick={{fontSize:9,fontFamily:'JetBrains Mono'}} stroke="#78716c" tickFormatter={(v)=>v?.slice(5)}/>
                    <YAxis tick={{fontSize:9,fontFamily:'JetBrains Mono'}} stroke="#78716c" domain={['auto','auto']} tickFormatter={(v)=>fp(v)}/>
                    <Tooltip contentStyle={{backgroundColor:'#1c1917',border:'none',color:'#fff',fontFamily:'JetBrains Mono',fontSize:11}} formatter={(v)=>[`₩${fp(v)}`,'종가']}/>
                    <Line type="monotone" dataKey="close" stroke="#1c1917" strokeWidth={2} dot={false} activeDot={{r:5,fill:'#dc2626'}}/>
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}

            <div className="border-2 border-stone-900">
              <div className="border-b-2 border-stone-900 px-4 py-3 flex justify-between items-center">
                <div className="text-xs font-bold tracking-widest">일별 종가</div>
                <div className="mono text-xs text-stone-400">최신순</div>
              </div>
              <ul>
                {priceData.map((row, i) => (
                  <li key={i} className={`px-4 py-3 flex items-center justify-between ${i !== priceData.length-1 ? 'border-b border-stone-100':''}`}>
                    <div className="mono text-sm text-stone-500">{row.date}</div>
                    <div className="text-right">
                      <div className="mono text-base font-bold">₩{fp(row.close)}</div>
                      <div className={`mono text-xs font-bold ${cc(row.change)}`}>{fChange(row.change, row.changePct)}</div>
                    </div>
                  </li>
                ))}
              </ul>
            </div>

            {(priceSource || fetchedAt) && (
              <div className="text-center mono text-xs text-stone-400 pb-4 leading-relaxed">
                {priceSource && <div>출처: {priceSource}</div>}
                {fetchedAt && <div>조회: {new Date(fetchedAt).toLocaleTimeString('ko-KR')} · 30분 캐시</div>}
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );

  // ── 메인 리스트 화면 ─────────────────────────────────────────
  return (
    <div className="min-h-screen bg-stone-50" style={BASE}>
      {STYLE}
      <header className="sticky top-0 z-10 bg-white border-b-2 border-stone-900">
        <div className="px-4 py-4 flex items-center justify-between">
          <div>
            <div className="text-[10px] tracking-[0.25em] text-stone-400 mb-0.5">KOREA STOCK</div>
            <h1 className="text-xl font-black">관심종목</h1>
          </div>
          <button
            onClick={() => setScreen('search')}
            className="flex items-center gap-1.5 text-xs font-bold bg-stone-900 text-white px-4 py-2.5 active:bg-stone-700"
          >
            <Search size={14}/> 종목 추가
          </button>
        </div>
      </header>

      <main className="p-4">
        {!ready
          ? <div className="pt-16 text-center"><Loader2 className="animate-spin mx-auto text-stone-300" size={24}/></div>
          : watchlist.length === 0
            ? (
              <div className="mt-8 border-2 border-dashed border-stone-300 bg-white p-12 text-center">
                <Search className="mx-auto text-stone-300 mb-3" size={36}/>
                <p className="text-stone-500 text-sm leading-loose">
                  관심종목이 없습니다.<br/>
                  <span className="font-bold">종목 추가</span> 버튼으로 검색해서 추가하세요.
                </p>
              </div>
            )
            : (
              <>
                <div className="mono text-[11px] text-stone-400 mb-3">{watchlist.length}개 종목 등록됨</div>
                <ul className="space-y-2">
                  {watchlist.map((s) => (
                    <li key={s.c}>
                      <div className="flex items-stretch bg-white border-2 border-stone-200 active:border-stone-900 transition-colors">
                        <button className="flex-1 text-left px-4 py-4 min-w-0" onClick={() => openDetail(s)}>
                          <div className="font-bold text-base truncate">{s.n}</div>
                          <div className="mono text-xs text-stone-500 mt-0.5">{s.c} · {s.m}</div>
                        </button>
                        <button onClick={() => removeStock(s.c)} className="px-4 border-l border-stone-100 text-stone-300 active:text-red-600 active:bg-red-50">
                          <Trash2 size={16}/>
                        </button>
                      </div>
                    </li>
                  ))}
                </ul>
                <div className="mono text-[10px] text-stone-400 text-center mt-6">종목을 탭하면 종가 조회</div>
              </>
            )
        }
      </main>
    </div>
  );
}
