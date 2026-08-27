const http = require("http");
const https = require("https");
const WebSocket = require("ws");
const fs = require("fs");
const path = require("path");

const PORT = process.env.PORT || 8787;
const RELAY_SECRET = process.env.RELAY_SECRET;
const KIWOOM_REAL_HOST = "api.kiwoom.com";

// 웹소켓 실시간 시세용 (지수 등). 앱키/시크릿이 없으면 웹소켓 기능만 비활성화되고
// 기존 REST 중계는 그대로 동작함 (하위호환 - 환경변수 추가 전에도 안 죽음)
const APP_KEY = process.env.KIWOOM_APP_KEY_REAL;
const APP_SECRET = process.env.KIWOOM_APP_SECRET_REAL;
const WS_URL = "wss://api.kiwoom.com:10000/api/dostk/websocket";

if (!RELAY_SECRET) {
  console.error("RELAY_SECRET 환경변수가 없습니다.");
  process.exit(1);
}

// ---------- 실시간 시세 캐시 (웹소켓으로 받은 최신값을 메모리에 보관) ----------
// Worker가 조회하면 이 캐시를 즉시 반환 -> 키움 TR 호출 없이 실시간에 가까운 값 제공
const realtimeCache = {
  index: {}, // { "001": {price, rate, time, updatedAt}, "101": {...} }
  stock: {}, // { "005930": {price, rate, volume, cntrStr, time, updatedAt}, ... }
  // 조건검색: 현재 조건을 만족하는 종목 집합 (실시간 편입/이탈로 갱신됨)
  condition: {
    seq: null,
    name: null, // 조건식 이름 (CNSRLST 응답에서 확보) - 자동편입 라벨 등에 표시용
    codes: [], // 현재 조건 만족 종목코드 목록
    lastEventAt: null,
    events: [], // 최근 편입/이탈 이벤트 (최대 50개, 디버깅/확인용)
    history: [], // 편입 이력 (최대 60개) - 조건에서 빠져나가도 유지되어 놓치지 않게 함
  },
};

// 감시할 조건식 번호. 환경변수로 지정 (미설정이면 조건검색 기능 비활성화)
const CONDITION_SEQ = process.env.KIWOOM_CONDITION_SEQ || "";
const WORKER_URL = process.env.WORKER_URL || "https://kiwoomapi.usbkr.workers.dev";
const ADMIN_KEY = process.env.ADMIN_KEY || "";

// ---------- 실시간가 로컬 파일 스냅샷 (VM 재시작 시 즉시 복구용) ----------
// 웹소켓 재연결 전까지는 값이 비어서 손익판단/화면이 잠깐 비는데, 재시작 직전 스냅샷을
// 먼저 메모리에 올려두면 재연결될 때까지의 공백을 직전 값으로 메꿀 수 있음(참고용, 신선도는 낮음).
const SNAPSHOT_PATH = path.join(__dirname, "realtime-snapshot.json");
const SNAPSHOT_INTERVAL_MS = 15000;

function saveRealtimeSnapshot() {
  try {
    const stockCount = Object.keys(realtimeCache.stock).length;
    if (stockCount === 0) return; // 빈 값으로 덮어쓰면 마지막 유효 스냅샷이 소실됨 - 값 있을 때만 저장
    const data = {
      savedAt: new Date().toISOString(),
      index: realtimeCache.index,
      stock: realtimeCache.stock,
    };
    fs.writeFileSync(SNAPSHOT_PATH, JSON.stringify(data));
  } catch (e) {
    console.log("스냅샷 저장 실패: " + e.message);
  }
}

function loadRealtimeSnapshot() {
  try {
    if (!fs.existsSync(SNAPSHOT_PATH)) return;
    const raw = JSON.parse(fs.readFileSync(SNAPSHOT_PATH, "utf8"));
    const ageMs = Date.now() - new Date(raw.savedAt).getTime();
    // 장중엔 10분 넘게 지난 값은 버림(그 사이 장 상황이 바뀌었을 것). 장마감 후엔 마지막 장중 값이
    // 여전히 유효한 "현재가"이므로 신선도 제한 없이 그대로 복구(다음 장 시작 전까지 안 바뀌는 데이터).
    if (isMarketHoursKST() && ageMs > 10 * 60 * 1000) return;
    if (raw.index) Object.assign(realtimeCache.index, raw.index);
    if (raw.stock) Object.assign(realtimeCache.stock, raw.stock);
    console.log(`실시간가 스냅샷 복구: 종목 ${Object.keys(raw.stock || {}).length}개 (${Math.round(ageMs / 1000)}초 전 값)`);
  } catch (e) {
    console.log("스냅샷 복구 실패: " + e.message);
  }
}

loadRealtimeSnapshot();
setInterval(saveRealtimeSnapshot, SNAPSHOT_INTERVAL_MS);

// 현재 구독 중인 종목코드 목록. Worker가 /realtime/subscribe 로 갱신하면 웹소켓에 재등록함.
// 키움 제한: 한 연결에서 등록 가능한 실시간 종목이 총 200개(실측 확인 - 그룹 합산 기준).
// 지수 2개 + 여유분을 빼고, 관심종목을 우선 배정한 뒤 남는 만큼만 리스트 종목에 씀.
const TOTAL_STOCK_LIMIT = 180; // 지수(2) + 조건검색 등 여유를 빼고 종목에 쓸 총량
const WATCH_RESERVED = 40; // 관심종목에 우선 배정할 최대 수
let subscribedStocks = []; // 관심종목 (그룹2)
let subscribedListStocks = []; // 화면 리스트 종목 (그룹3)

let ws = null;
let wsConnected = false;
let wsLoggedIn = false;
let wsReconnectDelay = 5000; // 재연결 대기 (실패 누적 시 늘어남, 최대 60초)
let wsLastMessageAt = 0;
let wsLastGroup9MessageAt = 0; // 그룹9(0D 호가잔량) 전용 최근 수신시각 - 전체 트래픽은 정상인데 이것만 죽는 부분장애 감지용
let wsLoginAt = 0; // 로그인 완료 시각 - 직후 구독 요청이 몰리는 것을 막는 데 씀

function parseSignedNumber(v) {
  // 키움 실시간 값은 "+6629.24" / "-118077" 형태로 부호가 붙어서 옴
  const n = parseFloat(String(v == null ? "" : v).replace(/[^0-9.\-]/g, ""));
  return isNaN(n) ? 0 : n;
}

function issueToken() {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify({
      grant_type: "client_credentials",
      appkey: APP_KEY,
      secretkey: APP_SECRET,
    });
    const req = https.request(
      {
        hostname: KIWOOM_REAL_HOST,
        path: "/oauth2/token",
        method: "POST",
        headers: {
          "Content-Type": "application/json;charset=UTF-8",
          "Content-Length": Buffer.byteLength(body),
        },
      },
      (res) => {
        let raw = "";
        res.on("data", (c) => (raw += c));
        res.on("end", () => {
          try {
            const d = JSON.parse(raw);
            if (!d.token) return reject(new Error("토큰 없음: " + raw.slice(0, 200)));
            resolve(d.token);
          } catch (e) {
            reject(new Error("토큰 응답 파싱 실패: " + raw.slice(0, 200)));
          }
        });
      }
    );
    req.on("error", reject);
    req.end(body);
  });
}

// 종목명 캐시 { code: name } - 조건검색은 종목코드만 주기 때문에 이름을 따로 조회해서 보관.
// 한 번 조회하면 계속 재사용(종목명은 바뀌지 않음).
const stockNameCache = {};
let nameFetchQueue = [];
let nameFetchRunning = false;

function queueNameFetch(codes) {
  for (const c of codes) {
    if (!stockNameCache[c] && !nameFetchQueue.includes(c)) nameFetchQueue.push(c);
  }
  runNameFetch();
}

function runNameFetch() {
  if (nameFetchRunning || !nameFetchQueue.length) return;
  nameFetchRunning = true;
  const code = nameFetchQueue.shift();

  issueTokenCached()
    .then((token) => kiwoomRest("/api/dostk/stkinfo", "ka10001", { stk_cd: code }, token))
    .then((data) => {
      const name = data && (data.stk_nm || data.stk_name);
      if (name) stockNameCache[code] = String(name).trim();
    })
    .catch(() => {})
    .finally(() => {
      nameFetchRunning = false;
      // 키움 TR 초당1건 제한 준수
      setTimeout(runNameFetch, 1100);
    });
}

// relay 내부에서 키움 REST를 직접 호출할 때 쓰는 헬퍼 (종목명 조회용)
function kiwoomRest(path, apiId, body, token) {
  return new Promise((resolve, reject) => {
    const payload = JSON.stringify(body);
    const req = https.request(
      {
        hostname: KIWOOM_REAL_HOST,
        path: path,
        method: "POST",
        headers: {
          "Content-Type": "application/json;charset=UTF-8",
          authorization: "Bearer " + token,
          "cont-yn": "N",
          "next-key": "",
          "api-id": apiId,
          "Content-Length": Buffer.byteLength(payload),
        },
      },
      (res) => {
        let raw = "";
        res.on("data", (c) => (raw += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(raw));
          } catch (e) {
            reject(new Error("파싱 실패"));
          }
        });
      }
    );
    req.on("error", reject);
    req.end(payload);
  });
}

// 토큰 캐시 (종목명 조회에 재사용 - 매번 발급하면 낭비)
let restToken = null;
let restTokenAt = 0;
function issueTokenCached() {
  if (restToken && Date.now() - restTokenAt < 3 * 60 * 60 * 1000) return Promise.resolve(restToken);
  return issueToken().then((t) => {
    restToken = t;
    restTokenAt = Date.now();
    return t;
  });
}

// ---------- 등락률 상위 종목 수집 (ka10027) - Worker collectAndStore 이전 ----------
// 원래 Worker의 2분 cron이 CPU시간 안에서 KOSPI/KOSDAQ 순차조회(1.1초 대기 포함)를 했는데,
// relay는 상시구동이라 이 대기가 부담 없음. relay가 수집+파싱까지 끝내고 결과 배열만
// Worker(/api/ingest/snapshots)로 POST -> Worker는 D1 insert만 수행(가벼움).
function kiwoomRankingUp(mrktTp, token) {
  const body = {
    mrkt_tp: mrktTp,
    sort_tp: "1",
    trde_qty_cnd: "0000",
    updown_incls: "1",
    stk_cnd: "0",
    crd_cnd: "0",
    pric_cnd: "0",
    trde_prica_cnd: "0",
    flu_cnd: "1",
    stex_tp: "3",
  };
  return kiwoomRest("/api/dostk/rkinfo", "ka10027", body, token).then((data) => {
    if (data.return_code !== 0) throw new Error(`ka10027 실패(mrkt_tp=${mrktTp}): ${JSON.stringify(data).slice(0, 200)}`);
    return data;
  });
}

function parseKiwoomRankingRows(json) {
  let rows = [];
  for (const key of Object.keys(json)) {
    if (Array.isArray(json[key])) {
      rows = json[key];
      break;
    }
  }
  return rows
    .map((row) => {
      const code = (row.stk_cd || row.stk_no || "").split("_")[0];
      const name = row.stk_nm || row.stk_name || "";
      const price = Math.abs(parseInt(String(row.cur_prc ?? "0").replace(/[^\d-]/g, ""), 10)) || 0;
      const rate = parseFloat(row.flu_rt ?? row.updn_rt ?? "0") || 0;
      const volume = Math.abs(parseInt(String(row.now_trde_qty ?? row.trde_qty ?? "0").replace(/[^\d-]/g, ""), 10)) || 0;
      const cntrStr = parseFloat(row.cntr_str ?? "0") || 0;
      const buyReq = Math.abs(parseInt(String(row.buy_req ?? "0").replace(/[^\d-]/g, ""), 10)) || 0;
      const selReq = Math.abs(parseInt(String(row.sel_req ?? "0").replace(/[^\d-]/g, ""), 10)) || 0;
      return { code, name, price, rate, volume, cntrStr, buyReq, selReq };
    })
    .filter((r) => r.code);
}

const MIN_RATE = 5;
const MAX_RATE = 15;
// Worker의 isRegularStock/NON_STOCK_KEYWORD/ETF_BRAND_PREFIX와 완전히 동일한 기준으로 유지해야
// 두 경로(구버전 Worker 직접수집 vs relay 이전수집) 사이에 필터링 결과가 어긋나지 않음.
const NON_STOCK_KEYWORD = /(ETN|ETF|인버스|레버리지|선물|커버드콜|합성|파생결합|TDF|액티브|스팩|리츠|맥쿼리인프라)/i;
const ETF_BRAND_PREFIX =
  /^(KODEX|TIGER|KBSTAR|KIWOOM|ACE|SOL|RISE|PLUS|HANARO|KOSEF|KINDEX|TIMEFOLIO|마이다스|파워|WOORI|히어로즈|신한|대신|KTOP|FOCUS|네비게이터|파빌리온|우리|코세프|VITA|1Q|삼성|미래에셋|한투|마이티|WON|IBK|메리츠)\s?[0-9A-Za-z가-힣]*(200|100|150|300|배당|채권|국고채|MSCI|합성)/i;
function isRegularStockName(name) {
  if (!name) return false;
  if (NON_STOCK_KEYWORD.test(name)) return false;
  if (ETF_BRAND_PREFIX.test(name)) return false;
  return true;
}

async function fetchRiseListForMarket(mrktTp, market, token) {
  const json = await kiwoomRankingUp(mrktTp, token);
  const rows = parseKiwoomRankingRows(json);
  return rows
    .filter((r) => r.rate >= MIN_RATE && r.rate <= MAX_RATE && isRegularStockName(r.name))
    .map((r) => ({ ...r, market }));
}

// 데이터 수집용 장시간 판단 - 매매중지(isTradingActiveKST, 15:50컷)와는 별개.
// Worker의 isMarketHoursKST(09:01~15:46)와 동일 기준으로 맞춰야 수집 공백/시간대 불일치가 안 생김.
function isMarketHoursKST() {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const day = kst.getDay();
  if (day === 0 || day === 6) return false;
  const minutes = kst.getHours() * 60 + kst.getMinutes();
  return minutes >= 9 * 60 + 1 && minutes <= 15 * 60 + 46;
}

async function collectAndForwardSnapshots() {
  if (!ADMIN_KEY) return; // 인증 없으면 Worker가 받아주지 않으므로 스킵
  if (!isMarketHoursKST()) return;
  try {
    const token = await issueTokenCached();
    const kospi = await fetchRiseListForMarket("001", "KOSPI", token);
    await new Promise((r) => setTimeout(r, 1100)); // ka10027 초당 1건 제한
    const kosdaq = await fetchRiseListForMarket("101", "KOSDAQ", token);
    const all = [...kospi, ...kosdaq];
    if (!all.length) return;
    const result = await workerRequest("/api/ingest/snapshots", "POST", { items: all, capturedAt: new Date().toISOString() });
    if (result.ok) {
      console.log(`스냅샷 전송 완료: ${result.saved}건 (${result.capturedAt})`);
    } else {
      console.log("스냅샷 전송 실패: " + (result.error || "unknown"));
    }
  } catch (e) {
    console.log("스냅샷 수집 실패: " + e.message);
  }
}
// Worker cron의 collectAndStore를 완전히 대체 - 2분 주기로 relay가 직접 수집.
setInterval(collectAndForwardSnapshots, 120000);
setTimeout(collectAndForwardSnapshots, 5000); // 재시작 직후 2분 공백 방지용 1회 즉시 실행(5초 뒤, 토큰발급 여유)

// ---------- 해외지수(다우/나스닥/S&P500) + 원달러 환율 ----------
// 키움 국내주식 API 권한으로는 해외지수/환율을 못 받아옴(별도 해외파생 API 권한 필요) - 대신
// 네이버 모바일증권의 공개 JSON API(인증 불필요, 비공식이지만 안정적으로 널리 쓰임)를 사용.
// 국내 장 시간과 무관하게(미국 장은 밤에 열림) 24시간 갱신 - 장중 게이트 없음.
function fetchNaverIndex(code) {
  return new Promise((resolve, reject) => {
    https
      .get(
        `https://m.stock.naver.com/api/index/${encodeURIComponent(code)}/basic`,
        { headers: { "User-Agent": "Mozilla/5.0" }, timeout: 8000 },
        (res) => {
          let body = "";
          res.on("data", (c) => (body += c));
          res.on("end", () => {
            try {
              resolve(JSON.parse(body));
            } catch (e) {
              reject(new Error("파싱 실패: " + body.slice(0, 200)));
            }
          });
        }
      )
      .on("error", reject)
      .on("timeout", function () {
        this.destroy(new Error("타임아웃"));
      });
  });
}

const globalIndexCache = { dji: null, ixic: null, spx: null, usdkrw: null, updatedAt: null };
async function refreshGlobalIndices() {
  const targets = [
    ["dji", ".DJI"], // 다우존스
    ["ixic", ".IXIC"], // 나스닥종합
    ["spx", ".SPX"], // S&P500
    ["usdkrw", "FX_USDKRW"], // 원달러 환율
  ];
  for (const [key, code] of targets) {
    try {
      const json = await fetchNaverIndex(code);
      // 네이버 응답 필드명은 지수/환율 종류에 따라 조금씩 다를 수 있어 여러 후보를 순서대로 확인
      const price = parseFloat(json.closePrice ?? json.now ?? json.tradePrice ?? json.closePriceStr ?? "0");
      const rate = parseFloat(
        String(json.fluctuationsRatio ?? json.changeRate ?? json.fluctuationsRatioStr ?? "0").replace(/[^0-9.-]/g, "")
      );
      if (price > 0) {
        globalIndexCache[key] = { price, rate };
      }
    } catch (e) {
      console.log(`해외지수(${code}) 조회 실패: ${e.message}`);
    }
  }
  globalIndexCache.updatedAt = new Date().toISOString();
}
setInterval(refreshGlobalIndices, 5000); // 5초마다 - 너무 짧으면(3초 이하) 네이버 차단 위험, 5초가 안전권에서 최대한 당긴 값
setTimeout(refreshGlobalIndices, 3000);

// 국내(웹소켓 실시간)+해외(네이버 폴링) 지수를 한 번에 묶어서 반환 - SSE/realtime-all 등 여러
// 응답 지점에서 공통으로 재사용.
function buildIndexPayload() {
  return {
    kospi: realtimeCache.index["001"] || null,
    kosdaq: realtimeCache.index["101"] || null,
    dji: globalIndexCache.dji,
    ixic: globalIndexCache.ixic,
    spx: globalIndexCache.spx,
    usdkrw: globalIndexCache.usdkrw,
  };
}

// ---------- 15:36 최종 종가 재조회 (Worker collectFinalAccurateQuotes/retryFinalQuotePending 완전 이전) ----------
// Worker는 호출당 서브리퀘스트 한도(약 50개)가 있어서 종목이 많으면 여러 틱(15:36/38/40/42/44)에
// 나눠 재시도해야 했음. relay는 그런 한도가 없어서 한 번에 전종목 순차조회(1.1초 간격) 가능.
function kiwoomQuoteRelay(code, token) {
  return kiwoomRest("/api/dostk/mrkcond", "ka10007", { stk_cd: code }, token);
}
function parseKiwoomQuoteRelay(json) {
  const abs = (v) => Math.abs(parseInt(String(v ?? "0").replace(/[^\d-]/g, ""), 10)) || 0;
  return {
    price: abs(json.cur_prc),
    rate: parseFloat(json.flu_rt ?? "0") || 0,
    volume: abs(json.trde_qty ?? json.now_trde_qty),
  };
}

let finalQuoteDoneToday = null; // "YYYY-MM-DD" - 하루 한 번만 실행되게 (같은 날 재시작돼도 중복 방지)
async function runFinalQuoteReconcile() {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const dateKey = `${kst.getFullYear()}-${String(kst.getMonth() + 1).padStart(2, "0")}-${String(kst.getDate()).padStart(2, "0")}`;
  if (finalQuoteDoneToday === dateKey) return;
  if (!ADMIN_KEY) return;
  try {
    const targetsRes = await workerRequest("/api/final-quote-targets", "GET");
    if (!targetsRes.ok || !targetsRes.targets.length) return;
    const targets = targetsRes.targets;
    const token = await issueTokenCached();
    const rows = [];
    const failedCodes = [];
    for (const t of targets) {
      try {
        const raw = await kiwoomQuoteRelay(t.code, token);
        const q = parseKiwoomQuoteRelay(raw);
        rows.push({ code: t.code, name: t.name, price: q.price, rate: q.rate, volume: q.volume, market: t.market });
      } catch (e) {
        failedCodes.push(t.code);
      }
      await new Promise((r) => setTimeout(r, 1100)); // 키움 TR 초당1건 제한
    }
    if (rows.length) {
      const result = await workerRequest("/api/ingest/final-quotes", "POST", {
        rows, capturedAt: new Date().toISOString(), failedCodes,
      });
      if (result.ok) {
        console.log(`최종 종가 재조회 완료: ${result.saved}/${targets.length}종목 (실패 ${failedCodes.length}종목)`);
        finalQuoteDoneToday = dateKey;
      } else {
        console.log("최종 종가 재조회 전송 실패: " + (result.error || "unknown"));
      }
    }
  } catch (e) {
    console.log("최종 종가 재조회 실패: " + e.message);
  }
}
// 15:36 KST 정각을 정확히 맞추기보다, 15:35~15:40 사이 1분 간격으로 체크해서 그 구간에 한 번만 실행.
// (분 단위 트리거를 setInterval로 대충 맞추는 방식 - cron 없는 Node 프로세스라 이렇게 처리)
setInterval(() => {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const minutes = kst.getHours() * 60 + kst.getMinutes();
  if (minutes >= 15 * 60 + 36 && minutes <= 15 * 60 + 40) {
    runFinalQuoteReconcile();
  }
}, 60000);

// ---------- SSE(Server-Sent Events) 실시간 스트리밍 ----------
// Worker가 2초마다 폴링하며 relay를 두드리던 구조 대신, relay가 웹소켓으로 값을 받는
// 즉시 연결된 모든 SSE 클라이언트(Worker 경유)에 바로 push. 폴링 지연이 사라지고
// 키움->relay->Worker->브라우저 전 구간이 이벤트 기반이 됨(진짜 실시간에 가까워짐).
const sseClients = new Set(); // Set<http.ServerResponse>
// 캐시에 남아있는 전 종목이 아니라, 실제로 화면에 쓰이는 3그룹(관심종목/화면리스트/실시간포착)에
// 속한 종목만 골라서 반환 - SSE 브로드캐스트와 폴링 엔드포인트(/realtime/all, /realtime/stocks)가
// 공통으로 씀. 정리(trim) 타이밍 사이에 남아있는 자투리 데이터까지 매번 통째로 직렬화/전송하던 낭비를 줄임.
function relevantStocksPayload() {
  const relevantCodes = new Set([...subscribedStocks, ...subscribedListStocks, ...realtimeCache.condition.codes]);
  const stocks = {};
  for (const code of relevantCodes) {
    if (realtimeCache.stock[code]) stocks[code] = realtimeCache.stock[code];
  }
  return stocks;
}

function sseBroadcast(payload) {
  if (!sseClients.size) return;
  const line = `data: ${JSON.stringify(payload)}\n\n`;
  for (const res of sseClients) {
    try {
      res.write(line);
    } catch (e) {
      sseClients.delete(res);
    }
  }
}
// 매 웹소켓 메시지마다 브로드캐스트하면 너무 잦을 수 있어(체결이 빈번한 종목은 초당 여러 번) 묶어서
// 전송. 200ms는 장중 종목 수가 많아지면(관심종목+화면리스트+실시간포착 합쳐 최대 250여개) relay
// CPU와 클라이언트 렌더링 부하가 누적돼 장중 갈수록 느려지는 원인이 됐음 - 500ms로 완화.
// 그래도 기존 2초 폴링보다 4배 빠름.
let sseBroadcastPending = false;
function scheduleSseBroadcast() {
  if (sseBroadcastPending || !sseClients.size) return;
  sseBroadcastPending = true;
  setTimeout(() => {
    sseBroadcastPending = false;
    const cond = realtimeCache.condition;
    const history = buildConditionHistory();
    sseBroadcast({
      index: buildIndexPayload(),
      stocks: relevantStocksPayload(),
      condition: { seq: cond.seq, name: cond.name, codes: cond.codes, count: cond.codes.length, lastEventAt: cond.lastEventAt, history },
    });
  }, 500);
}

// 당일 최고 등락률(0B 체결 틱마다 갱신) / 직전 호가잔량(0D 틱마다 갱신) - 배치(2분 cron)로만
// 계산하던 isTodayHigh/bidTurnedPositive/buyReqSpike/sellReqThinning을 relay가 실시간으로
// 직접 계산하기 위한 캐시. 장 시작 시 리셋은 아래 miniCandleCacheClearedDate 옆 setInterval에서 같이 처리.
const todayMaxRateCache = {}; // { code: 오늘 최고 등락률 }
const prevOrderFlowCache = {}; // { code: { buyReq, selReq } } - 직전 호가 틱 값
let group9ResyncPending = false;
let group9LastCodes = []; // 직전에 실제로 등록한 목록 - 내용이 안 바뀌었으면 재등록 스킵
function scheduleGroup9Resync() {
  if (group9ResyncPending) return;
  group9ResyncPending = true;
  setTimeout(() => {
    group9ResyncPending = false;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const current = [...realtimeCache.condition.codes];
    const changed = current.length !== group9LastCodes.length || current.some((c) => !group9LastCodes.includes(c));
    if (!changed) return;
    ws.send(JSON.stringify({ trnm: "REMOVE", grp_no: "9" }));
    if (current.length) {
      ws.send(JSON.stringify({
        trnm: "REG", grp_no: "9", refresh: "1",
        data: [{ item: current, type: ["0B", "0D"] }],
      }));
    }
    group9LastCodes = current;
    console.log("실시간포착 호가잔량 구독 재동기화:", current.length + "종목");
  }, 2000); // 조건검색 편입/이탈이 짧은 시간에 몰아서 일어날 수 있어 2초 묶어서 처리(REG 스팸 방지)
}

function handleRealtimeMessage(msg) {
  if (!Array.isArray(msg.data)) return;
  for (const entry of msg.data) {
    if (entry.type === "0J" && entry.values) {
      // 업종지수: 10=현재가, 12=등락률, 20=체결시각
      // 주의: 키움 실시간 "현재가"는 부호가 붙어 오지만(-71400 등) 이건 가격이 마이너스라는 뜻이 아니라
      // "기준가 대비 하락중"이라는 방향 표시임. 가격 자체는 항상 절댓값으로 처리해야 함(그대로 두면 하락일에
      // 가격이 음수로 계산되는 버그가 생김 - 실측으로 확인됨). 등락률(12)은 방향이 의미 있으니 부호 유지.
      realtimeCache.index[entry.item] = {
        price: Math.abs(parseSignedNumber(entry.values["10"])),
        rate: parseSignedNumber(entry.values["12"]),
        time: entry.values["20"] || "",
        updatedAt: new Date().toISOString(),
      };
    } else if (entry.type === "0B" && entry.values) {
      // 주식체결: 10=현재가, 12=등락률, 13=누적거래량, 228=체결강도, 20=체결시각
      // 현재가는 위와 동일한 이유로 절댓값 처리
      const rate = parseSignedNumber(entry.values["12"]);
      // 당일 최고 등락률을 실시간으로 계속 갱신 - 배치(2분 cron)로만 계산하던 isTodayHigh를
      // relay가 체결 틱마다 즉시 갱신할 수 있게 됨(장 시작 시 리셋은 아래 setInterval 참고)
      const prevMax = todayMaxRateCache[entry.item];
      if (prevMax === undefined || rate > prevMax) todayMaxRateCache[entry.item] = rate;
      const isTodayHigh = rate >= (todayMaxRateCache[entry.item] ?? rate) - 0.001;
      const existing = realtimeCache.stock[entry.item] || {};
      realtimeCache.stock[entry.item] = {
        ...existing,
        price: Math.abs(parseSignedNumber(entry.values["10"])),
        rate,
        volume: parseSignedNumber(entry.values["13"]),
        cntrStr: parseSignedNumber(entry.values["228"]),
        time: entry.values["20"] || "",
        isTodayHigh,
        updatedAt: new Date().toISOString(),
      };
    } else if (entry.type === "0D" && entry.values) {
      // 주식호가잔량: 121=매도호가총잔량, 125=매수호가총잔량 - 배치(2분 cron)로만 비교하던
      // 매수전환/매수잔량급증/매도잔량급감을 relay가 호가 변동 틱마다 즉시 계산할 수 있게 됨.
      const code = entry.item;
      const buyReq = Math.abs(parseSignedNumber(entry.values["125"]));
      const selReq = Math.abs(parseSignedNumber(entry.values["121"]));
      const prev = prevOrderFlowCache[code];
      let bidTurnedPositive = false, buyReqSpike = false, sellReqThinning = false;
      if (prev) {
        // 매수전환: 직전엔 매도잔량이 더 많았는데 지금 막 매수잔량 우위로 뒤집힘
        bidTurnedPositive = buyReq > selReq && prev.buyReq <= prev.selReq;
        // 매수잔량급증: 직전 대비 매수잔량이 1.5배 이상
        buyReqSpike = prev.buyReq > 0 && buyReq / prev.buyReq >= 1.5;
        // 매도잔량급감: 직전 대비 매도잔량이 절반 이하로 줄어듦
        sellReqThinning = prev.selReq > 0 && selReq / prev.selReq <= 0.5;
      }
      prevOrderFlowCache[code] = { buyReq, selReq };
      wsLastGroup9MessageAt = Date.now(); // 그룹9(0D) 전용 최근 수신시각 - 아래 부분장애 감지에서 씀
      const existing2 = realtimeCache.stock[code] || {};
      realtimeCache.stock[code] = {
        ...existing2,
        buyReq, selReq, bidTurnedPositive, buyReqSpike, sellReqThinning,
        updatedAt: new Date().toISOString(),
      };
    } else if (entry.type === "02" && entry.values) {
      // 조건검색 실시간: 9001=종목코드, 843=편입(I)/이탈(D), 20=시각
      // 조건에 새로 들어오거나 빠지는 순간 즉시 통보되므로, 2분 폴링 없이 실시간 포착 가능
      const rawCode = String(entry.values["9001"] || entry.item || "");
      const code = rawCode.replace(/^A/, ""); // 응답에 A가 붙어오는 경우가 있어 제거
      const inOut = entry.values["843"];
      if (!code) continue;

      const isInsert = inOut === "I"; // I=Insert(편입), D=Delete(이탈)
      const idx = realtimeCache.condition.codes.indexOf(code);
      if (isInsert) {
        if (idx === -1) realtimeCache.condition.codes.push(code);
      } else {
        if (idx !== -1) realtimeCache.condition.codes.splice(idx, 1);
      }
      // 호가잔량(0D)까지 실시간 구독하는 그룹9을 "지금 조건에 걸려있는 종목"과 항상 일치시킴 -
      // 예전엔 한 번 편입되면 하루 종일(최대 80종목까지) 구독이 안 빠져서, 시간이 갈수록 실시간
      // 메시지량이 누적돼 relay(1vCPU) 부하로 장중 갈수록 느려지는 원인이 됐음. 이제는 조건에서
      // 이탈하면 그 즉시 구독도 같이 빠짐 - 실제 필요한 만큼(보통 몇~수십 개)만 유지됨.
      scheduleGroup9Resync();

      realtimeCache.condition.lastEventAt = new Date().toISOString();
      realtimeCache.condition.events.unshift({
        code: code,
        action: isInsert ? "편입" : "이탈",
        time: entry.values["20"] || "",
        at: realtimeCache.condition.lastEventAt,
      });
      if (realtimeCache.condition.events.length > 50) realtimeCache.condition.events.length = 50;

      // 편입 이력은 따로 보관: 조건에서 금방 빠져나가도 "방금 이런 게 있었다"를 놓치지 않게 함.
      // (현재 조건 만족 목록만 보여주면, 잠깐 스쳐간 종목은 화면에서 그냥 사라져버림)
      if (isInsert) {
        const hist = realtimeCache.condition.history;
        const existing = hist.findIndex((h) => h.code === code);
        if (existing !== -1) hist.splice(existing, 1); // 재편입이면 맨 위로 올림
        hist.unshift({
          code: code,
          time: entry.values["20"] || "",
          at: realtimeCache.condition.lastEventAt,
        });
        if (hist.length > 60) hist.length = 60;
        queueNameFetch([code]);
      }
    }
  }
  scheduleSseBroadcast();
}

function registerSubscriptions() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

  // 재연결마다 그룹9(조건검색 실시간포착 호가잔량 0D) 상태를 리셋 - 웹소켓이 새로 열리면 키움 서버
  // 쪽 REG 상태도 전부 초기화되는데, group9LastCodes(diff 비교용 캐시)는 relay 프로세스 메모리에
  // 그대로 남아있어서 "이미 등록했으니 다를 게 없다"고 착각하고 재등록을 건너뛰는 문제가 있었음.
  // 그러면 재연결 이후 다음 자연 편입/이탈 이벤트가 올 때까지 0D 데이터가 전혀 안 들어와서,
  // isTodayHigh 하드블록 필터(데이터 없으면 통과시키는 설계)가 그 사이 조용히 무력화됨.
  group9LastCodes = [];

  // 요청을 한꺼번에 몰아 보내면 키움이 일부(특히 CNSRREQ)를 처리하지 못하는 현상이 있어,
  // 조건검색을 가장 먼저 보내고 나머지는 간격을 두고 순차 전송함.
  const send = (payload, label) => {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    ws.send(JSON.stringify(payload));
    if (label) console.log(label);
  };

  let delay = 0;
  const later = (fn) => {
    delay += 400;
    setTimeout(fn, delay);
  };

  // 1) 조건검색: 목록조회(CNSRLST)를 먼저 보냄.
  //    CNSRLST 없이 바로 CNSRREQ를 보내면 응답이 오지 않는 현상이 있어(실측),
  //    CNSRLST 응답을 받은 뒤에 CNSRREQ를 보내도록 함(아래 message 핸들러에서 처리).
  if (CONDITION_SEQ) {
    send({ trnm: "CNSRLST" }, "조건검색 목록조회 요청 (seq=" + CONDITION_SEQ + " 등록 준비)");
  }

  // 2) 지수
  later(() =>
    send(
      { trnm: "REG", grp_no: "1", refresh: "1", data: [{ item: ["001", "101"], type: ["0J"] }] },
      "실시간 지수 구독 등록 요청"
    )
  );

  // 3) 관심종목
  if (subscribedStocks.length) {
    later(() =>
      send(
        { trnm: "REG", grp_no: "2", refresh: "1", data: [{ item: subscribedStocks, type: ["0B"] }] },
        "실시간 관심종목 구독 등록 요청: " + subscribedStocks.length + "종목"
      )
    );
  }

  // 4) 화면 리스트 종목
  if (subscribedListStocks.length) {
    later(() =>
      send(
        { trnm: "REG", grp_no: "3", refresh: "1", data: [{ item: subscribedListStocks, type: ["0B"] }] },
        "실시간 리스트종목 구독 등록 요청: " + subscribedListStocks.length + "종목"
      )
    );
  }
}

async function connectWebSocket() {
  if (!APP_KEY || !APP_SECRET) {
    console.log("KIWOOM_APP_KEY_REAL/SECRET 미설정 - 웹소켓 기능 비활성화 (REST 중계는 정상 동작)");
    return;
  }

  let token;
  try {
    token = await issueToken();
  } catch (e) {
    console.error("웹소켓용 토큰 발급 실패:", e.message);
    scheduleReconnect();
    return;
  }

  ws = new WebSocket(WS_URL);
  wsConnected = false;
  wsLoggedIn = false;

  ws.on("open", () => {
    wsConnected = true;
    console.log("웹소켓 연결됨 - LOGIN 전송");
    ws.send(JSON.stringify({ trnm: "LOGIN", token: token }));
  });

  ws.on("message", (raw) => {
    wsLastMessageAt = Date.now();
    let msg;
    try {
      msg = JSON.parse(raw.toString());
    } catch (e) {
      return;
    }

    // PING은 그대로 되돌려줘야 연결이 유지됨
    if (msg.trnm === "PING") {
      ws.send(JSON.stringify(msg));
      return;
    }

    if (msg.trnm === "LOGIN") {
      if (msg.return_code !== 0) {
        console.error("웹소켓 로그인 실패:", msg.return_msg);
        ws.close();
        return;
      }
      wsLoggedIn = true;
      wsLoginAt = Date.now();
      wsReconnectDelay = 5000; // 성공했으니 백오프 초기화
      console.log("웹소켓 로그인 성공");
      registerSubscriptions();
      return;
    }

    if (msg.trnm === "REAL") {
      handleRealtimeMessage(msg);
      return;
    }

    // 조건검색 목록조회 응답 -> 이어서 실시간 등록(CNSRREQ) 요청
    if (msg.trnm === "CNSRLST") {
      const list = msg.data || [];
      const found = list.find((x) => String(Array.isArray(x) ? x[0] : x.seq) === String(CONDITION_SEQ));
      if (!found) {
        console.error("조건식 seq=" + CONDITION_SEQ + " 을(를) 목록에서 찾지 못했습니다. 등록된 조건식:", list.length + "개");
        return;
      }
      const name = Array.isArray(found) ? found[1] : found.name;
      console.log("조건식 확인: seq=" + CONDITION_SEQ + " name=" + name + " -> 실시간 등록 요청");
      realtimeCache.condition.name = name || null;
      ws.send(JSON.stringify({
        trnm: "CNSRREQ",
        seq: String(CONDITION_SEQ),
        search_type: "1",
        stex_tp: "K",
      }));
      realtimeCache.condition.seq = CONDITION_SEQ;
      return;
    }

    // 조건검색 등록 응답 - 현재 조건을 만족하는 종목 목록이 한 번에 옴
    if (msg.trnm === "CNSRREQ") {
      if (msg.return_code !== 0) {
        console.error("조건검색 등록 실패:", msg.return_code, msg.return_msg);
        return;
      }
      const codes = (msg.data || [])
        .map((d) => String(d.jmcode || "").replace(/^A/, ""))
        .filter(Boolean);
      realtimeCache.condition.codes = codes;
      realtimeCache.condition.lastEventAt = new Date().toISOString();

      // 초기 목록도 이력에 넣어둠. 안 그러면 relay 재시작 직후 화면이 텅 비어 보임
      // (이력은 편입 이벤트로만 쌓이는데, 재시작 시점엔 이미 조건에 들어와 있던 종목은 이벤트가 안 옴)
      const nowIso = realtimeCache.condition.lastEventAt;
      const nowHHMMSS = new Date(Date.now() + 9 * 3600 * 1000)
        .toISOString()
        .slice(11, 19)
        .replace(/:/g, "");
      realtimeCache.condition.history = codes.slice(0, 60).map((c) => ({
        code: c,
        time: nowHHMMSS,
        at: nowIso,
        initial: true, // 실시간 편입이 아니라 시작 시점 스냅샷임을 표시
      }));

      queueNameFetch(codes.slice(0, 40)); // 초기 목록도 이름을 미리 받아둠(초당1건이라 상위 일부만)
      console.log("조건검색 초기 종목:", codes.length + "종목 (seq=" + msg.seq + ")");
      // 재연결 직후 이 시점에 바로 그룹9(호가잔량 0D) 재동기화를 걸어서, 다음 자연 편입/이탈
      // 이벤트를 기다리지 않고 즉시 수급 데이터가 채워지게 함 (위 group9LastCodes 리셋과 짝)
      scheduleGroup9Resync();
      return;
    }

    // 예상 못한 응답만 로그로 남김. REG/REMOVE 정상응답(0)은 너무 잦아서 제외하되,
    // 실패는 반드시 남겨서 200 초과 같은 문제를 놓치지 않게 함.
    if (msg.trnm && msg.trnm !== "REAL") {
      const isRoutineOk = (msg.trnm === "REG" || msg.trnm === "REMOVE") && msg.return_code === 0;
      if (!isRoutineOk) console.log("미처리 메시지:", JSON.stringify(msg).slice(0, 300));
    }
  });

  ws.on("error", (e) => {
    console.error("웹소켓 에러:", e.message);
  });

  ws.on("close", () => {
    wsConnected = false;
    wsLoggedIn = false;
    console.log("웹소켓 연결 종료 - 재연결 예약");
    scheduleReconnect();
  });
}

function scheduleReconnect() {
  setTimeout(connectWebSocket, wsReconnectDelay);
  wsReconnectDelay = Math.min(wsReconnectDelay * 2, 60000); // 지수 백오프 (최대 60초)
}

// 좀비 연결 감지: 소켓은 열려있는데 데이터가 한참 안 오면 강제로 끊고 재연결
// (키움 웹소켓은 장중 계속 푸시가 오므로, 3분 침묵은 비정상)
setInterval(() => {
  if (!wsConnected || !wsLastMessageAt) return;
  if (Date.now() - wsLastMessageAt > 3 * 60 * 1000) {
    console.log("웹소켓 3분간 무응답 - 강제 재연결");
    try {
      ws.terminate();
    } catch (e) {}
  }
}, 60000);

// 그룹9(호가잔량 0D) 부분장애 감지 - 조건검색에 종목이 있는데도 2분 넘게 0D 데이터가 전혀
// 안 들어오면(REG가 서버 쪽에서 조용히 씹혔거나 등 웹소켓 자체는 멀쩡한 부분장애) 강제로
// 재동기화. 전체 웹소켓을 끊는 것보다 가벼워서 우선 시도하고, 그래도 안 되면 위 3분 감지가
// 결국 잡아냄. isTodayHigh 하드블록 필터가 이 데이터에 의존하므로 방치하면 안전장치가
// 조용히 무력화된 채로 계속 돌게 됨.
setInterval(() => {
  if (!wsConnected || !isTradingActiveKST()) return;
  if (!realtimeCache.condition.codes.length) return; // 조건에 걸린 종목 자체가 없으면 0D가 안 오는 게 정상
  const silentFor = wsLastGroup9MessageAt ? Date.now() - wsLastGroup9MessageAt : Infinity;
  if (silentFor > 2 * 60 * 1000) {
    console.log("그룹9(호가잔량) 2분간 무응답 - 강제 재동기화");
    group9LastCodes = []; // diff 캐시를 리셋해서 다음 resync가 반드시 REG를 다시 보내게 함
    scheduleGroup9Resync();
  }
}, 60000);

connectWebSocket();

// ---------- 관심종목 손절/익절 자동체크 (10초 주기) ----------
// Worker의 2분 cron(checkWatchlistRiskLevels)보다 훨씬 빠르게 -2.5%/+3.5% 트리거.
// relay는 이미 웹소켓으로 실시간가를 들고 있으므로 키움 TR 호출 없이 즉시 계산 가능.
const AUTO_REMOVE_PNL_PCT = -2.5; // 손절 (2026-08-21 외부 분석 근거로 -1.5%->-2.5% 확대: +3.5%/-1.5% 장벽에서
// 드리프트 0인 무작위 워크라도 손절이 먼저 맞을 확률이 1.5/(3.5+1.5)=70%로 구조적으로 손절 우위였음.
// -2.5%로 넓히면 41.7%로 개선됨. 급등주 장중 변동성(±1.5% 스윙이 몇 분 안에 발생)이 예전 손절폭
// 안에 있어서 노이즈에 잦게 걸렸던 문제도 같이 완화됨.
const AUTO_TAKE_PROFIT_PNL_PCT = 3.5; // 익절 (고정값 - 트레일링스톱이 실질적 익절 로직을 보완함)

// 15:50 이후 자동매매(익절/손절) 중지 - Worker도 동일 기준으로 403 처리하지만
// relay 쪽에서 먼저 걸러서 불필요한 요청/로그 방지.
function isTradingActiveKST() {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const day = kst.getDay();
  if (day === 0 || day === 6) return false;
  const minutes = kst.getHours() * 60 + kst.getMinutes();
  return minutes >= 9 * 60 + 1 && minutes < 15 * 60 + 50;
}
function workerRequest(path, method, body) {
  return new Promise((resolve, reject) => {
    const url = new URL(WORKER_URL + path);
    const data = body ? JSON.stringify(body) : null;
    const req = https.request(
      {
        hostname: url.hostname,
        path: url.pathname + url.search,
        method,
        headers: Object.assign(
          { "X-Admin-Key": ADMIN_KEY },
          data ? { "Content-Type": "application/json", "Content-Length": Buffer.byteLength(data) } : {}
        ),
        timeout: 8000,
      },
      (res) => {
        let chunks = "";
        res.on("data", (c) => (chunks += c));
        res.on("end", () => {
          try {
            resolve(JSON.parse(chunks));
          } catch (e) {
            reject(e);
          }
        });
      }
    );
    req.on("error", reject);
    req.on("timeout", () => req.destroy(new Error("timeout")));
    if (data) req.write(data);
    req.end();
  });
}

// entries(코드+진입가) 자체는 자주 안 바뀌므로 5초 TTL로 캐싱 -> 2초 틱마다 Worker를
// 두드리지 않고, 실시간가 비교(로컬 연산)만 매 틱 수행. 관심종목 추가/삭제는 몇 초 지연되어
// 반영되지만 손익 판단 정확도에는 영향 없음(가격은 항상 최신 realtimeCache 사용).
let entriesCache = { items: [], fetchedAt: 0 };
const ENTRIES_TTL_MS = 5000;
let entriesCacheHits = 0;
let entriesCacheMisses = 0;
async function getWatchlistEntriesCached() {
  if (Date.now() - entriesCache.fetchedAt < ENTRIES_TTL_MS) {
    entriesCacheHits++;
    return entriesCache.items;
  }
  entriesCacheMisses++;
  const entries = await workerRequest("/api/watchlist-entries", "GET");
  if (entries.ok) {
    entriesCache = { items: entries.items, fetchedAt: Date.now() };
  }
  return entriesCache.items;
}
// 10분마다 캐시 히트율 로그 - watchlist-entries 실제 호출 빈도 확인용
setInterval(() => {
  const total = entriesCacheHits + entriesCacheMisses;
  if (total === 0) return;
  console.log(`entries 캐시 통계(10분): 히트 ${entriesCacheHits} / 미스(실제호출) ${entriesCacheMisses} / 히트율 ${((entriesCacheHits / total) * 100).toFixed(1)}%`);
  entriesCacheHits = 0;
  entriesCacheMisses = 0;
}, 600000);

// ---------- 관심종목 미니차트(1분봉) 백그라운드 캐시 ----------
// 원래 Worker가 화면 로드 때마다 종목당 1.1초씩 순차조회(ka10080)했던 게 관심종목 수만큼
// 누적되어 체감 로딩이 느렸음(10종목이면 11초+). relay가 백그라운드에서 미리 갱신해두고
// Worker는 그 캐시를 즉시 반환하게 바꿔 - 화면에서는 사실상 즉시(0.1초 이내) 뜨게 됨.
// 파일로도 영속화해서 relay 재시작(배포 등)에도 캐시가 날아가지 않게 함 - 장마감 후에도
// 마지막 장중 데이터를 그대로 즉시 서빙 가능(어차피 그 시점 이후로 안 바뀌는 데이터라 유효함).
const miniCandleCache = {}; // { code: { candles: [...], tradingDate, updatedAt } }
const MINI_CANDLE_CACHE_PATH = path.join(__dirname, "mini-candle-cache.json");
function saveMiniCandleCache() {
  try {
    fs.writeFileSync(MINI_CANDLE_CACHE_PATH, JSON.stringify(miniCandleCache));
  } catch (e) {
    console.log("미니차트 캐시 저장 실패: " + e.message);
  }
}
function loadMiniCandleCache() {
  try {
    if (!fs.existsSync(MINI_CANDLE_CACHE_PATH)) return;
    const raw = JSON.parse(fs.readFileSync(MINI_CANDLE_CACHE_PATH, "utf8"));
    Object.assign(miniCandleCache, raw);
    console.log(`미니차트 캐시 복구: 종목 ${Object.keys(raw).length}개`);
  } catch (e) {
    console.log("미니차트 캐시 복구 실패: " + e.message);
  }
}
loadMiniCandleCache();
setInterval(saveMiniCandleCache, 60000); // 갱신 주기와 맞춰 1분마다 저장

function todayYYYYMMDDRelay() {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const y = kst.getFullYear();
  const m = String(kst.getMonth() + 1).padStart(2, "0");
  const d = String(kst.getDate()).padStart(2, "0");
  return `${y}${m}${d}`;
}
function parseKiwoomChartOHLCRelay(json) {
  let rows = [];
  for (const key of Object.keys(json)) {
    if (Array.isArray(json[key])) {
      rows = json[key];
      break;
    }
  }
  const abs = (v) => Math.abs(parseInt(String(v ?? "0").replace(/[^\d-]/g, ""), 10)) || 0;
  return rows
    .map((row) => ({
      open: abs(row.open_pric),
      high: abs(row.high_pric),
      low: abs(row.low_pric),
      close: abs(row.cur_prc ?? row.close_pric),
      volume: abs(row.trde_qty ?? row.now_trde_qty),
      time: row.cntr_tm || "",
    }))
    .filter((r) => r.close > 0 && r.high > 0 && r.low > 0)
    .reverse();
}
async function refreshMiniCandlesForWatchlist() {
  if (!ADMIN_KEY) return;
  if (!isMarketHoursKST()) return; // 장시간 외엔 갱신 불필요(어차피 안 바뀜)
  try {
    const entries = await getWatchlistEntriesCached();
    if (!entries.length) return;
    const token = await issueTokenCached();
    for (const item of entries) {
      try {
        const raw = await kiwoomRest("/api/dostk/chart", "ka10080", { stk_cd: item.code, tic_scope: "1", upd_stkpc_tp: "1" }, token);
        const parsed = parseKiwoomChartOHLCRelay(raw);
        const todayStr = todayYYYYMMDDRelay();
        const hasToday = parsed.some((c) => c.time.slice(0, 8) === todayStr);
        const targetDate = hasToday ? todayStr : parsed.reduce((max, c) => (c.time.slice(0, 8) > max ? c.time.slice(0, 8) : max), "");
        const candles = parsed.filter((c) => c.time.slice(0, 8) === targetDate && c.time.slice(8, 12) >= "0900");
        miniCandleCache[item.code] = { candles, tradingDate: targetDate || null, updatedAt: Date.now() };
      } catch (e) {
        // 개별 종목 실패는 건너뜀 - 다음 갱신 주기에 재시도
      }
      await new Promise((r) => setTimeout(r, 1100)); // 키움 TR 초당1건 제한
    }
  } catch (e) {
    console.log("미니차트 캐시 갱신 실패: " + e.message);
  }
}

// ---------- 관심종목 추가/삭제 즉시 반영 ----------
// 새로 추가된 종목은 다음 60초 정기갱신을 기다리지 않고 바로(장시작~현재까지 전체 1분봉을) 채워서
// 화면에서 "아직 캐시 안 됨" 공백이 최소화되게 함. 삭제된 종목은 캐시에서 즉시 제거해서
// 메모리가 무한정 쌓이지 않게 함(관심종목 아닌 종목의 낡은 데이터가 계속 파일에 남는 것 방지).
let prevWatchlistCodes = new Set();
let newCodeFetchRunning = false;
const newCodeFetchQueue = [];
async function processNewCodeFetchQueue() {
  if (newCodeFetchRunning) return;
  newCodeFetchRunning = true;
  while (newCodeFetchQueue.length) {
    const code = newCodeFetchQueue.shift();
    try {
      const token = await issueTokenCached();
      const raw = await kiwoomRest("/api/dostk/chart", "ka10080", { stk_cd: code, tic_scope: "1", upd_stkpc_tp: "1" }, token);
      const parsed = parseKiwoomChartOHLCRelay(raw);
      const todayStr = todayYYYYMMDDRelay();
      const hasToday = parsed.some((c) => c.time.slice(0, 8) === todayStr);
      const targetDate = hasToday ? todayStr : parsed.reduce((max, c) => (c.time.slice(0, 8) > max ? c.time.slice(0, 8) : max), "");
      const candles = parsed.filter((c) => c.time.slice(0, 8) === targetDate && c.time.slice(8, 12) >= "0900");
      miniCandleCache[code] = { candles, tradingDate: targetDate || null, updatedAt: Date.now() };
      console.log(`신규 관심종목 차트 즉시조회 완료: ${code} (${candles.length}봉)`);
    } catch (e) {
      console.log(`신규 관심종목 차트 즉시조회 실패: ${code} - ${e.message}`);
    }
    await new Promise((r) => setTimeout(r, 1100)); // 키움 TR 초당1건 제한
  }
  newCodeFetchRunning = false;
}
async function checkWatchlistMembershipChanges() {
  if (!ADMIN_KEY) return;
  try {
    const entries = await getWatchlistEntriesCached();
    const currentCodes = new Set(entries.map((e) => e.code));

    // 삭제된 종목: 캐시에서 즉시 제거
    for (const code of prevWatchlistCodes) {
      if (!currentCodes.has(code)) {
        delete miniCandleCache[code];
        console.log(`관심종목 삭제 감지 - 캐시 제거: ${code}`);
      }
    }

    // 신규 종목: 장중이면 즉시조회 큐에 추가(60초 정기갱신을 기다리지 않음)
    if (isMarketHoursKST()) {
      for (const code of currentCodes) {
        if (!prevWatchlistCodes.has(code) && !miniCandleCache[code] && !newCodeFetchQueue.includes(code)) {
          newCodeFetchQueue.push(code);
        }
      }
      if (newCodeFetchQueue.length) processNewCodeFetchQueue();
    }

    // 웹소켓 실시간가 구독도 관심종목 변경에 맞춰 자체 갱신 - 브라우저가 페이지를 안 열어놔도
    // (아무도 /realtime/subscribe를 호출 안 해도) 관심종목은 항상 최신 상태로 구독 유지됨.
    // 구독 등록은 웹소켓 메시지라 키움 REST 초당1건 제한과 무관 - 걸릴 일 없음.
    const codesArr = [...currentCodes];
    const changed = codesArr.length !== subscribedStocks.length || codesArr.some((c) => !subscribedStocks.includes(c));
    if (changed && ws && ws.readyState === WebSocket.OPEN && wsLoggedIn) {
      subscribedStocks = codesArr;
      ws.send(JSON.stringify({ trnm: "REG", grp_no: "2", refresh: "1", data: [{ item: subscribedStocks, type: ["0B"] }] }));
      console.log("관심종목 변경 감지 - 웹소켓 구독 자체 갱신: " + subscribedStocks.length + "종목");
    }

    prevWatchlistCodes = currentCodes;
  } catch (e) {
    // entries 조회 실패는 다음 틱에 재시도
  }
}
setInterval(checkWatchlistMembershipChanges, 3000); // entries 자체는 5초 캐시라 3초 체크해도 실제 호출은 그만큼 안 늘어남

// 1분마다 갱신 - 1분봉 데이터라 이보다 자주 갱신해도 의미 없음
setInterval(refreshMiniCandlesForWatchlist, 60000);
setTimeout(refreshMiniCandlesForWatchlist, 8000); // 재시작 직후 워밍업 (토큰발급/entries조회 여유)

// 매일 장 시작 전(09:00 KST) 캐시 초기화 - 어제 데이터가 오늘 장중에도 잠깐 보이는 걸 방지.
// 09:01부터 refreshMiniCandlesForWatchlist가 새 거래일 데이터로 다시 채움.
let miniCandleCacheClearedDate = null;
setInterval(() => {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const dateKey = `${kst.getFullYear()}-${String(kst.getMonth() + 1).padStart(2, "0")}-${String(kst.getDate()).padStart(2, "0")}`;
  const minutes = kst.getHours() * 60 + kst.getMinutes();
  if (minutes >= 9 * 60 && minutes < 9 * 60 + 1 && miniCandleCacheClearedDate !== dateKey) {
    Object.keys(miniCandleCache).forEach((k) => delete miniCandleCache[k]);
    saveMiniCandleCache();
    Object.keys(todayMaxRateCache).forEach((k) => delete todayMaxRateCache[k]);
    Object.keys(prevOrderFlowCache).forEach((k) => delete prevOrderFlowCache[k]);
    group9LastCodes = [];
    miniCandleCacheClearedDate = dateKey;
    console.log("미니차트 캐시 초기화 (새 거래일 시작)");
  }
}, 30000);

const TRAIL_ACTIVATE_PCT = 2.0; // 이 손익률에 한 번이라도 도달하면 트레일링 스톱 활성화
const TRAIL_DISTANCE_PCT = 1.5; // 활성화 후 고점 대비 이만큼 밀리면 조기 청산
const positionPeaks = new Map(); // code -> 그 포지션이 지금까지 도달한 최고 손익률 (relay가 상주 프로세스라 메모리로 충분 - 재시작되면 초기화되지만 큰 문제 없음, 다음 상승에서 다시 쌓임)

async function checkWatchlistStopLoss() {
  if (!ADMIN_KEY) return; // 키 미설정이면 조용히 스킵 (fail closed)
  if (!isTradingActiveKST()) return; // 15:50 이후 자동매매 중지
  try {
    const items = await getWatchlistEntriesCached();
    if (!items.length) return;
    const stillHeldCodes = new Set(items.map((it) => it.code));
    for (const code of positionPeaks.keys()) {
      if (!stillHeldCodes.has(code)) positionPeaks.delete(code); // 이미 청산된 종목은 추적 그만(메모리 누수 방지)
    }
    for (const item of items) {
      const q = realtimeCache.stock[item.code];
      if (!q || !q.price) continue; // 아직 실시간가 미수신 - 다음 틱에 재시도
      const pnlPct = ((q.price - item.entry_price) / item.entry_price) * 100;

      const prevPeak = positionPeaks.get(item.code) || 0;
      const peak = Math.max(prevPeak, pnlPct);
      if (peak !== prevPeak) positionPeaks.set(item.code, peak);
      // 고정 +3.5% 익절선은 승률 35% 안팎 전략에서 드문 대승(오른쪽 꼬리)이 전체 기대값을
      // 만들어야 하는데 그 꼬리를 정확히 잘라내는 문제가 있었음(외부 분석: +3.9%, +4.6%로
      // 오버슈트하며 청산된 사례 확인). +2% 한 번이라도 도달하면 활성화되고, 그 뒤로 고점 대비
      // -1.5% 밀리면(최소 +0.5%는 확정 확보한 채로) 조기 확정 - 계속 오르면 익절선(+3.5%)까지 안
      // 잘리고 그대로 따라감.
      const trailingHit = peak >= TRAIL_ACTIVATE_PCT && pnlPct <= peak - TRAIL_DISTANCE_PCT;

      if (pnlPct <= AUTO_REMOVE_PNL_PCT || pnlPct >= AUTO_TAKE_PROFIT_PNL_PCT || trailingHit) {
        const reason = pnlPct >= 0 ? "익절" : "손절"; // trailingHit도 peak>=2%였으므로 pnlPct는 항상 +0.5% 이상 - 부호로 정확히 판정됨
        try {
          await workerRequest("/api/watchlist/auto-remove", "POST", { code: item.code, pnlPct, name: stockNameCache[item.code] });
          entriesCache.items = entriesCache.items.filter((x) => x.code !== item.code); // 즉시 캐시에서도 제거(중복삭제 요청 방지)
          positionPeaks.delete(item.code);
          const tag = trailingHit ? reason + "(트레일링)" : reason;
          console.log(`${tag} 자동삭제: ${item.code} (${pnlPct.toFixed(2)}%, 고점 ${peak.toFixed(2)}%)`);
        } catch (e) {
          console.log(`${reason} 자동삭제 요청 실패: ${item.code} - ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.log("관심종목 손절체크 실패: " + e.message);
  }
}
setInterval(checkWatchlistStopLoss, 2000);

// ---------- 15:50 일괄정리 (하루 1회) ----------
// 15:50부터는 신규 매매/자동삭제가 전부 중지되는데, 그 직전 시점 기준으로 조건(+3.5%/-1.5%)에
// 걸려있는 종목들은 중지되기 전에 한 번 정리해줌. 이후(15:50~장마감)엔 다시 매매중지 유지.
let finalSweepDoneToday = null;
async function runFinalSweep() {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const dateKey = `${kst.getFullYear()}-${String(kst.getMonth() + 1).padStart(2, "0")}-${String(kst.getDate()).padStart(2, "0")}`;
  if (finalSweepDoneToday === dateKey) return;
  if (!ADMIN_KEY) return;
  try {
    const entries = await workerRequest("/api/watchlist-entries", "GET");
    if (!entries.ok || !entries.items.length) {
      finalSweepDoneToday = dateKey;
      return;
    }
    const items = [];
    for (const item of entries.items) {
      const q = realtimeCache.stock[item.code];
      if (!q || !q.price) continue;
      const pnlPct = ((q.price - item.entry_price) / item.entry_price) * 100;
      // 예전엔 이미 +3.5%/-1.5% 조건을 넘긴 것만 정리하고, 그 사이(예: +1.5%)에 있는 포지션은
      // 그대로 밤새 들고 가게 뒀음. 다음날 개장 갭(장중 변동성과 무관하게 밤사이 뉴스·수급으로
      // 시가 자체가 크게 튀는 현상)에 그대로 노출돼서, 09:01 개장 직후 -10%/-6%/-5% 같은 대형
      // 손절이 무더기로 발생하는 원인이 됐음(-1.5% 손절 기준을 갭 하나로 몇 배씩 뚫어버림).
      // 이 시스템 자체가 장중 데이트레이딩(±1.5%/+3.5% 타이트한 리스크) 전제라 밤을 넘기는 순간
      // 그 전제가 깨지므로, 조건 충족 여부와 무관하게 남은 전량을 무조건 청산함.
      items.push({ code: item.code, pnlPct });
    }
    const result = await workerRequest("/api/watchlist/final-sweep", "POST", { items });
    if (result.ok) {
      console.log(`15:50 일괄정리 완료: ${result.removed}종목 삭제 (대상 ${items.length}건 중)`);
      finalSweepDoneToday = dateKey;
    } else {
      console.log("15:50 일괄정리 실패: " + (result.error || "unknown"));
    }
  } catch (e) {
    console.log("15:50 일괄정리 실패: " + e.message);
  }
}
// 15:50 정각을 정확히 맞추기보다 15:50~15:52 구간에서 1분 간격 체크 (매매중지 게이트가 15:50부터
// 걸리므로, 이 구간이 지나기 전에 반드시 한 번은 실행되어야 함).
setInterval(() => {
  const kst = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Seoul" }));
  const minutes = kst.getHours() * 60 + kst.getMinutes();
  if (minutes >= 15 * 60 + 50 && minutes <= 15 * 60 + 52) {
    runFinalSweep();
  }
}, 30000);

// ---------- HTTP 서버 (기존 REST 중계 + 신규 실시간 캐시 조회) ----------
// 조건검색 편입 이력에 이름/현재가를 붙여서 반환 - /realtime/all과 /realtime/condition 둘 다에서 씀
function buildConditionHistory() {
  const cond = realtimeCache.condition;
  return cond.history.map((h) => {
    const q = realtimeCache.stock[h.code];
    return {
      code: h.code,
      name: stockNameCache[h.code] || null,
      time: h.time,
      at: h.at,
      initial: !!h.initial,
      price: q ? q.price : null,
      rate: q ? q.rate : null,
      stillIn: cond.codes.indexOf(h.code) !== -1, // 아직 조건을 만족 중인지
    };
  });
}

const server = http.createServer((req, res) => {
  if (req.headers["x-relay-secret"] !== RELAY_SECRET) {
    res.writeHead(401, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "relay secret mismatch" }));
    return;
  }

  // SSE 스트리밍 - Worker가 이 연결을 열어두고 받는 대로 브라우저에 그대로 릴레이함.
  // 연결 직후 현재 스냅샷을 1회 즉시 보내고, 이후엔 값이 바뀔 때마다(최대 200ms 간격) push.
  if (req.url === "/realtime/stream") {
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      "connection": "keep-alive",
    });
    const cond = realtimeCache.condition;
    const initHistory = buildConditionHistory();
    res.write(`data: ${JSON.stringify({
      index: buildIndexPayload(),
      stocks: realtimeCache.stock,
      condition: { seq: cond.seq, name: cond.name, codes: cond.codes, count: cond.codes.length, lastEventAt: cond.lastEventAt, history: initHistory },
    })}\n\n`);
    sseClients.add(res);
    const keepAlive = setInterval(() => {
      try { res.write(": ping\n\n"); } catch (e) { clearInterval(keepAlive); sseClients.delete(res); }
    }, 20000); // 중간 프록시/타임아웃 방지용 주기적 코멘트 핑
    req.on("close", () => {
      clearInterval(keepAlive);
      sseClients.delete(res);
    });
    return;
  }

  // 실시간 지수 조회 - 웹소켓으로 받아둔 최신값을 즉시 반환 (키움 TR 호출 없음)
  // 지수+종목시세+조건검색을 한 번에 반환 - Worker가 이전엔 3개 엔드포인트를 따로 호출했는데,
  // 다 relay 메모리에서 읽는 거라 굳이 나눌 이유가 없어서 하나로 합침 (Worker<->relay 왕복 3번 -> 1번)
  if (req.url === "/realtime/all") {
    const cond = realtimeCache.condition;
    const history = buildConditionHistory();
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        wsConnected: wsConnected,
        wsLoggedIn: wsLoggedIn,
        index: buildIndexPayload(),
        stocks: relevantStocksPayload(),
        condition: { seq: cond.seq, name: cond.name, codes: cond.codes, count: cond.codes.length, lastEventAt: cond.lastEventAt, history },
      })
    );
    return;
  }

  if (req.url === "/realtime/index") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        wsConnected: wsConnected,
        wsLoggedIn: wsLoggedIn,
        ...buildIndexPayload(),
      })
    );
    return;
  }

  // 실시간 종목 구독 목록 갱신 - Worker가 종목 목록을 보내면 그걸로 교체
  // POST body: {"codes":[...], "listCodes":[...]}
  //   codes     -> 관심종목 (그룹2)
  //   listCodes -> 화면 리스트 종목 (그룹3)
  if (req.url === "/realtime/subscribe" && req.method === "POST") {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      let codes = [];
      let listCodes = [];
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString() || "{}");
        const valid = (arr) => (Array.isArray(arr) ? arr.filter((c) => /^[0-9A-Za-z]{6}$/.test(c)) : []);
        codes = valid(parsed.codes).slice(0, WATCH_RESERVED); // 관심종목 우선
        // 리스트는 총량에서 관심종목을 뺀 만큼만. 중복 종목은 제외(같은 종목 두 번 등록 방지)
        const remain = Math.max(0, TOTAL_STOCK_LIMIT - codes.length);
        listCodes = valid(parsed.listCodes)
          .filter((c) => !codes.includes(c))
          .slice(0, remain);
      } catch (e) {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: "invalid json" }));
        return;
      }

      // 순서만 바뀐 경우는 재등록 불필요 - 정렬해서 내용이 실제로 달라졌을 때만 갱신
      const sameSet = (a, b) => {
        if (a.length !== b.length) return false;
        const sa = [...a].sort(), sb = [...b].sort();
        return sa.every((v, i) => v === sb[i]);
      };
      const changedWatch = !sameSet(codes, subscribedStocks);
      const changedList = !sameSet(listCodes, subscribedListStocks);

      subscribedStocks = codes;
      subscribedListStocks = listCodes;

      const canSend = ws && ws.readyState === WebSocket.OPEN && wsLoggedIn;
      // 로그인 직후 3초는 registerSubscriptions()가 순차 전송 중이라, 여기서 끼어들면
      // 조건검색(CNSRREQ) 응답을 못 받는 경우가 있어 잠시 미룸 (목록은 이미 저장됐으니 다음 요청 때 반영됨)
      const settling = wsLoginAt && Date.now() - wsLoginAt < 3000;

      if (canSend && !settling && changedWatch && codes.length) {
        // refresh:"1"만으로는 기존 등록이 남아 누적되는 현상이 있어(200 초과 에러), 먼저 명시적으로 해제
        ws.send(JSON.stringify({ trnm: "REMOVE", grp_no: "2" }));
        ws.send(JSON.stringify({
          trnm: "REG", grp_no: "2", refresh: "1",
          data: [{ item: codes, type: ["0B"] }],
        }));
        console.log("관심종목 구독 갱신:", codes.length + "종목");
      }
      if (canSend && !settling && changedList && listCodes.length) {
        ws.send(JSON.stringify({ trnm: "REMOVE", grp_no: "3" }));
        ws.send(JSON.stringify({
          trnm: "REG", grp_no: "3", refresh: "1",
          data: [{ item: listCodes, type: ["0B"] }],
        }));
        console.log("리스트종목 구독 갱신:", listCodes.length + "종목");
      }

      // 세 그룹(관심종목/화면리스트/조건검색 실시간포착) 어디에도 없는 종목의 캐시만 정리
      if (changedWatch || changedList) {
        const keep = new Set([...codes, ...listCodes, ...realtimeCache.condition.codes]);
        for (const cached of Object.keys(realtimeCache.stock)) {
          if (!keep.has(cached)) delete realtimeCache.stock[cached];
        }
      }

      // 로그인 직후라 미뤘던 경우, 안정화된 뒤 한 번 자동으로 등록해줌
      if (canSend && settling && (changedWatch || changedList)) {
        setTimeout(() => {
          if (!ws || ws.readyState !== WebSocket.OPEN || !wsLoggedIn) return;
          if (subscribedStocks.length) {
            ws.send(JSON.stringify({
              trnm: "REG", grp_no: "2", refresh: "1",
              data: [{ item: subscribedStocks, type: ["0B"] }],
            }));
          }
          if (subscribedListStocks.length) {
            ws.send(JSON.stringify({
              trnm: "REG", grp_no: "3", refresh: "1",
              data: [{ item: subscribedListStocks, type: ["0B"] }],
            }));
          }
          console.log("지연 구독 등록 완료 (관심 " + subscribedStocks.length + " / 리스트 " + subscribedListStocks.length + ")");
        }, 3500);
      }

      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({
        ok: true,
        subscribedWatch: subscribedStocks.length,
        subscribedList: subscribedListStocks.length,
      }));
    });
    return;
  }

  // 실시간 종목 시세 조회 - 웹소켓으로 받아둔 최신 체결값 반환
  if (req.url === "/realtime/stocks") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        wsConnected: wsConnected,
        wsLoggedIn: wsLoggedIn,
        subscribed: subscribedStocks.length,
        subscribedList: subscribedListStocks.length,
        stocks: relevantStocksPayload(),
      })
    );
    return;
  }

  // 조건검색 실시간 결과 조회 - 현재 조건을 만족하는 종목 목록 + 최근 편입/이탈 이벤트
  if (req.url === "/realtime/condition") {
    const cond = realtimeCache.condition;
    const history = buildConditionHistory();
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        wsConnected: wsConnected,
        wsLoggedIn: wsLoggedIn,
        seq: cond.seq,
        name: cond.name,
        codes: cond.codes,
        count: cond.codes.length,
        lastEventAt: cond.lastEventAt,
        names: stockNameCache,
        history: history,
        events: cond.events.slice(0, 20),
      })
    );
    return;
  }

  // SNS 급등 조짐 판단용 - 네이버 종목토론방 게시글수 조회. 종목코드 6자리(영문 포함)만 받고
  // 그 값을 finance.naver.com의 고정 URL 패턴에 끼워넣는 것 외에는 임의 URL을 받지 않음(오픈
  // 프록시 방지). Cloudflare Worker에서 직접 네이버 페이지를 스크래핑하면 차단된 전례가 있어서
  // (worker.js 상단 주석 참고) relay를 거쳐 우회함. 페이지 구조가 바뀔 수 있으므로 여러 패턴으로
  // 방어적으로 파싱하고, 못 찾으면 null을 반환해 호출측이 조용히 건너뛰게 함.
  if (req.url.startsWith("/proxy/naver-board")) {
    const u = new URL(req.url, "http://localhost");
    const code = u.searchParams.get("code");
    if (!code || !/^[0-9A-Za-z]{6}$/.test(code)) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "code 형식 오류" }));
      return;
    }
    https.get(
      `https://finance.naver.com/item/board.naver?code=${code}`,
      { headers: { "User-Agent": "Mozilla/5.0", Referer: "https://finance.naver.com/" }, timeout: 5000 },
      (pageRes) => {
        let body = "";
        pageRes.on("data", (chunk) => { body += chunk; });
        pageRes.on("end", () => {
          // 페이지 구조가 자주 바뀌므로 여러 패턴을 순서대로 시도함:
          // 1) "총 N건" 류의 텍스트
          // 2) 페이지네이션의 최대 page= 번호 * 페이지당 20건(네이버 게시판 관례값)으로 근사
          let totalPosts = null;
          const totalMatch = body.match(/총\s*([\d,]+)\s*건/);
          if (totalMatch) {
            totalPosts = parseInt(totalMatch[1].replace(/,/g, ""), 10);
          } else {
            const pageNums = [...body.matchAll(/[?&]page=(\d+)/g)].map((m) => parseInt(m[1], 10));
            if (pageNums.length) totalPosts = Math.max(...pageNums) * 20; // 근사치 - 상대적 증감 판단용이라 정밀할 필요 없음
          }
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: totalPosts !== null, totalPosts }));
        });
      }
    ).on("error", (e) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: e.message }));
    }).on("timeout", function () {
      this.destroy();
    });
    return;
  }

  // 관심종목 현재가 즉시조회 - realtimeCache.stock에 값이 없는 종목(웹소켓 구독 전, 장마감 후
  // 재시작 등)을 Worker가 요청하면 그 자리에서 키움 개별시세(ka10007)를 조회해서 바로 채워줌.
  // 조회 결과는 realtimeCache.stock에도 반영해서 다음 요청부턴 캐시로 즉시 응답됨.
  // 이미 최근(30초 이내) 조회한 값이 있으면 재조회하지 않고 그대로 반환 - /api/latest가 반복
  // 호출될 때마다 매번 키움을 다시 두드리는 낭비 방지.
  if (req.url.startsWith("/realtime/quote-now")) {
    const q = new URL(req.url, "http://localhost").searchParams;
    const code = q.get("code");
    if (!code) {
      res.writeHead(400, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "code 누락" }));
      return;
    }
    const existing = realtimeCache.stock[code];
    if (existing && existing.updatedAt && Date.now() - new Date(existing.updatedAt).getTime() < 30000) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true, price: existing.price, rate: existing.rate, volume: existing.volume || 0 }));
      return;
    }
    (async () => {
      try {
        const token = await issueTokenCached();
        const raw = await kiwoomQuoteRelay(code, token);
        const parsed = parseKiwoomQuoteRelay(raw);
        if (parsed.price > 0) {
          realtimeCache.stock[code] = { ...parsed, cntrStr: 0, time: "", updatedAt: new Date().toISOString() };
        }
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: parsed.price > 0, price: parsed.price, rate: parsed.rate, volume: parsed.volume }));
      } catch (e) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ ok: false, error: e.message }));
      }
    })();
    return;
  }

  // 관심종목 미니차트(1분봉) 캐시 조회 - Worker의 /api/mini-candles가 이걸 우선 사용해서
  // 매번 종목당 1.1초 순차조회하던 걸 즉시 응답으로 바꿈 (relay가 백그라운드로 미리 갱신해둠).
  if (req.url.startsWith("/realtime/mini-candles")) {
    const q = new URL(req.url, "http://localhost").searchParams;
    const code = q.get("code");
    const cached = code && miniCandleCache[code];
    res.writeHead(200, { "content-type": "application/json" });
    if (cached) {
      res.end(JSON.stringify({ ok: true, candles: cached.candles, tradingDate: cached.tradingDate, updatedAt: cached.updatedAt }));
    } else {
      res.end(JSON.stringify({ ok: false, error: "캐시 없음(관심종목이 아니거나 아직 미갱신)" }));
    }
    return;
  }

  // 관심종목 미니차트 전체 일괄조회 - Worker의 /api/latest가 페이지 로드 시 이걸 한 번에 받아가서
  // 응답에 포함시킴. 종목별로 /api/mini-candles를 따로따로 부르던 왕복(브라우저<->Worker<->relay)을
  // 아예 없애서 첫 로드 시 차트가 별도 요청 없이 즉시 뜨게 함(가장 빠른 경로).
  if (req.url === "/realtime/mini-candles-all") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, cache: miniCandleCache }));
    return;
  }

  // 웹소켓 상태 확인용 (헬스체크에서 씀)
  if (req.url === "/realtime/status") {
    const mem = process.memoryUsage();
    res.writeHead(200, { "content-type": "application/json" });
    res.end(
      JSON.stringify({
        ok: true,
        wsConnected: wsConnected,
        wsLoggedIn: wsLoggedIn,
        lastMessageAt: wsLastMessageAt ? new Date(wsLastMessageAt).toISOString() : null,
        cachedIndexCount: Object.keys(realtimeCache.index).length,
        subscribedStockCount: subscribedStocks.length,
        subscribedListCount: subscribedListStocks.length,
        cachedStockCount: Object.keys(realtimeCache.stock).length,
        conditionSeq: realtimeCache.condition.seq,
        conditionCount: realtimeCache.condition.codes.length,
        sseClientCount: sseClients.size,
        memoryRssMb: Math.round(mem.rss / 1024 / 1024),
        memoryHeapUsedMb: Math.round(mem.heapUsed / 1024 / 1024),
      })
    );
    return;
  }

  // 그 외는 기존대로 키움 REST로 그대로 중계
  const chunks = [];
  req.on("data", (chunk) => chunks.push(chunk));
  req.on("end", () => {
    const body = Buffer.concat(chunks);

    const forwardHeaders = {};
    for (const [key, value] of Object.entries(req.headers)) {
      if (["content-type", "authorization", "cont-yn", "next-key", "api-id"].includes(key.toLowerCase())) {
        forwardHeaders[key] = value;
      }
    }
    if (body.length) forwardHeaders["content-length"] = Buffer.byteLength(body);

    const upstreamReq = https.request(
      {
        hostname: KIWOOM_REAL_HOST,
        path: req.url,
        method: req.method,
        headers: forwardHeaders,
      },
      (upstreamRes) => {
        res.writeHead(upstreamRes.statusCode, upstreamRes.headers);
        upstreamRes.pipe(res);
      }
    );

    upstreamReq.on("error", (err) => {
      res.writeHead(502, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: "중계서버 -> 키움 요청 실패: " + err.message }));
    });

    upstreamReq.end(body);
  });
});

server.listen(PORT, () => {
  console.log(`키움 중계서버 실행 중: 포트 ${PORT}`);
});
