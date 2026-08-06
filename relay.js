const http = require("http");
const https = require("https");
const WebSocket = require("ws");

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
      realtimeCache.stock[entry.item] = {
        price: Math.abs(parseSignedNumber(entry.values["10"])),
        rate: parseSignedNumber(entry.values["12"]),
        volume: parseSignedNumber(entry.values["13"]),
        cntrStr: parseSignedNumber(entry.values["228"]),
        time: entry.values["20"] || "",
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
}

function registerSubscriptions() {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;

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

connectWebSocket();

// ---------- 관심종목 손절 자동체크 (10초 주기) ----------
// Worker의 2분 cron(checkWatchlistRiskLevels)보다 훨씬 빠르게 -1.5% 손절 트리거.
// relay는 이미 웹소켓으로 실시간가를 들고 있으므로 키움 TR 호출 없이 즉시 계산 가능.
const AUTO_REMOVE_PNL_PCT = -1.5;
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

async function checkWatchlistStopLoss() {
  if (!ADMIN_KEY) return; // 키 미설정이면 조용히 스킵 (fail closed)
  try {
    const entries = await workerRequest("/api/watchlist-entries", "GET");
    if (!entries.ok || !entries.items.length) return;
    for (const item of entries.items) {
      const q = realtimeCache.stock[item.code];
      if (!q || !q.price) continue; // 아직 실시간가 미수신 - 다음 틱에 재시도
      const pnlPct = ((q.price - item.entry_price) / item.entry_price) * 100;
      if (pnlPct <= AUTO_REMOVE_PNL_PCT) {
        try {
          await workerRequest("/api/watchlist/auto-remove", "POST", { code: item.code, pnlPct, name: stockNameCache[item.code] });
          console.log(`손절 자동삭제: ${item.code} (${pnlPct.toFixed(2)}%)`);
        } catch (e) {
          console.log(`손절 자동삭제 요청 실패: ${item.code} - ${e.message}`);
        }
      }
    }
  } catch (e) {
    console.log("관심종목 손절체크 실패: " + e.message);
  }
}
setInterval(checkWatchlistStopLoss, 10000);

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
        index: { kospi: realtimeCache.index["001"] || null, kosdaq: realtimeCache.index["101"] || null },
        stocks: realtimeCache.stock,
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
        kospi: realtimeCache.index["001"] || null,
        kosdaq: realtimeCache.index["101"] || null,
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

      // 두 그룹 어디에도 없는 종목의 캐시는 정리 (오래된 값이 남아 오해를 주지 않도록)
      if (changedWatch || changedList) {
        const keep = new Set([...codes, ...listCodes]);
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
        stocks: realtimeCache.stock,
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
