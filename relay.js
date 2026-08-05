// 자동배포 테스트

const http = require("http");
const https = require("https");

const PORT = process.env.PORT || 8787;
const RELAY_SECRET = process.env.RELAY_SECRET;
const KIWOOM_REAL_HOST = "api.kiwoom.com";

if (!RELAY_SECRET) {
  console.error("RELAY_SECRET 환경변수가 없습니다.");
  process.exit(1);
}

const server = http.createServer((req, res) => {
  if (req.headers["x-relay-secret"] !== RELAY_SECRET) {
    res.writeHead(401, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: false, error: "relay secret mismatch" }));
    return;
  }

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
