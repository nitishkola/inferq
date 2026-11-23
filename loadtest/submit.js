// k6 load test for the submit API alone (how many submissions/s the API + Redis absorb).
// For end-to-end latency use bench/bench.py instead.
//   k6 run -e RATE=500 loadtest/submit.js
import http from "k6/http";
import { check } from "k6";

const BASE = __ENV.BASE_URL || "http://localhost:8080";

export const options = {
  scenarios: {
    submit: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.RATE || 200),
      timeUnit: "1s",
      duration: __ENV.DURATION || "60s",
      preAllocatedVUs: 50,
      maxVUs: 500,
    },
  },
  thresholds: {
    http_req_duration: ["p(99)<50"],
    checks: ["rate>0.99"],
  },
};

export default function () {
  const tenant = `k6-${Math.floor(Math.random() * 50)}`;
  const res = http.post(
    `${BASE}/v1/jobs`,
    JSON.stringify({ prompt: "load test prompt", maxTokens: 32 }),
    { headers: { "Content-Type": "application/json", "X-Tenant-Id": tenant } },
  );
  check(res, { "accepted or backpressured": (r) => [202, 429, 503].includes(r.status) });
}
