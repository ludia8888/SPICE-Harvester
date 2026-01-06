import http from "k6/http";
import { check, sleep } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

const BASE_URL = (__ENV.K6_BASE_URL || "http://host.docker.internal:8002/api/v1").replace(/\/+$/, "");
const ADMIN_TOKEN = __ENV.K6_ADMIN_TOKEN || __ENV.ADMIN_TOKEN || "change_me";
const BRANCH = __ENV.K6_BRANCH || "main";

const headers = {
  "Content-Type": "application/json",
  "X-Admin-Token": ADMIN_TOKEN,
  "Accept-Language": "en",
};

const submitMs = new Trend("async_submit_ms", true);
const completeMs = new Trend("async_complete_ms", true);
const pollsPerCommand = new Trend("async_poll_count");
const commandsTotal = new Counter("async_commands_total");
const commandOk = new Rate("async_command_ok");
const commandFailed = new Rate("async_command_failed");
const commandTimedOut = new Rate("async_command_timed_out");

const pollResponseOk = http.expectedStatuses({ min: 200, max: 399 }, 404);

export const options = {
  scenarios: {
    async_instance_create: {
      executor: "shared-iterations",
      vus: parseInt(__ENV.K6_VUS || "5", 10),
      iterations: parseInt(__ENV.K6_ITERATIONS || "20", 10),
      maxDuration: __ENV.K6_MAX_DURATION || "10m",
    },
  },
  thresholds: {
    http_req_failed: ["rate<0.02"],
    async_command_failed: ["rate<0.02"],
    async_command_timed_out: ["rate<0.02"],
  },
};

function api(path) {
  const normalized = path.startsWith("/") ? path : `/${path}`;
  return `${BASE_URL}${normalized}`;
}

function safeJson(res) {
  try {
    return res.json();
  } catch (_) {
    return null;
  }
}

function extractCommandId(payload) {
  if (!payload || typeof payload !== "object") return null;
  if (typeof payload.command_id === "string" && payload.command_id.length) return payload.command_id;
  if (payload.data && typeof payload.data.command_id === "string" && payload.data.command_id.length) {
    return payload.data.command_id;
  }
  return null;
}

function waitForCommand(commandId, { timeoutMs = 60000, pollIntervalMs = 500 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let polls = 0;
  let lastStatus = "UNKNOWN";

  while (Date.now() < deadline) {
    polls += 1;

    const res = http.get(api(`/commands/${commandId}/status`), {
      headers,
      responseCallback: pollResponseOk,
    });
    if (res.status === 404) {
      lastStatus = "NOT_FOUND";
      sleep(pollIntervalMs / 1000);
      continue;
    }
    if (res.status !== 200) {
      lastStatus = `HTTP_${res.status}`;
      sleep(pollIntervalMs / 1000);
      continue;
    }

    const body = safeJson(res) || {};
    const status = body.status || body.state || body.command_status || "UNKNOWN";
    lastStatus = status;

    if (status === "COMPLETED" || status === "FAILED" || status === "CANCELLED") {
      return { status, polls, body };
    }

    sleep(pollIntervalMs / 1000);
  }

  return { status: "TIMEOUT", polls, body: null, lastStatus };
}

function randHex(len) {
  let out = Math.random().toString(16).slice(2);
  while (out.length < len) out += "0";
  return out.slice(0, len);
}

function makeDbName() {
  const seed = __ENV.K6_RUN_ID || `${Date.now()}_${randHex(6)}`;
  return `perf_${seed}`
    .toLowerCase()
    .replace(/[^a-z0-9_-]/g, "_")
    .slice(0, 48);
}

export function setup() {
  // 1) Create a dedicated DB for this run (and delete it in teardown).
  const dbName = makeDbName();
  // Printed so operators can manually clean up if teardown can't delete (e.g., missing expected-seq helper).
  console.log(`[k6] perf db_name=${dbName}`);
  const createDbRes = http.post(
    api("/databases"),
    JSON.stringify({ name: dbName, description: "k6 async instance perf" }),
    { headers }
  );
  check(createDbRes, {
    "db create accepted": (r) => r.status === 202 || r.status === 201,
  });

  const createDbBody = safeJson(createDbRes);
  const dbCmd = extractCommandId(createDbBody);
  if (dbCmd) {
    const dbWait = waitForCommand(dbCmd, { timeoutMs: 180000, pollIntervalMs: 750 });
    check(dbWait, { "db create completed": (r) => r.status === "COMPLETED" });
  }

  // 2) Create a minimal ontology class for instance creation.
  const classId = "LoadTestItem";
  const ontology = {
    id: classId,
    label: "LoadTestItem",
    description: "k6 perf load test class",
    properties: [
      // InstanceWorker always writes `{class_id.lower()}_id` into TerminusDB nodes.
      // If the ontology doesn't declare it, Terminus rejects inserts (schema check failure).
      { name: "loadtestitem_id", type: "xsd:string", label: "LoadTestItem ID", required: true },
      { name: "name", type: "xsd:string", label: "Name", required: false },
    ],
    relationships: [],
  };

  const createOntRes = http.post(
    api(`/databases/${dbName}/ontology?branch=${encodeURIComponent(BRANCH)}`),
    JSON.stringify(ontology),
    { headers }
  );
  check(createOntRes, { "ontology accepted": (r) => r.status === 202 || r.status === 200 });

  const createOntBody = safeJson(createOntRes);
  const ontCmd = extractCommandId(createOntBody);
  if (ontCmd) {
    const ontWait = waitForCommand(ontCmd, { timeoutMs: 180000, pollIntervalMs: 750 });
    check(ontWait, { "ontology completed": (r) => r.status === "COMPLETED" });
  }

  return { dbName, classId, branch: BRANCH };
}

export default function (data) {
  const itemId = `item_${data.dbName}_${__VU}_${__ITER}_${randHex(6)}`;
  const payload = {
    data: {
      // Pass internal property IDs directly (BFF supports this for advanced clients).
      loadtestitem_id: itemId,
      name: `Load Test ${itemId}`,
    },
    metadata: { perf: true },
  };

  const submitStart = Date.now();
  const res = http.post(
    api(
      `/databases/${data.dbName}/instances/${encodeURIComponent(
        data.classId
      )}/create?branch=${encodeURIComponent(data.branch)}`
    ),
    JSON.stringify(payload),
    { headers }
  );
  submitMs.add(Date.now() - submitStart);

  const ok = check(res, { "instance create accepted": (r) => r.status === 202 });
  if (!ok) {
    commandsTotal.add(1);
    commandOk.add(false);
    commandFailed.add(true);
    commandTimedOut.add(false);
    return;
  }

  const body = safeJson(res);
  const cmdId = extractCommandId(body) || (body && body.command_id);
  if (!cmdId) {
    commandsTotal.add(1);
    commandOk.add(false);
    commandFailed.add(true);
    commandTimedOut.add(false);
    return;
  }

  const waitStart = Date.now();
  const result = waitForCommand(cmdId, { timeoutMs: 90000, pollIntervalMs: 500 });
  completeMs.add(Date.now() - waitStart);
  pollsPerCommand.add(result.polls);
  commandsTotal.add(1);

  const succeeded = result.status === "COMPLETED";
  const timedOutNow = result.status === "TIMEOUT";
  commandOk.add(succeeded);
  commandTimedOut.add(timedOutNow);
  commandFailed.add(!succeeded && !timedOutNow);

  // Small think time to avoid tight polling loops when users increase iterations.
  sleep(parseFloat(__ENV.K6_THINK_TIME_SECONDS || "0.1"));
}

export function teardown(data) {
  // Best-effort cleanup: delete DB (requires expected_seq).
  const seqRes = http.get(api(`/databases/${data.dbName}/expected-seq`), { headers });
  if (seqRes.status !== 200) {
    if (seqRes.status === 404) {
      console.log(
        `[k6] skipping DB cleanup (expected-seq endpoint not found); db_name=${data.dbName}`
      );
    }
    return;
  }

  const seqBody = safeJson(seqRes) || {};
  const expectedSeq =
    (seqBody.data && typeof seqBody.data.expected_seq === "number" && seqBody.data.expected_seq) ||
    (typeof seqBody.expected_seq === "number" && seqBody.expected_seq) ||
    0;

  const delRes = http.del(api(`/databases/${data.dbName}?expected_seq=${expectedSeq}`), null, { headers });
  if (delRes.status !== 202 && delRes.status !== 200) return;

  const delBody = safeJson(delRes);
  const delCmd = extractCommandId(delBody);
  if (delCmd) {
    waitForCommand(delCmd, { timeoutMs: 180000, pollIntervalMs: 1000 });
  }
}
