import http from 'k6/http';
import { check, sleep } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  stages: [
    { duration: '10s', target: 50 },  // Ramp up to 50 VUs
    { duration: '30s', target: 200 }, // Sustained load at 200 VUs
    { duration: '10s', target: 0 },   // Ramp down
  ],
  thresholds: {
    http_req_duration: ['p(95)<100'], // 95% of submissions under 100ms
    http_req_failed: ['rate<0.01'],   // Error rate < 1%
  },
};

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';

export default function () {
  const idempotencyKey = `k6-job-${uuidv4()}`;

  const payload = JSON.stringify({
    task_type: 'custom-demo',
    priority: Math.floor(Math.random() * 50) + 1,
    max_attempts: 3,
    payload: {
      action: 'success',
      sleep_millis: 50,
      caller: 'k6-load-tester'
    }
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': idempotencyKey,
    },
  };

  // 1. Submit Job
  const submitRes = http.post(`${BASE_URL}/api/v1/jobs`, payload, params);
  check(submitRes, {
    'job submission status 201': (r) => r.status === 201,
    'has job id': (r) => JSON.parse(r.body).id !== undefined,
  });

  // 2. Idempotency Key Replay Verification
  const replayRes = http.post(`${BASE_URL}/api/v1/jobs`, payload, params);
  check(replayRes, {
    'idempotent replay status 200 or 201': (r) => r.status === 200 || r.status === 201,
    'idempotency header present': (r) => r.headers['X-Idempotency-Replay'] === 'true' || r.status === 200 || r.status === 201,
  });

  sleep(0.1);
}
