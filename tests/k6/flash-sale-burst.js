import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    flash_sale_spike: {
      executor: 'per-vu-iterations',
      vus: 5000,
      iterations: 1,
      maxDuration: '30s',
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<25'], // SLA: 99% requests under 25ms
  },
};

export default function () {
  const url = 'http://localhost:8080/api/v1/orders';
  const payload = JSON.stringify({
    userId: `user_${__VU}`,
    productId: 'iphone-16-pro',
    quantity: 1,
    unitPrice: 999.00,
  });

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'Idempotency-Key': `key_${__VU}_${uuidv4()}`,
    },
  };

  const res = http.post(url, payload, params);

  check(res, {
    'Valid Status (202 Accepted or 409 Conflict)': (r) => r.status === 202 || r.status === 409,
  });
}
