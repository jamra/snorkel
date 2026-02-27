#!/usr/bin/env python3
"""
Generate fake OpenTelemetry trace data and send to Snorkel.

Usage: python3 generate_traces.py [num_traces] [endpoint]
"""

import json
import random
import string
import sys
import time
import urllib.request
import urllib.error

NUM_TRACES = int(sys.argv[1]) if len(sys.argv) > 1 else 10
ENDPOINT = sys.argv[2] if len(sys.argv) > 2 else "http://localhost:8080"

# Services in our fake distributed system
SERVICES = {
    "api-gateway": ["POST /api/orders", "GET /api/users/{id}", "GET /api/products", "POST /api/checkout"],
    "user-service": ["GetUser", "ValidateToken", "UpdateProfile", "ListUsers"],
    "order-service": ["CreateOrder", "GetOrder", "ListOrders", "CancelOrder"],
    "payment-service": ["ProcessPayment", "RefundPayment", "ValidateCard", "GetPaymentStatus"],
    "inventory-service": ["CheckStock", "ReserveItems", "ReleaseItems", "UpdateStock"],
    "notification-service": ["SendEmail", "SendSMS", "SendPush", "QueueNotification"],
}

def random_hex(length):
    return ''.join(random.choices('abcdef0123456789', k=length))

def now_nanos():
    return int(time.time() * 1_000_000_000)

def generate_trace():
    trace_id = random_hex(32)
    start_ns = now_nanos()

    # Root span (API Gateway)
    root_span_id = random_hex(16)
    root_op = random.choice(SERVICES["api-gateway"])
    root_duration_ms = random.randint(50, 250)
    root_end_ns = start_ns + (root_duration_ms * 1_000_000)

    # Randomly add error
    root_status = {"code": 1}  # OK
    if random.randint(0, 9) == 0:
        root_status = {"code": 2, "message": "Internal server error"}

    spans = [{
        "traceId": trace_id,
        "spanId": root_span_id,
        "name": root_op,
        "kind": 2,  # SERVER
        "startTimeUnixNano": str(start_ns),
        "endTimeUnixNano": str(root_end_ns),
        "attributes": [
            {"key": "http.method", "value": {"stringValue": "POST"}},
            {"key": "http.url", "value": {"stringValue": root_op}},
            {"key": "http.status_code", "value": {"intValue": "200"}}
        ],
        "status": root_status
    }]

    # Add 2-4 child spans
    num_children = random.randint(2, 4)
    child_start = start_ns + 5_000_000  # 5ms after root

    backend_services = [s for s in SERVICES.keys() if s != "api-gateway"]

    for i in range(num_children):
        service = random.choice(backend_services)
        span_id = random_hex(16)
        op = random.choice(SERVICES[service])
        duration_ms = random.randint(10, 100)
        end_ns = child_start + (duration_ms * 1_000_000)

        status = {"code": 1}
        if random.randint(0, 14) == 0:
            status = {"code": 2, "message": "Service unavailable"}

        spans.append({
            "traceId": trace_id,
            "spanId": span_id,
            "parentSpanId": root_span_id,
            "name": op,
            "kind": 2,  # SERVER
            "startTimeUnixNano": str(child_start),
            "endTimeUnixNano": str(end_ns),
            "attributes": [
                {"key": "rpc.service", "value": {"stringValue": service}},
                {"key": "rpc.method", "value": {"stringValue": op}}
            ],
            "status": status
        })

        # Sometimes add grandchild
        if random.randint(0, 2) == 0:
            gc_service = random.choice(backend_services)
            gc_span_id = random_hex(16)
            gc_op = random.choice(SERVICES[gc_service])
            gc_start = child_start + 2_000_000
            gc_duration = random.randint(5, 30)
            gc_end = gc_start + (gc_duration * 1_000_000)

            spans.append({
                "traceId": trace_id,
                "spanId": gc_span_id,
                "parentSpanId": span_id,
                "name": gc_op,
                "kind": 1,  # INTERNAL
                "startTimeUnixNano": str(gc_start),
                "endTimeUnixNano": str(gc_end),
                "attributes": [
                    {"key": "rpc.service", "value": {"stringValue": gc_service}}
                ],
                "status": {"code": 1}
            })

        child_start = end_ns + 1_000_000  # 1ms gap

    # Build OTLP request
    return {
        "resourceSpans": [{
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "api-gateway"}},
                    {"key": "service.version", "value": {"stringValue": "1.0.0"}},
                    {"key": "deployment.environment", "value": {"stringValue": "production"}}
                ]
            },
            "scopeSpans": [{
                "scope": {"name": "snorkel-test", "version": "1.0"},
                "spans": spans
            }]
        }]
    }

def send_trace(payload):
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(
        f"{ENDPOINT}/v1/traces",
        data=data,
        headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(req) as response:
            return response.status, response.read().decode('utf-8')
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode('utf-8')
    except Exception as e:
        return 0, str(e)

def main():
    print(f"Generating {NUM_TRACES} traces and sending to {ENDPOINT}/v1/traces...")
    print()

    success = 0
    failed = 0

    for i in range(1, NUM_TRACES + 1):
        payload = generate_trace()
        status, body = send_trace(payload)

        if status == 200:
            success += 1
            print(f"[{i}/{NUM_TRACES}] Sent trace successfully")
        else:
            failed += 1
            print(f"[{i}/{NUM_TRACES}] Failed (HTTP {status}): {body[:100]}")

        time.sleep(0.1)  # Small delay to spread timestamps

    print()
    print(f"Done! Sent {success} traces successfully, {failed} failed.")
    print()
    print(f"View traces at: {ENDPOINT} (click 'Traces' tab)")
    print()
    print("Or query via SQL:")
    print("  SELECT service_name, span_name, COUNT(*), AVG(duration_ms)")
    print("  FROM otel_traces")
    print("  GROUP BY service_name, span_name")

if __name__ == "__main__":
    main()
