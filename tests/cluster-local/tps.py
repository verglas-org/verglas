#!/usr/bin/env python3
"""Client-observed write TPS against Verglas vs the origin, across concurrency.

The write-back path acknowledges on an EC quorum instead of an origin PUT, so
per-object latency is not the figure of merit — sustained client transactions
per second is. A latency-bound path scales with concurrency; a throughput-bound
one plateaus. Running both endpoints at each level shows which.
"""
import concurrent.futures as cf, os, sys, time, uuid
import boto3
from botocore.config import Config

OBJ = int(os.environ.get("OBJECT_BYTES", "4096"))
N = int(os.environ.get("COUNT", "400"))
LEVELS = [int(x) for x in os.environ.get("LEVELS", "1,4,16,64").split(",")]
BODY = os.urandom(OBJ)


def clients(endpoints, key, secret):
    """One client per ingress. A cloud deployment puts a load balancer in front
    of every node, so pinning all load on one node measures a single node's
    serialization rather than the cluster's capacity."""
    return [client(e, key, secret) for e in endpoints]


def client(endpoint, key, secret):
    return boto3.client(
        "s3", endpoint_url=endpoint,
        aws_access_key_id=key, aws_secret_access_key=secret,
        region_name="us-east-1",
        config=Config(max_pool_connections=256, retries={"max_attempts": 1},
                      s3={"addressing_style": "path"}),
    )


def run(pool, prefix, n, conc):
    def put(i):
        # Round-robin across ingresses, standing in for the cloud load balancer.
        c = pool[i % len(pool)]
        t0 = time.perf_counter()
        c.put_object(Bucket="verglas-test", Key=f"{prefix}/{i}", Body=BODY)
        return time.perf_counter() - t0

    start = time.perf_counter()
    with cf.ThreadPoolExecutor(max_workers=conc) as ex:
        lat = list(ex.map(put, range(n)))
    wall = time.perf_counter() - start
    lat.sort()
    return {"tps": n / wall, "wall": wall,
            "p50_ms": lat[len(lat) // 2] * 1000,
            "p99_ms": lat[int(len(lat) * 0.99)] * 1000}


def main():
    vg_endpoints = os.environ.get(
        "S3_ENDPOINTS", os.environ.get("S3_ENDPOINT", "http://127.0.0.1:18333")
    ).split(",")
    vg = clients(vg_endpoints, "verglas-engine", "verglas-engine-secret")
    direct = clients([os.environ.get("ORIGIN_ENDPOINT", "http://127.0.0.1:19000")],
                     "verglas-local", "verglas-local-secret")
    print(f"verglas ingresses: {len(vg_endpoints)} ({', '.join(vg_endpoints)})")
    tag = uuid.uuid4().hex[:8]
    print(f"{'conc':>5} {'verglas_tps':>12} {'direct_tps':>11} {'ratio':>7} "
          f"{'vg_p50':>8} {'vg_p99':>8} {'dir_p50':>8}")
    for conc in LEVELS:
        v = run(vg, f"tps-{tag}-vg-{conc}", N, conc)
        d = run(direct, f"tps-{tag}-dir-{conc}", N, conc)
        print(f"{conc:>5} {v['tps']:>12.1f} {d['tps']:>11.1f} "
              f"{v['tps']/d['tps']:>7.2f} {v['p50_ms']:>8.1f} {v['p99_ms']:>8.1f} "
              f"{d['p50_ms']:>8.1f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
