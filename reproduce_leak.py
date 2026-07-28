#!/usr/bin/env python3
import time
import subprocess
import json
import urllib.request
import urllib.parse
import os
import sys
import threading

def post_ch_query(host, port, sql):
    url = f"http://{host}:{port}/"
    try:
        req = urllib.request.Request(url, data=sql.encode('utf-8'))
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.read().decode('utf-8')
    except Exception as e:
        return str(e)

def get_ch_metrics(host, port):
    query = "SELECT metric, value FROM system.asynchronous_metrics WHERE metric IN ('jemalloc.allocated', 'TrackedMemory', 'MemoryResident')"
    url = f"http://{host}:{port}/?query=" + urllib.parse.quote(query) + "+FORMAT+JSONEachRow"
    try:
        req = urllib.request.urlopen(url, timeout=5)
        lines = req.read().decode('utf-8').strip().split('\n')
        metrics = {}
        for line in lines:
            if not line:
                continue
            row = json.loads(line)
            metrics[row['metric']] = float(row['value'])
        return metrics
    except Exception as e:
        return {'error': str(e)}

def get_docker_memory(container_name):
    try:
        out = subprocess.check_output(['docker', 'inspect', '--format', '{{.State.Pid}}', container_name]).decode().strip()
        pid = out
        cgroup_out = subprocess.check_output(['awk', '-F:', '$1 == "0" {print $3}', f'/proc/{pid}/cgroup']).decode().strip()
        cgroup_path = f'/sys/fs/cgroup{cgroup_out}/memory.current'
        if os.path.exists(cgroup_path):
            with open(cgroup_path, 'r') as f:
                return int(f.read().strip())
    except Exception:
        pass
    return 0

def init_target(host, port):
    post_ch_query(host, port, "CREATE DATABASE IF NOT EXISTS main_db ENGINE = Atomic")
    post_ch_query(host, port, "CREATE TABLE IF NOT EXISTS main_db.target_table (id UInt64, p Date, val String, extra_col String) ENGINE = MergeTree PARTITION BY p ORDER BY id")

def single_pipeline_worker(thread_id, host, port, duration_sec):
    start_time = time.time()
    batch_id = 0

    while time.time() - start_time < duration_sec:
        batch_id += 1
        pipeline_db = f"pipeline_{thread_id}_{batch_id % 5}"
        stage_table = f"batch_{thread_id}_{batch_id}"

        # 1. CREATE DATABASE & STAGE TABLE
        post_ch_query(host, port, f"CREATE DATABASE IF NOT EXISTS {pipeline_db} ENGINE = Atomic")
        create_sql = f"CREATE TABLE IF NOT EXISTS {pipeline_db}.{stage_table} (id UInt64, p Date, val String) ENGINE = MergeTree PARTITION BY p ORDER BY id"
        post_ch_query(host, port, create_sql)

        # 2. DESCRIBE TABLE METADATA
        post_ch_query(host, port, f"DESCRIBE TABLE {pipeline_db}.{stage_table} SETTINGS describe_include_subcolumns=0")

        # 3. INSERT BATCH 1
        insert1_sql = f"INSERT INTO {pipeline_db}.{stage_table} (id, p, val) SELECT number, '2026-07-28', concat('val_', toString(number)) FROM numbers(1000)"
        post_ch_query(host, port, insert1_sql)

        # 4. ALTER TABLE ADD COLUMN
        post_ch_query(host, port, f"ALTER TABLE {pipeline_db}.{stage_table} ADD COLUMN IF NOT EXISTS extra_col String")

        # 5. INSERT BATCH 2
        insert2_sql = f"INSERT INTO {pipeline_db}.{stage_table} (id, p, val, extra_col) SELECT number+10000, '2026-07-28', 'v2', 'extra' FROM numbers(1000)"
        post_ch_query(host, port, insert2_sql)

        # 6. STOP MERGES
        post_ch_query(host, port, f"SYSTEM STOP MERGES {pipeline_db}.{stage_table}")

        # 7. MOVE PARTITION
        move_sql = f"ALTER TABLE {pipeline_db}.{stage_table} MOVE PARTITION '2026-07-28' TO TABLE main_db.target_table"
        post_ch_query(host, port, move_sql)

        # 8. START MERGES
        post_ch_query(host, port, f"SYSTEM START MERGES {pipeline_db}.{stage_table}")

        # 9. DROP STAGE TABLE SYNC
        post_ch_query(host, port, f"DROP TABLE IF EXISTS {pipeline_db}.{stage_table} SYNC")

def start_stress_on_node(host, port, concurrency, duration_sec):
    init_target(host, port)
    threads = []
    for tid in range(concurrency):
        t = threading.Thread(target=single_pipeline_worker, args=(tid, host, port, duration_sec))
        t.start()
        threads.append(t)
    return threads

def monitor_reproduction(concurrency=10, duration_sec=900):
    print(f"==========================================")
    print(f"ClickHouse 26.3 vs 25.8 Memory Leak Reproduction")
    print(f"Concurrency: {concurrency} Threads per instance | Duration: {duration_sec}s")
    print(f"Target 26.3: 127.0.0.1:8129 (ch26_issue_repro)")
    print(f"Target 25.8: 127.0.0.1:8130 (ch25_issue_repro)")
    print(f"==========================================")

    threads_26 = start_stress_on_node("127.0.0.1", 8129, concurrency, duration_sec)
    threads_25 = start_stress_on_node("127.0.0.1", 8130, concurrency, duration_sec)

    start_time = time.time()
    samples_26 = []
    samples_25 = []

    while time.time() - start_time < duration_sec:
        elapsed = time.time() - start_time

        m26 = get_ch_metrics("127.0.0.1", 8129)
        cg26 = get_docker_memory("ch26_issue_repro")
        s26 = {
            'elapsed': round(elapsed, 1),
            'cgroup_mb': round(cg26 / 1024 / 1024, 2),
            'jemalloc_mb': round(m26.get('jemalloc.allocated', 0) / 1024 / 1024, 2),
            'resident_mb': round(m26.get('MemoryResident', 0) / 1024 / 1024, 2)
        }
        samples_26.append(s26)

        m25 = get_ch_metrics("127.0.0.1", 8130)
        cg25 = get_docker_memory("ch25_issue_repro")
        s25 = {
            'elapsed': round(elapsed, 1),
            'cgroup_mb': round(cg25 / 1024 / 1024, 2),
            'jemalloc_mb': round(m25.get('jemalloc.allocated', 0) / 1024 / 1024, 2),
            'resident_mb': round(m25.get('MemoryResident', 0) / 1024 / 1024, 2)
        }
        samples_25.append(s25)

        print(f"[T={s26['elapsed']}s] CH 26.3: Cgroup={s26['cgroup_mb']}MB, RSS={s26['resident_mb']}MB, jemalloc={s26['jemalloc_mb']}MB | CH 25.8: Cgroup={s25['cgroup_mb']}MB, RSS={s25['resident_mb']}MB, jemalloc={s25['jemalloc_mb']}MB")
        time.sleep(10)

    for t in threads_26:
        t.join()
    for t in threads_25:
        t.join()

    return samples_26, samples_25

def summarize(samples):
    if not samples:
        return {'start_cgroup': 0, 't300_cgroup': 0, 'end_cgroup': 0, 'diff_cgroup': 0, 'slope_second_half_cgroup': 0, 'peak_cgroup': 0,
                'start_jemalloc': 0, 't300_jemalloc': 0, 'end_jemalloc': 0, 'diff_jemalloc': 0, 'slope_second_half_jemalloc': 0, 'peak_jemalloc': 0,
                'start_rss': 0, 't300_rss': 0, 'end_rss': 0, 'diff_rss': 0, 'slope_second_half_rss': 0, 'peak_rss': 0}
    start = samples[0]
    mid_idx = len(samples) // 2
    for idx, s in enumerate(samples):
        if s['elapsed'] >= 300:
            mid_idx = idx
            break
    mid = samples[mid_idx]
    end = samples[-1]

    peak_cgroup = max(s['cgroup_mb'] for s in samples)
    peak_jemalloc = max(s['jemalloc_mb'] for s in samples)
    peak_rss = max(s['resident_mb'] for s in samples)

    second_half_minutes = max((end['elapsed'] - mid['elapsed']) / 60.0, 0.1)

    return {
        'start_cgroup': start['cgroup_mb'],
        't300_cgroup': mid['cgroup_mb'],
        'end_cgroup': end['cgroup_mb'],
        'diff_cgroup': round(end['cgroup_mb'] - start['cgroup_mb'], 2),
        'slope_second_half_cgroup': round((end['cgroup_mb'] - mid['cgroup_mb']) / second_half_minutes, 2),
        'peak_cgroup': peak_cgroup,

        'start_jemalloc': start['jemalloc_mb'],
        't300_jemalloc': mid['jemalloc_mb'],
        'end_jemalloc': end['jemalloc_mb'],
        'diff_jemalloc': round(end['jemalloc_mb'] - start['jemalloc_mb'], 2),
        'slope_second_half_jemalloc': round((end['jemalloc_mb'] - mid['jemalloc_mb']) / second_half_minutes, 2),
        'peak_jemalloc': peak_jemalloc,

        'start_rss': start['resident_mb'],
        't300_rss': mid['resident_mb'],
        'end_rss': end['resident_mb'],
        'diff_rss': round(end['resident_mb'] - start['resident_mb'], 2),
        'slope_second_half_rss': round((end['resident_mb'] - mid['resident_mb']) / second_half_minutes, 2),
        'peak_rss': peak_rss
    }

if __name__ == '__main__':
    concurrency = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    duration = int(sys.argv[2]) if len(sys.argv) > 2 else 900

    s26, s25 = monitor_reproduction(concurrency=concurrency, duration_sec=duration)
    r26 = summarize(s26)
    r25 = summarize(s25)

    print("\n==========================================")
    print("REPRODUCTION SUMMARY (CH 26.3 vs CH 25.8)")
    print("==========================================")
    print(f"Metric                      | ClickHouse 26.3         | ClickHouse 25.8")
    print(f"----------------------------+-------------------------+-------------------------")
    print(f"cgroup T=0s -> T=300s -> End| {r26['start_cgroup']} -> {r26['t300_cgroup']} -> {r26['end_cgroup']} MB | {r25['start_cgroup']} -> {r25['t300_cgroup']} -> {r25['end_cgroup']} MB")
    print(f"cgroup 2nd-Half Slope       | {r26['slope_second_half_cgroup']:+} MB/min            | {r25['slope_second_half_cgroup']:+} MB/min")
    print(f"jemalloc T=0s -> 300s -> End| {r26['start_jemalloc']} -> {r26['t300_jemalloc']} -> {r26['end_jemalloc']} MB | {r25['start_jemalloc']} -> {r25['t300_jemalloc']} -> {r25['end_jemalloc']} MB")
    print(f"jemalloc 2nd-Half Slope     | {r26['slope_second_half_jemalloc']:+} MB/min            | {r25['slope_second_half_jemalloc']:+} MB/min")
    print(f"RSS T=0s -> T=300s -> End   | {r26['start_rss']} -> {r26['t300_rss']} -> {r26['end_rss']} MB | {r25['start_rss']} -> {r25['t300_rss']} -> {r25['end_rss']} MB")
    print(f"RSS 2nd-Half Slope          | {r26['slope_second_half_rss']:+} MB/min            | {r25['slope_second_half_rss']:+} MB/min")
