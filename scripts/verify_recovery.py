#!/usr/bin/env python3
"""Fault test for the local Compose stack: briefly stops Kafka and the app.

Keeps all volumes and data; leaves one verification employee. Run from repo root.
"""
import json
import subprocess
import time
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

BASE = 'http://localhost:8080'

def compose(*args):
    subprocess.run(['docker', 'compose', *args], check=True, capture_output=True, text=True)

def request(path, body=None):
    req = urllib.request.Request(BASE + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.load(response)

def wait_for(action, timeout=180):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = action()
            if result:
                return result
        except Exception as error:
            last_error = error
        time.sleep(2)
    raise RuntimeError(f'Timed out waiting for recovery: {last_error}')

def outbox(employee_id):
    # employee_id is an API-generated UUID; JSON encoding also keeps this query safe.
    js = 'JSON.stringify(db.outbox.findOne({employeeId:' + json.dumps(employee_id) + '},{published:1}))'
    result = subprocess.run(['docker', 'compose', 'exec', '-T', 'mongo', 'mongosh', '--quiet',
        'mongodb://localhost:27017/employees?replicaSet=rs0', '--eval', js], check=True, capture_output=True, text=True)
    return json.loads(result.stdout)

wait_for(lambda: request('/api/pipeline/relay')['state'] == 'STREAMING')
wait_for(lambda: request('/api/pipeline/relay')['lastCheckpointAt'])
baseline = request('/api/stats')
assert baseline['mongo'] == baseline['opensearch'] and baseline['pendingOutbox'] == 0, baseline
probe = 'Recovery' + uuid.uuid4().hex
try:
    compose('stop', 'kafka')
    print('Kafka stopped; creating employee through the live API', flush=True)
    employee = request('/api/employees', {'firstName': probe, 'lastName': 'RecoveryTest',
        'department': 'Engineering', 'skills': [], 'assignments': []})
    assert outbox(employee['id'])['published'] is False
    assert request('/api/stats')['mongo'] == baseline['mongo'] + 1
    print('PASS: MongoDB commit succeeds during Kafka outage; event stays unpublished', flush=True)
    compose('stop', 'app')
    compose('start', 'kafka')
    compose('start', 'app')
    query = '/api/search?' + urllib.parse.urlencode({'q': probe})
    wait_for(lambda: request(query)['hits']['total']['value'] == 1)
    wait_for(lambda: outbox(employee['id'])['published'])
    def converged():
        current = request('/api/stats')
        return current if current['mongo'] == current['opensearch'] and current['pendingOutbox'] == 0 else None
    stats = wait_for(converged)
    assert stats['mongo'] == baseline['mongo'] + 1, stats
    report = {'verifiedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'kafkaOutageMongoCommit': 'passed', 'pendingEventSurvivesAppRestart': 'passed',
        'indexedExactlyOneDocument': 'passed', 'employeeId': employee['id'],
        'stats': stats, 'relay': request('/api/pipeline/relay')}
    Path('docs/change-stream-verification.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2), flush=True)
finally:
    compose('start', 'kafka', 'app')
