#!/usr/bin/env python3
"""Exercise the live pipeline. --million also resumes and verifies the 1M load."""
import argparse
import json
import time
import uuid
import urllib.parse
import urllib.request
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--million', action='store_true')
parser.add_argument('--base', default='http://localhost:8080')
args = parser.parse_args()

def request(path, body=None, method=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(args.base + path, data=data, method=method,
                                 headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=60) as response:
        return json.load(response)

def search(**params):
    return request('/api/search?' + urllib.parse.urlencode(params))['hits']

probe = 'Probe' + uuid.uuid4().hex
employee = request('/api/employees', {
    'firstName': probe, 'lastName': 'Verification', 'department': 'Engineering',
    'title': 'Pipeline tester', 'status': 'Active',
    'address': {'city': 'Bengaluru', 'country': 'India'},
    'skills': [{'name': 'Java', 'level': 1}, {'name': 'Python', 'level': 5}],
    'assignments': [{'project': 'Alpha', 'role': 'Reviewer', 'region': 'India', 'active': True},
                    {'project': 'Beta', 'role': 'Lead', 'region': 'India', 'active': True}],
    'manager': {'id': 'test-manager', 'name': 'Test Manager'}
})
deadline = time.monotonic() + 120
while time.monotonic() < deadline:
    if search(q=probe)['total']['value'] == 1:
        break
    time.sleep(2)
else:
    raise RuntimeError('Created employee did not reach search within 120 seconds')
assert search(q=probe, skill='Java', minSkillLevel=5)['total']['value'] == 0
assert search(q=probe, skill='Java', minSkillLevel=1)['total']['value'] == 1
assert search(q=probe, project='Alpha', role='Lead')['total']['value'] == 0
assert search(q=probe, project='Alpha', role='Reviewer')['total']['value'] == 1
print('PASS: MongoDB → outbox → Kafka → OpenSearch; nested predicates do not cross array elements', flush=True)

if args.million:
    request('/api/seed?count=1000000', method='POST')
    deadline = time.monotonic() + 3600
    while time.monotonic() < deadline:
        seed = request('/api/seed')
        stats = request('/api/stats')
        print(json.dumps({'seed': seed, 'stats': stats}), flush=True)
        if seed.get('error'):
            raise RuntimeError(seed['error'])
        if seed['inserted'] >= 1000000 and stats['pendingOutbox'] == 0 and stats['mongo'] == stats['opensearch']:
            break
        time.sleep(15)
    else:
        raise RuntimeError('Pipeline did not converge before timeout')
    print('PASS: at least 1,000,000 employees committed and searchable', flush=True)
report = {'verifiedAt': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
          'probeId': employee['id'], 'nestedMatching': 'passed', 'stats': request('/api/stats'),
          'seed': request('/api/seed')}
Path('verification.json').write_text(json.dumps(report, indent=2) + '\n')
print(json.dumps(report, indent=2), flush=True)
