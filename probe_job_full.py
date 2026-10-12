"""One bounded, serial class observation; dry-run unless --execute is given."""
import argparse
import http.client
import json
import math
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path


def stamp():
    return datetime.now(timezone.utc).isoformat()


def identity(row):
    return row['characterName'].casefold(), row['worldID']


def signature(payload):
    return payload['totalCount'], [[r[k] for k in ('characterName', 'worldID', 'level', 'exp', 'rank')] for r in payload['ranks']]


def locate_boundary(fetch, threshold, max_rows):
    """Find the last qualifying list position, assuming descending level order."""
    cache = {}

    def get(position):
        if position not in cache:
            cache[position] = fetch(position, 'boundary')
        return cache[position]

    first = get(1)
    if first['ranks'][0]['level'] < threshold:
        return 0, cache
    low = 1
    high = min(10001, max_rows + 1)
    if get(high)['ranks'][0]['level'] >= threshold:
        low, high = high, max_rows + 1
        if get(high)['ranks'][0]['level'] >= threshold:
            raise RuntimeError('Target exceeds configured page budget; no full scan')
    while high - low > 1:
        mid = (low + high) // 2
        if get(mid)['ranks'][0]['level'] >= threshold:
            low = mid
        else:
            high = mid
    before, after = get(low), get(high)
    if before['ranks'][0]['level'] < threshold or after['ranks'][0]['level'] >= threshold:
        raise RuntimeError('Boundary inconsistent')
    if [identity(r) for r in before['ranks'][1:]] != [identity(r) for r in after['ranks'][:-1]]:
        raise RuntimeError('Adjacent boundary responses shifted inconsistently')
    return low, cache


class Probe:
    def __init__(self, args):
        self.args = args
        self.root = Path(__file__).resolve().parent / 'job-full-integrity'
        self.root.mkdir(exist_ok=True)
        if (self.root / 'refusal.json').exists():
            raise RuntimeError('Prior refusal recorded; no requests sent')
        self.out = self.root / datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')
        self.out.mkdir()
        self.started = stamp()
        self.clock = time.monotonic()
        self.conn = http.client.HTTPSConnection('www.nexon.com', timeout=25)
        self.last_send = None
        self.requests = []
        self.total = None
        self.boundary = None
        self.rows = []
        self.seen = set()
        self.pages = 0
        self.anchors = {}
        self.anchors_checked = []
        self.status = 'running'
        self.error = None

    def fetch(self, position, phase):
        limit = 32 if self.args.mode == 'boundary' else self.args.max_pages + 40
        if len(self.requests) >= limit or time.monotonic() - self.clock >= self.args.max_seconds:
            raise RuntimeError('Request/time budget reached')
        record = {'position': position, 'phase': phase, 'started_at_utc': stamp(), 'status': None}
        started = time.monotonic()
        try:
            reused = self.conn.sock is not None
            connected = time.monotonic()
            if not reused:
                self.conn.connect()
            record['connect_ms'] = round((time.monotonic() - connected) * 1000, 3)
            record['remote_address'] = list(self.conn.sock.getpeername())
            record['local_address'] = list(self.conn.sock.getsockname())
            if self.last_send is not None:
                time.sleep(max(0, self.last_send + 1 / self.args.rps - time.monotonic()))
            if time.monotonic() - self.clock >= self.args.max_seconds:
                raise RuntimeError('Deadline before GET')
            now = time.monotonic()
            record.update(sent_at_utc=stamp(), send_monotonic_seconds=now,
                          send_gap_ms=round((now - self.last_send) * 1000, 3) if self.last_send is not None else None,
                          connection_reused=reused)
            self.last_send = now
            params = {'type': 'job', 'id': self.args.job, 'reboot_index': 0, 'page_index': position}
            self.conn.request('GET', '/api/maplestory/no-auth/ranking/v2/na?' + urllib.parse.urlencode(params),
                              headers={'User-Agent': 'GMSRankingResearch/6.0', 'Accept': 'application/json', 'Accept-Encoding': 'identity'})
            response = self.conn.getresponse()
            record.update(status=response.status, retry_after=response.getheader('Retry-After'), server=response.getheader('Server'))
            body = response.read()
            record.update(body_bytes=len(body), response_ms=round((time.monotonic() - now) * 1000, 3))
            (self.out / f'{len(self.requests)+1:04d}-{phase}-{position}.json').write_bytes(body)
            if response.will_close:
                self.conn.close()
            if response.status in (401, 403, 429):
                (self.root / 'refusal.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
            if response.status != 200:
                raise RuntimeError(f'HTTP {response.status}; no retry')
            payload = json.loads(body)
            rows = payload.get('ranks', [])
            record.update(total_count=payload.get('totalCount'), row_count=len(rows))
            if len(rows) != 10 or payload.get('pageIndex') != position:
                raise RuntimeError('Empty/short page or unexpected pageIndex; not a valid boundary')
            if self.total is None:
                self.total = payload['totalCount']
            if payload['totalCount'] != self.total:
                raise RuntimeError('totalCount changed; snapshot not consistent')
            if any(r.get('jobName') != self.args.job for r in rows):
                raise RuntimeError('Class filter not verified')
            if any(a['level'] < b['level'] for a, b in zip(rows, rows[1:])):
                raise RuntimeError('Page not in descending level order')
            if len({identity(r) for r in rows}) != 10:
                raise RuntimeError('Duplicate character within page')
            return payload
        except Exception as exc:
            record['error'] = f'{type(exc).__name__}: {exc}'
            raise
        finally:
            record['total_ms'] = round((time.monotonic() - started) * 1000, 3)
            self.requests.append(record)
            with (self.out / 'requests.jsonl').open('a', encoding='utf-8') as f:
                f.write(json.dumps(record) + '\n')

    def add_page(self, payload, position):
        expected = min(10, self.boundary - position + 1)
        kept = [r for r in payload['ranks'] if r['level'] >= self.args.min_level]
        if len(kept) != expected:
            raise RuntimeError('Level boundary changed during scan')
        if self.rows and kept and self.rows[-1]['level'] < kept[0]['level']:
            raise RuntimeError('Cross-page level ordering changed')
        if any(identity(r) in self.seen for r in kept):
            raise RuntimeError('Cross-page duplicate; no complete snapshot')
        self.rows.extend(kept)
        self.seen.update(identity(r) for r in kept)
        self.pages += 1

    def report(self):
        elapsed = time.monotonic() - self.clock
        result = {'started_at_utc': self.started, 'updated_at_utc': stamp(), 'status': self.status, 'error': self.error,
                  'scope': {'job': self.args.job, 'reboot_index': 0, 'min_level': self.args.min_level},
                  'configured_rps': self.args.rps, 'total_count': self.total, 'last_qualifying_position': self.boundary,
                  'expected_scan_pages': math.ceil(self.boundary / 10) if self.boundary is not None else None,
                  'successful_scan_pages': self.pages, 'rows': len(self.rows), 'unique_characters': len(self.seen),
                  'duplicate_rows': len(self.rows) - len(self.seen), 'requests': len(self.requests),
                  'statuses': dict(Counter(str(r['status']) for r in self.requests)),
                  'minimum_send_gap_ms': min((r['send_gap_ms'] for r in self.requests if r.get('send_gap_ms') is not None), default=None),
                  'elapsed_seconds': round(elapsed, 3), 'anchors_checked': self.anchors_checked,
                  'new_connections': sum(not r.get('connection_reused', False) for r in self.requests),
                  'snapshot_caveat': 'Sequential observation with finite anchor checks; no server-side snapshot token, cannot exclude all unobserved changes.'}
        (self.out / 'report.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        return result

    def run(self):
        try:
            self.boundary, cache = locate_boundary(self.fetch, self.args.min_level, self.args.max_pages * 10)
            (self.out / 'boundary.json').write_text(json.dumps({'last_qualifying_position': self.boundary,
                'pages': math.ceil(self.boundary / 10), 'observed_positions': sorted(cache)}, indent=2), encoding='utf-8')
            print(f'Boundary: {self.args.job} LV{self.args.min_level}+, {self.boundary} rows, {math.ceil(self.boundary/10)} pages', flush=True)
            if self.args.mode == 'boundary':
                self.status = 'boundary_verified'
            else:
                if not self.boundary:
                    raise RuntimeError('No target characters; full stability observation not applicable')
                count = math.ceil(self.boundary / 10)
                anchor_positions = {1, 1 + ((count - 1) // 2) * 10, 1 + (count - 1) * 10}
                for i in range(count):
                    position = 1 + 10 * i
                    payload = self.fetch(position, 'scan')
                    self.add_page(payload, position)
                    if position in anchor_positions:
                        self.anchors[position] = signature(payload)
                    if self.pages % 20 == 0 or self.pages == count:
                        print(f'Progress: {self.pages}/{count} pages, {len(self.rows)} unique rows', flush=True)
                        self.report()
                for position in sorted(self.anchors):
                    if signature(self.fetch(position, 'anchor')) != self.anchors[position]:
                        raise RuntimeError('Scan anchor changed')
                    self.anchors_checked.append(position)
                for position in (self.boundary, self.boundary + 1):
                    if signature(self.fetch(position, 'boundary_anchor')) != signature(cache[position]):
                        raise RuntimeError('Boundary anchor changed')
                    self.anchors_checked.append(position)
                if len(self.rows) != self.boundary or len(self.seen) != self.boundary:
                    raise RuntimeError('Incomplete qualifying rows')
                self.status = 'complete_class_observation'
        except Exception as exc:
            self.error = f'{type(exc).__name__}: {exc}'
            self.status = 'incomplete'
        finally:
            self.conn.close()
            print(json.dumps({**self.report(), 'saved': str(self.out)}), flush=True)
        return 0 if self.status in ('boundary_verified', 'complete_class_observation') else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute', action='store_true')
    p.add_argument('--mode', choices=('boundary', 'full'), default='boundary')
    p.add_argument('--job', default='Hero')
    p.add_argument('--min-level', type=int, default=275)
    p.add_argument('--rps', type=float, default=.4)
    p.add_argument('--max-pages', type=int, default=2000)
    p.add_argument('--max-seconds', type=int, default=5700)
    args = p.parse_args()
    if not 0 < args.rps <= .4 or not 1 <= args.max_pages <= 2000 or not 1 <= args.max_seconds <= 5700:
        p.error('Bounded probe: rps <= 0.4, max-pages <= 2000, max-seconds <= 5700')
    if not args.execute:
        print(json.dumps({'dry_run': True, **vars(args), 'request_budget': 32 if args.mode == 'boundary' else args.max_pages + 40}))
        return 0
    return Probe(args).run()


if __name__ == '__main__':
    raise SystemExit(main())
