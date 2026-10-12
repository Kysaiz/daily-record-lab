"""Bounded original-class matrix observation; no scheduled execution.

One class per runner, five-request batches, shared 429 cooldown and four
attempts per page. No identity changes or replacement runners for failed work.
"""
import argparse
import asyncio
import json
import os
import platform
import subprocess
import time
from collections import Counter
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

CLASSES = [
    'Adele', 'Angelic Buster', 'Aran', 'Arch Mage (F/P)', 'Arch Mage (I/L)',
    'Ark', 'Battle Mage', 'Bishop', 'Blade Master', 'Blaster', 'Blaze Wizard',
    'Bow Master', 'Buccaneer', 'Cadena', 'Corsair', 'Dark Knight', 'Dawn Warrior',
    'Demon Avenger', 'Demon Slayer', 'Evan', 'Hayato', 'Hero', 'Hoyoung', 'Illium',
    'Kain', 'Kaiser', 'Kanna', 'Khali', 'Kinesis', 'Lara', 'Luminous', 'Marksman',
    'Mechanic', 'Mercedes', 'Mihile', 'Night Lord', 'Night Walker', 'Paladin',
    'Pathfinder', 'Phantom', 'Shade', 'Shadower', 'Thunder Breaker', 'Wild Hunter',
    'Wind Archer', 'Xenon', 'Zero',
]
API = 'https://www.nexon.com/api/maplestory/no-auth/ranking/v2/na'
MAX_PAGES, MAX_SECONDS, MAX_EXTRA, MAX_429 = 3000, 1500, 40, 12


def stamp():
    return datetime.now(timezone.utc).isoformat()


def key(row):
    return row['characterName'].casefold(), row['worldID']


def signature(payload):
    return [payload['totalCount'], [[r[k] for k in ('characterName', 'worldID', 'level', 'exp', 'rank')]
                                  for r in payload['ranks']]]


def validate(payload, position, job, total=None):
    count, rows = payload.get('totalCount'), payload.get('ranks')
    if not isinstance(count, int) or count < 0 or not isinstance(rows, list):
        raise RuntimeError('Invalid response schema')
    if payload.get('pageIndex') != position or (total is not None and total != count):
        raise RuntimeError('pageIndex or totalCount changed')
    if len(rows) != max(0, min(10, count-position+1)):
        raise RuntimeError('Unexpected empty/short response')
    if any(r.get('jobName') != job for r in rows):
        raise RuntimeError('Unexpected class')
    if len({key(r) for r in rows}) != len(rows):
        raise RuntimeError('Duplicate within page')
    if any(a['level'] < b['level'] for a, b in zip(rows, rows[1:])):
        raise RuntimeError('Level ordering changed')
    return count


def cooldown_seconds(value, attempt):
    fallback = 30 * attempt
    if value is None:
        return fallback
    try:
        seconds = float(value)
    except ValueError:
        seconds = (parsedate_to_datetime(value)-datetime.now(timezone.utc)).total_seconds()
    if seconds > 600:
        raise RuntimeError('Retry-After exceeds observation budget; stop')
    return max(fallback, seconds, 0)


class Gate:
    def __init__(self):
        self.until = 0

    def extend(self, seconds):
        self.until = max(self.until, time.monotonic()+seconds)

    async def wait(self):
        while self.until > time.monotonic():
            await asyncio.sleep(self.until-time.monotonic())


async def gather_batch(fetch, positions):
    tasks = [asyncio.create_task(fetch(p, 'scan')) for p in positions]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


def environment(out, stage):
    data = {'at_utc': stamp(), 'run_id': os.getenv('GITHUB_RUN_ID'),
            'job': os.getenv('PROBE_JOB'), 'runner_name': os.getenv('RUNNER_NAME'),
            'runner_os': os.getenv('RUNNER_OS'), 'runner_arch': os.getenv('RUNNER_ARCH'),
            'image_os': os.getenv('ImageOS'), 'image_version': os.getenv('ImageVersion'),
            'python': platform.python_version(),
            'caveat': 'External IPv4 observation, not Nexon-confirmed egress; different runners may share IPs.'}
    try:
        result = subprocess.run(['curl', '--ipv4', '--fail', '--silent', '--show-error',
                                 '--max-time', '10', 'https://api.ipify.org?format=json'],
                                capture_output=True, text=True, timeout=15)
        data['egress_ipv4'] = json.loads(result.stdout)['ip'] if result.returncode == 0 else None
        data['egress_error'] = result.stderr[:500] if result.returncode else None
    except Exception as exc:
        data['egress_ipv4'], data['egress_error'] = None, str(exc)
    (out/f'environment-{stage}.json').write_text(json.dumps(data, indent=2), encoding='utf-8')
    print(json.dumps(data), flush=True)
    return data


class Collector:
    def __init__(self, job, out):
        self.job, self.out = job, out
        out.mkdir(parents=True, exist_ok=False)
        self.started, self.clock = stamp(), time.monotonic()
        self.gate = Gate()
        self.requests, self.rows, self.seen = [], [], set()
        self.scan, self.anchors = {}, []
        self.total, self.boundary, self.session = None, None, None
        self.status, self.error = 'running', None
        self.refusals, self.extra, self.recovered = 0, 0, 0
        self.active, self.max_active = 0, 0

    def budget(self):
        if time.monotonic()-self.clock >= MAX_SECONDS or len(self.requests) >= MAX_PAGES+MAX_EXTRA+10:
            raise RuntimeError('Time/request budget exhausted')

    async def fetch(self, position, phase):
        had_429 = False
        for attempt in range(1, 5):
            await self.gate.wait()
            self.budget()
            if attempt > 1:
                self.extra += 1
                if self.extra > MAX_EXTRA:
                    raise RuntimeError('Retry budget exhausted')
            record = {'request_id': len(self.requests)+1, 'position': position, 'phase': phase,
                      'attempt': attempt, 'started_at_utc': stamp(), 'status': None}
            self.requests.append(record)
            self.active += 1
            self.max_active = max(self.max_active, self.active)
            started = time.monotonic()
            try:
                async with self.session.get(API, params={'type':'job', 'id':self.job,
                    'reboot_index':0, 'page_index':position}, allow_redirects=False,
                    trace_request_ctx=record) as response:
                    record.update(status=response.status, retry_after=response.headers.get('Retry-After'),
                                  server=response.headers.get('Server'))
                    connection = response.connection
                    transport = getattr(connection, 'transport', None)
                    if transport is not None:
                        record.update(remote_address=transport.get_extra_info('peername'),
                                      local_address=transport.get_extra_info('sockname'))
                    body = await response.read()
                    record['body_bytes'] = len(body)
                    record['response_ms'] = (time.monotonic()-record.get('send_monotonic_seconds', started))*1000
                    (self.out/f'{record["request_id"]:05d}-{phase}-{position}.json').write_bytes(body)
                    if response.status == 429:
                        had_429 = True
                        self.refusals += 1
                        if attempt == 4 or self.refusals >= MAX_429:
                            raise RuntimeError('429 retry/refusal limit reached')
                        delay = cooldown_seconds(record['retry_after'], attempt)
                        self.gate.extend(delay)
                        record['shared_cooldown_seconds'] = delay
                        print(f'{self.job}: 429 at {position}, shared pause {delay}s, attempt {attempt}/4', flush=True)
                    elif response.status != 200:
                        raise RuntimeError(f'HTTP {response.status}; stop class')
                    else:
                        payload = json.loads(body)
                        self.total = validate(payload, position, self.job, self.total)
                        record.update(total_count=self.total, row_count=len(payload['ranks']))
                        if had_429:
                            self.recovered += 1
                        return payload
            except BaseException as exc:
                record['error'] = f'{type(exc).__name__}: {exc}'
                raise
            finally:
                self.active -= 1
                record['finished_at_utc'] = stamp()
                with (self.out/'completion-order.jsonl').open('a', encoding='utf-8') as file:
                    file.write(json.dumps(record)+'\n')
        raise RuntimeError('No successful response')

    def add(self, position, payload):
        rows = payload['ranks']
        eligible = [r for r in rows if r['level'] >= 275]
        if self.rows and eligible and self.rows[-1]['level'] < eligible[0]['level']:
            raise RuntimeError('Cross-page level ordering changed')
        if any(key(r) in self.seen for r in eligible):
            raise RuntimeError('Cross-page duplicate')
        self.rows.extend(eligible)
        self.seen.update(key(r) for r in eligible)
        self.scan[position] = signature(payload)
        return len(eligible) < len(rows) or position+len(rows) > self.total

    def report(self):
        sent = [r for r in self.requests if r.get('sent_at_utc')]
        result = {'scope': {'job':self.job, 'reboot_index':0, 'min_level':275},
                  'started_at_utc':self.started, 'updated_at_utc':stamp(),
                  'elapsed_seconds':time.monotonic()-self.clock, 'status':self.status, 'error':self.error,
                  'total_count':self.total, 'last_qualifying_position':self.boundary,
                  'successful_scan_pages':len(self.scan), 'rows':len(self.rows),
                  'unique_characters':len(self.seen), 'duplicate_rows':len(self.rows)-len(self.seen),
                  'attempts':len(self.requests), 'sent_requests':len(sent),
                  'statuses':dict(Counter(str(r['status']) for r in self.requests)),
                  '429_responses':self.refusals, 'pages_recovered_after_429':self.recovered,
                  'max_concurrent_requests':self.max_active, 'anchors_checked':self.anchors,
                  'strategy': 'five requests per batch, 0.5s batch pause; shared 429 gate; max four attempts',
                  'limits': {'pages':MAX_PAGES, 'seconds':MAX_SECONDS, 'extra_attempts':MAX_EXTRA, '429':MAX_429},
                  'snapshot_caveat':'Finite anchors, no atomic server snapshot. Original 47-class cohort, not proven all-GMS coverage.'}
        (self.out/'requests.jsonl').write_text(''.join(json.dumps(r)+'\n' for r in self.requests), encoding='utf-8')
        (self.out/'characters.json').write_text(json.dumps(self.rows), encoding='utf-8')
        (self.out/'report.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        return result

    async def collect(self):
        first = await self.fetch(1, 'scan')
        if self.total == 0:
            raise RuntimeError('Zero totalCount: unsupported class and empty population cannot be distinguished')
        terminal = False
        last_level = None
        for index in range(0, MAX_PAGES, 5):
            positions = [1+10*i for i in range(index, min(index+5, MAX_PAGES)) if 1+10*i <= self.total]
            if not positions:
                raise RuntimeError('Missing end-of-population evidence')
            remaining = [p for p in positions if p != 1]
            payloads = await gather_batch(self.fetch, remaining)
            by_position = dict(zip(remaining, payloads))
            if index == 0:
                by_position[1] = first
            for position in positions:
                payload = by_position[position]
                rows = payload['ranks']
                if last_level is not None and rows and last_level < rows[0]['level']:
                    raise RuntimeError('Cross-page level ordering changed')
                if rows:
                    last_level = rows[-1]['level']
                if not terminal:
                    terminal = self.add(position, payload)
                elif any(r['level'] >= 275 for r in rows):
                    raise RuntimeError('Qualifying rows after terminal page')
            self.report()
            if terminal:
                break
            if len(self.scan) % 100 == 0:
                print(f'{self.job}: {len(self.scan)} pages, {len(self.rows)} unique rows', flush=True)
            await asyncio.sleep(.5)
        else:
            raise RuntimeError('Page limit reached before level boundary')
        self.boundary = len(self.rows)
        # Include the terminal page so a zero-target cohort has evidence too.
        ordered = sorted(self.scan)
        anchors = sorted({ordered[0], ordered[(len(ordered)-1)//2], ordered[-1],
                          1+10*((self.boundary-1)//10) if self.boundary else 1})
        for position in anchors:
            if signature(await self.fetch(position, 'anchor')) != self.scan[position]:
                raise RuntimeError('Anchor changed')
            self.anchors.append(position)
        self.status = 'complete_class_observation'

    async def run(self):
        import aiohttp
        trace = aiohttp.TraceConfig()
        async def sent(session, ctx, params):
            if ctx.trace_request_ctx.get('sent_at_utc'):
                raise RuntimeError('Unexpected internal transport retry')
            ctx.trace_request_ctx.update(sent_at_utc=stamp(), send_monotonic_seconds=time.monotonic())
        async def reused(session, ctx, params):
            ctx.trace_request_ctx['connection_reused'] = True
        trace.on_request_headers_sent.append(sent)
        trace.on_connection_reuseconn.append(reused)
        environment(self.out, 'start')
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5),
                timeout=aiohttp.ClientTimeout(total=30), trace_configs=[trace],
                cookie_jar=aiohttp.DummyCookieJar(), headers={'User-Agent':'GMSRankingResearch/6.0',
                'Accept':'application/json','Accept-Encoding':'identity'}) as session:
                if aiohttp.__version__ != '3.14.3' or not hasattr(session, '_retry_connection'):
                    raise RuntimeError('Expected pinned aiohttp retry control')
                session._retry_connection = False
                self.session = session
                await asyncio.wait_for(self.collect(), timeout=MAX_SECONDS)
        except BaseException as exc:
            self.status, self.error = 'incomplete', f'{type(exc).__name__}: {exc}'
        finally:
            environment(self.out, 'end')
            print(json.dumps(self.report()), flush=True)
        return 0 if self.status == 'complete_class_observation' else 2


def combine(root):
    reports = {}
    duplicates, all_keys, issues = [], set(), []
    for path in root.rglob('report.json'):
        data = json.loads(path.read_text(encoding='utf-8'))
        job = data['scope']['job']
        if job in reports or job not in CLASSES:
            issues.append(f'Duplicate/unexpected class: {job}')
        reports[job] = data
        characters = json.loads((path.parent/'characters.json').read_text(encoding='utf-8'))
        identities = [key(r) for r in characters]
        if len(identities) != data['rows'] or len(set(identities)) != data['unique_characters']:
            issues.append(f'Character/report mismatch: {job}')
        for identity in identities:
            if identity in all_keys:
                duplicates.append(identity)
            all_keys.add(identity)
        for stage in ('start','end'):
            env = path.parent/f'environment-{stage}.json'
            data[f'environment_{stage}'] = json.loads(env.read_text(encoding='utf-8')) if env.exists() else None
    missing = sorted(set(CLASSES)-set(reports))
    incomplete = [job for job, r in reports.items() if r['status'] != 'complete_class_observation']
    starts = [datetime.fromisoformat(r['started_at_utc']) for r in reports.values()]
    ends = [datetime.fromisoformat(r['updated_at_utc']) for r in reports.values()]
    summary = {'cohort':'Original repository 47 classes; not proven all-GMS coverage',
        'expected_classes':len(CLASSES), 'reported_classes':len(reports), 'missing_classes':missing,
        'incomplete_classes':incomplete, 'issues':issues, 'cross_class_duplicates':duplicates,
        'unique_characters':len(all_keys), 'collection_complete':not (missing or incomplete or issues or duplicates),
        'collection_span_seconds':(max(ends)-min(starts)).total_seconds() if starts else None,
        'sent_requests':sum(r['sent_requests'] for r in reports.values()),
        '429_responses':sum(r['429_responses'] for r in reports.values()), 'classes':reports,
        'audit_caveat':'Aggregation checks only; raw HTTP evidence must be independently audited.'}
    Path('matrix-summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({k:v for k,v in summary.items() if k != 'classes'}), flush=True)
    return 0 if summary['collection_complete'] else 2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--combine', type=Path)
    parser.add_argument('--job', choices=CLASSES)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if args.list:
        print(json.dumps(CLASSES)); return 0
    if args.combine is not None:
        return combine(args.combine)
    if not args.execute:
        print(json.dumps({'dry_run':True,'classes':len(CLASSES),'parallel_runners':8,
            'min_level':275,'max_pages_per_class':MAX_PAGES,'max_seconds_per_class':MAX_SECONDS,
            'attempts_per_page':4,'shared_429_cooldown':[30,60,90]})); return 0
    if args.job is None:
        parser.error('--job is required for execution')
    return asyncio.run(Collector(args.job, Path('matrix-data')/str(CLASSES.index(args.job))).run())


if __name__ == '__main__':
    raise SystemExit(main())
