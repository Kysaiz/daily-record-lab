"""Corrected original five-request batches, bounded to one full class.

Reference: ebenitez1/maplestory-level-stats scraper/scrape.py.
Uses the tested serial probe's identity, boundary semantics and validation.
No retry, identity rotation, scheduling or automatic additional classes.
"""
import argparse
import asyncio
import json
import math
import time
from collections import Counter
from pathlib import Path
from types import SimpleNamespace

from probe_job_full import Probe, identity, signature, stamp


async def boundary(fetch, threshold, max_rows):
    cache = {}

    async def get(position):
        if position not in cache:
            cache[position] = await fetch(position, 'boundary')
        return cache[position]

    first = await get(1)
    if first['ranks'][0]['level'] < threshold:
        raise RuntimeError('No target rows; not a full stability observation')
    low, high = 1, max_rows + 1
    if (await get(high))['ranks'][0]['level'] >= threshold:
        raise RuntimeError('Target exceeds configured page budget')
    while high - low > 1:
        mid = (low + high) // 2
        if (await get(mid))['ranks'][0]['level'] >= threshold:
            low = mid
        else:
            high = mid
    before, after = await get(low), await get(high)
    if [identity(r) for r in before['ranks'][1:]] != [identity(r) for r in after['ranks'][:-1]]:
        raise RuntimeError('Boundary shifted inconsistently')
    return low, cache


async def batch(fetch, positions):
    tasks = [asyncio.create_task(fetch(p, 'scan')) for p in positions]
    try:
        return await asyncio.gather(*tasks)
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


class ParallelProbe(Probe):
    def __init__(self, args):
        super().__init__(args)
        self.stop = False
        self.active = 0
        self.max_active = 0
        self.first_failure = None
        self.session = None

    async def fetch_async(self, position, phase):
        if self.stop:
            raise RuntimeError('Observation already stopped; no new request')
        if len(self.requests) >= self.args.max_pages + 40 or time.monotonic() - self.clock >= self.args.max_seconds:
            raise RuntimeError('Request/time budget reached')
        record = {'request_id': len(self.requests) + 1, 'position': position, 'phase': phase,
                  'started_at_utc': stamp(), 'status': None, 'connection_reused': False}
        self.requests.append(record)
        started = time.monotonic()
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            params = {'type': 'job', 'id': self.args.job, 'reboot_index': 0, 'page_index': position}
            async with self.session.get('https://www.nexon.com/api/maplestory/no-auth/ranking/v2/na',
                                        params=params, trace_request_ctx=record, allow_redirects=False) as response:
                record.update(status=response.status, retry_after=response.headers.get('Retry-After'),
                              server=response.headers.get('Server'))
                connection = response.connection
                transport = getattr(connection, 'transport', None)
                if transport is not None:
                    record['remote_address'] = transport.get_extra_info('peername')
                    record['local_address'] = transport.get_extra_info('sockname')
                body = await response.read()
                record['body_bytes'] = len(body)
                record['response_ms'] = round((time.monotonic() - record.get('send_monotonic_seconds', started)) * 1000, 3)
                (self.out / f'{record["request_id"]:04d}-{phase}-{position}.json').write_bytes(body)
                if response.status != 200:
                    if response.status in (401, 403, 429):
                        (self.root / 'refusal.json').write_text(json.dumps(record, indent=2), encoding='utf-8')
                    raise RuntimeError(f'HTTP {response.status}; no retry')
                payload = json.loads(body)
                rows = payload.get('ranks', [])
                record.update(total_count=payload.get('totalCount'), row_count=len(rows))
                if len(rows) != 10 or payload.get('pageIndex') != position:
                    raise RuntimeError('Empty/short page or incorrect pageIndex')
                if self.total is None:
                    self.total = payload['totalCount']
                if payload['totalCount'] != self.total:
                    raise RuntimeError('totalCount changed')
                if any(r.get('jobName') != self.args.job for r in rows):
                    raise RuntimeError('Unexpected class')
                if len({identity(r) for r in rows}) != 10:
                    raise RuntimeError('Duplicate within page')
                if any(a['level'] < b['level'] for a, b in zip(rows, rows[1:])):
                    raise RuntimeError('Incorrect level ordering')
                return payload
        except BaseException as exc:
            record['error'] = f'{type(exc).__name__}: {exc}'
            self.stop = True
            if self.first_failure is None:
                self.first_failure = {'at_utc': stamp(), 'request_id': record['request_id'],
                                      'status': record['status'], 'error': record['error']}
            raise
        finally:
            self.active -= 1
            record['total_ms'] = round((time.monotonic() - started) * 1000, 3)
            record['finished_at_utc'] = stamp()
            with (self.out / 'completion-order.jsonl').open('a', encoding='utf-8') as f:
                f.write(json.dumps(record) + '\n')

    def report(self):
        ordered = sorted(self.requests, key=lambda r: r['request_id'])
        (self.out / 'requests.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in ordered), encoding='utf-8')
        result = super().report()
        sent = sorted(r['send_monotonic_seconds'] for r in ordered if r.get('send_monotonic_seconds') is not None)
        result.update(configured_rps=None, transport='aiohttp', strategy='five-request batches then 0.5-second pause',
                      page_concurrency=5, batch_sleep_seconds=.5, max_concurrent_fetches=self.max_active,
                      minimum_send_gap_ms=min(((b-a)*1000 for a,b in zip(sent,sent[1:])), default=None),
                      first_failure=self.first_failure, successful_http_responses=sum(r['status']==200 for r in ordered),
                      in_flight_on_failure_caveat='Already sent requests can finish or be cancelled; no next batch or retry.')
        (self.out / 'report.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        return result

    async def run_async(self):
        import aiohttp
        trace = aiohttp.TraceConfig()

        async def sent(session, ctx, params):
            if ctx.trace_request_ctx.get('sent_at_utc') is not None:
                raise RuntimeError('Unexpected transport retry; stop')
            ctx.trace_request_ctx.update(sent_at_utc=stamp(), send_monotonic_seconds=time.monotonic())

        async def reused(session, ctx, params):
            ctx.trace_request_ctx['connection_reused'] = True

        trace.on_request_headers_sent.append(sent)
        trace.on_connection_reuseconn.append(reused)
        try:
            async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5),
                timeout=aiohttp.ClientTimeout(total=30), trace_configs=[trace], cookie_jar=aiohttp.DummyCookieJar(),
                headers={'User-Agent':'GMSRankingResearch/6.0','Accept':'application/json','Accept-Encoding':'identity'}) as session:
                self.session = session
                # aiohttp 3.14.3 retries stale persistent GETs internally unless
                # this version-specific switch is disabled. Fail closed on drift.
                if aiohttp.__version__ != '3.14.3' or not hasattr(session,'_retry_connection'):
                    raise RuntimeError('Expected pinned aiohttp 3.14.3 retry control')
                session._retry_connection = False
                self.boundary, cache = await boundary(self.fetch_async, self.args.min_level, self.args.max_pages*10)
                count = math.ceil(self.boundary/10)
                (self.out/'boundary.json').write_text(json.dumps({'last_qualifying_position':self.boundary,
                    'pages':count,'observed_positions':sorted(cache)},indent=2),encoding='utf-8')
                print(f'Boundary: {self.args.job}, {self.boundary} qualifying rows, {count} pages',flush=True)
                anchors = {1,1+((count-1)//2)*10,1+(count-1)*10}
                for first in range(0,count,5):
                    positions = [1+10*i for i in range(first,min(first+5,count))]
                    payloads = await batch(self.fetch_async,positions)
                    for position,payload in zip(positions,payloads):
                        self.add_page(payload,position)
                        if position in anchors:
                            self.anchors[position]=signature(payload)
                    self.report()
                    if self.pages % 25 == 0 or self.pages == count:
                        print(f'Progress: {self.pages}/{count}, {len(self.seen)} unique characters',flush=True)
                    if self.pages < count:
                        await asyncio.sleep(.5)
                for position in sorted(self.anchors):
                    if signature(await self.fetch_async(position,'anchor')) != self.anchors[position]:
                        raise RuntimeError('Scan anchor changed')
                    self.anchors_checked.append(position)
                for position in (self.boundary,self.boundary+1):
                    if signature(await self.fetch_async(position,'boundary_anchor')) != signature(cache[position]):
                        raise RuntimeError('Boundary anchor changed')
                    self.anchors_checked.append(position)
                if len(self.rows) != self.boundary or len(self.seen) != self.boundary:
                    raise RuntimeError('Incomplete population')
                self.status='complete_class_observation'
        except BaseException as exc:
            self.error=f'{type(exc).__name__}: {exc}'
            self.status='incomplete'
        finally:
            self.conn.close()
            print(json.dumps({**self.report(),'saved':str(self.out)}),flush=True)
        return 0 if self.status=='complete_class_observation' else 2


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--job',default='Hero')
    args=parser.parse_args()
    if not args.execute:
        print(json.dumps({'dry_run':True,'job':args.job,'min_level':275,'page_concurrency':5,
                          'batch_sleep_seconds':.5,'max_pages':750,'request_budget':790,'time_budget_seconds':900}))
        return 0
    options=SimpleNamespace(job=args.job,min_level=275,max_pages=750,max_seconds=900,mode='full',rps=.4)
    return asyncio.run(ParallelProbe(options).run_async())


if __name__=='__main__':
    raise SystemExit(main())
