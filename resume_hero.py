"""One authorised Hero checkpoint continuation; never launches another runner."""
import argparse
import asyncio
import base64
import gzip
import hashlib
import json
import math
import time
from datetime import datetime, timezone
from pathlib import Path
import matrix_probe as base

FIELDS = ('characterName','worldID','jobName','level','exp','rank','gap')
CORE = ('characterName','worldID','level','exp','rank')
base.MAX_PAGES, base.MAX_SECONDS, base.MAX_EXTRA, base.MAX_429 = 500, 900, 0, 1


def compact(row): return {k:row[k] for k in FIELDS}


def core(rows): return [[r[k] for k in CORE] for r in rows]


def digest(rows):
    return hashlib.sha256(json.dumps(core(rows),separators=(',',':')).encode()).hexdigest()


def load_seed(path):
    raw = path.read_bytes()
    packed = base64.b64decode(raw) if path.suffix=='.b64' else raw
    seed = json.loads(gzip.decompress(packed))
    if seed['scope']!={'job':'Hero','reboot_index':0,'min_level':275}:
        raise RuntimeError('Unexpected checkpoint scope')
    rows = seed['rows']
    if not rows or len(rows)>8000 or len({base.key(r) for r in rows})!=len(rows):
        raise RuntimeError('Invalid checkpoint population')
    if any(r['level']<275 or r['jobName']!='Hero' for r in rows):
        raise RuntimeError('Unexpected checkpoint rows')
    if any(a['level']<b['level'] for a,b in zip(rows,rows[1:])):
        raise RuntimeError('Checkpoint level ordering')
    if digest(rows)!=seed['seed_core_sha256'] or seed['next_position']!=1+10*math.ceil(len(rows)/10):
        raise RuntimeError('Checkpoint hash or cursor mismatch')
    if len(rows)%10 and not seed.get('terminal_observed'):
        raise RuntimeError('Partial checkpoint without terminal evidence')
    return seed


class Resume(base.Collector):
    def __init__(self, seed, out, segment):
        super().__init__('Hero',out)
        self.seed, self.segment = seed, segment
        self.rows = [compact(r) for r in seed['rows']]
        self.seen = {base.key(r) for r in self.rows}
        self.total = seed['total_count']
        self.prior_rows, self.cursor = len(self.rows), seed['next_position']
        self.handoff_valid, self.terminal = False, bool(seed.get('terminal_observed'))
        self.handoff_checks, self.final_checks = [], []
        self.stage = 'handoff'

    async def check_rows(self, position, phase):
        expected = self.rows[position-1:position+9]
        payload = await self.fetch(position,phase)
        if core(payload['ranks'][:len(expected)])!=core(expected):
            raise RuntimeError(f'{phase} snapshot changed at {position}')
        if len(expected)<10 and any(r['level']>=275 for r in payload['ranks'][len(expected):]):
            raise RuntimeError('Target boundary moved')
        return payload

    def add(self, position, payload):
        before = len(self.rows)
        terminal = super().add(position,payload)
        self.rows[before:] = [compact(r) for r in self.rows[before:]]
        return terminal

    def report(self):
        result = super().report()
        result.update(segment=self.segment,stage=self.stage,prior_rows=self.prior_rows,
                      new_rows=len(self.rows)-self.prior_rows,next_position=1+10*math.ceil(len(self.rows)/10),
                      handoff_valid=self.handoff_valid,handoff_checks=self.handoff_checks,
                      terminal_observed=self.terminal,final_checks=self.final_checks,
                      core_sha256=digest(self.rows),reference_count=self.seed['reference_count'],
                      matches_full_reference=(digest(self.rows)==self.seed['reference_core_sha256']),
                      resumable=(self.status=='incomplete_refusal' and self.handoff_valid),
                      strategy='At most two continuation runners in workflow; five-request batches, 0.5s pause, first refusal stops; no retries here',
                      omitted_fields=['characterImgURL','legionLevel','raidPower','tierID','starSum'])
        (self.out/'report.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
        return result

    async def collect(self):
        count = math.ceil(self.prior_rows/10)
        positions = sorted({1,1+10*((count-1)//2),1+10*(count-1)})
        for position in positions:
            await self.check_rows(position,'handoff')
            self.handoff_checks.append(position)
        self.handoff_valid = True
        self.stage = 'scan'
        if not self.terminal:
            for index in range(0,500,5):
                positions = [self.cursor+10*i for i in range(index,min(index+5,500))
                             if self.cursor+10*i<=self.total]
                if not positions: raise RuntimeError('No terminal population evidence')
                payloads = await base.gather_batch(self.fetch,positions)
                for position,payload in zip(positions,payloads):
                    if not self.terminal:
                        self.terminal = self.add(position,payload)
                    elif any(r['level']>=275 for r in payload['ranks']):
                        raise RuntimeError('Target rows after terminal')
                self.report()
                if self.terminal: break
                if len(self.scan)%50==0:
                    print(f'Hero continuation {self.segment}: +{len(self.rows)-self.prior_rows} rows, total {len(self.rows)}',flush=True)
                await asyncio.sleep(.5)
            else: raise RuntimeError('Continuation page budget reached')
        self.stage = 'final_checks'
        count = math.ceil(len(self.rows)/10)
        for position in sorted({1,1+10*((count-1)//2),1+10*(count-1)}):
            await self.check_rows(position,'final_anchor')
            self.final_checks.append(position)
        boundary = len(self.rows)
        before = await self.fetch(boundary,'boundary_anchor')
        if core(before['ranks'][:1])!=core(self.rows[-1:]) or any(r['level']>=275 for r in before['ranks'][1:]):
            raise RuntimeError('Final boundary row changed')
        after = await self.fetch(boundary+1,'boundary_anchor')
        if any(r['level']>=275 for r in after['ranks']): raise RuntimeError('Final boundary moved')
        self.final_checks.extend([boundary,boundary+1])
        self.boundary = boundary
        self.status, self.stage = 'complete_class_observation', 'finished'

    async def run_resume(self):
        await super().run()
        if self.status=='incomplete' and self.handoff_valid and self.error and '429' in self.error:
            self.status = 'incomplete_refusal'
        report = self.report()
        if self.handoff_valid and self.status in ('complete_class_observation','incomplete_refusal'):
            checkpoint = {**self.seed,'rows':self.rows,'next_position':report['next_position'],
                'seed_core_sha256':digest(self.rows),'terminal_observed':self.terminal,
                'refusal_at_utc':max((r['finished_at_utc'] for r in self.requests if r['status']==429),default=None),
                'previous_segment':self.segment}
            packed = gzip.compress(json.dumps(checkpoint,separators=(',',':')).encode(),compresslevel=6,mtime=0)
            (self.out/'checkpoint.json.gz').write_bytes(packed)
        print(json.dumps(report),flush=True)
        return 0 if self.status=='complete_class_observation' else 2


def combine(root):
    reports = []
    for path in root.rglob('report.json'):
        report = json.loads(path.read_text())
        if report.get('segment') not in (1,2): continue
        for stage in ('start','end'):
            env = path.parent/f'environment-{stage}.json'
            report[f'environment_{stage}'] = json.loads(env.read_text()) if env.exists() else None
        report['relative_directory'] = str(path.parent.relative_to(root))
        reports.append(report)
    reports.sort(key=lambda r:r['segment'])
    if not reports: raise RuntimeError('No continuation evidence')
    if len({r['segment'] for r in reports})!=len(reports): raise RuntimeError('Duplicate segment evidence')
    latest = reports[-1]
    complete = latest['status']=='complete_class_observation'
    if len(reports)==2 and reports[1]['prior_rows']!=reports[0]['rows']:
        raise RuntimeError('Checkpoint chain mismatch')
    summary = {'collection_complete':complete,'segments':reports,'rows':latest['rows'],
        'new_rows':latest['rows']-2800,'sent_requests':sum(r['sent_requests'] for r in reports),
        'statuses':dict(sum((__import__('collections').Counter(r['statuses']) for r in reports),__import__('collections').Counter())),
        'matches_full_reference':latest['matches_full_reference'],
        'caveat':'Reference and finite anchor checks support this observation, not guaranteed daily stability or IP-only causality.'}
    checkpoint = root/latest['relative_directory']/'checkpoint.json.gz'
    if checkpoint.exists():
        seed = load_seed(checkpoint)
        compact_bytes = gzip.compress(json.dumps(seed['rows'],separators=(',',':')).encode(),compresslevel=6,mtime=0)
        Path('hero-characters.json.gz').write_bytes(compact_bytes)
        summary['compact_transfer_bytes']=len(compact_bytes)
        summary['compact_sha256']=hashlib.sha256(compact_bytes).hexdigest()
    Path('resume-summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary),flush=True)
    return 0 if complete else 2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true')
    p.add_argument('--seed',type=Path,default=Path('hero_resume_seed.b64'))
    p.add_argument('--segment',type=int,choices=(1,2),default=1)
    p.add_argument('--combine',type=Path)
    args = p.parse_args()
    if args.combine: return combine(args.combine)
    seed = load_seed(args.seed)
    if not args.execute:
        print(json.dumps({'dry_run':True,'seed_rows':len(seed['rows']),'next_position':seed['next_position'],
                          'segment':args.segment,'max_scan_pages':500,'first_refusal_stops':True})); return 0
    if args.segment==2 and seed.get('previous_segment')!=1:
        p.error('Second segment must use first segment checkpoint')
    if seed.get('refusal_at_utc'):
        seconds = max(0,300-(datetime.now(timezone.utc)-datetime.fromisoformat(seed['refusal_at_utc'])).total_seconds())
        if seconds: print(f'Wait {seconds:.1f}s before any Nexon request',flush=True); time.sleep(seconds)
    return asyncio.run(Resume(seed,Path('resume-data')/str(args.segment),args.segment).run_resume())


if __name__=='__main__': raise SystemExit(main())
