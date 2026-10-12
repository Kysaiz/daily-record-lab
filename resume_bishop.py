"""Two-runner Bishop observation with no deliberately added cooldown.

Tests immediate continuation, not necessarily complete Bishop coverage.
"""
import argparse
import asyncio
import gzip
import hashlib
import json
import math
from collections import Counter
from datetime import datetime
from pathlib import Path
import matrix_probe as base
from resume_hero import Resume, compact, core, digest

SCOPE={'job':'Bishop','reboot_index':0,'min_level':275}


def fresh_seed():
    return {'version':1,'scope':SCOPE,'rows':[],'total_count':None,'next_position':1,
            'seed_core_sha256':digest([]),'reference_count':None,'reference_core_sha256':None}


def load_checkpoint(path):
    seed=json.loads(gzip.decompress(path.read_bytes()))
    rows=seed['rows']
    if seed['scope']!=SCOPE or not rows or len(rows)>10000:
        raise RuntimeError('Invalid Bishop checkpoint scope/population')
    if len({base.key(r) for r in rows})!=len(rows) or any(r['jobName']!='Bishop' or r['level']<275 for r in rows):
        raise RuntimeError('Invalid checkpoint characters')
    if digest(rows)!=seed['seed_core_sha256'] or seed['next_position']!=1+10*math.ceil(len(rows)/10):
        raise RuntimeError('Checkpoint hash/cursor mismatch')
    if len(rows)%10 and not seed.get('terminal_observed'):
        raise RuntimeError('Partial checkpoint without terminal evidence')
    return seed


class Bishop(Resume):
    def __init__(self,seed,out,segment):
        base.Collector.__init__(self,'Bishop',out)
        self.seed,self.segment=seed,segment
        self.rows=[compact(r) for r in seed['rows']]
        self.seen={base.key(r) for r in self.rows}
        self.total=seed['total_count']
        self.prior_rows,self.cursor=len(self.rows),seed['next_position']
        self.handoff_valid,self.terminal=False,bool(seed.get('terminal_observed'))
        self.handoff_checks,self.final_checks=[],[]
        self.stage='fresh_scan' if not self.prior_rows else 'handoff'

    def report(self):
        result=super().report()
        sent=[r for r in self.requests if r.get('sent_at_utc')]
        refusals=[r for r in self.requests if r['status']==429]
        result.update(matches_full_reference=None,reference_count=None,
            resumable=result['resumable'] and len(self.rows)>0,
            strategy='At most two Bishop runners, five-request batches and 0.5s pause; first refusal stops; no added cooldown',
            artificial_cooldown_seconds=0,
            first_sent_at_utc=min((r['sent_at_utc'] for r in sent),default=None),
            first_scan_sent_at_utc=min((r['sent_at_utc'] for r in sent if r['phase']=='scan'),default=None),
            last_refusal_at_utc=max((r['finished_at_utc'] for r in refusals),default=None),
            no_full_reference_caveat='No prior complete Bishop reference. A successful continuation is distinct from full population completion.')
        (self.out/'report.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
        return result

    async def collect(self):
        first=None
        if self.prior_rows:
            count=math.ceil(self.prior_rows/10)
            for position in sorted({1,1+10*((count-1)//2),1+10*(count-1)}):
                await self.check_rows(position,'handoff')
                self.handoff_checks.append(position)
        else:
            first=await self.fetch(1,'scan')
            if self.total==0: raise RuntimeError('Zero totalCount cannot prove valid class coverage')
            self.seed['total_count']=self.total
        self.handoff_valid=True
        self.stage='scan'
        if not self.terminal:
            for index in range(0,500,5):
                positions=[self.cursor+10*i for i in range(index,min(index+5,500)) if self.cursor+10*i<=self.total]
                if not positions: raise RuntimeError('No terminal population evidence')
                remaining=[p for p in positions if not (first is not None and p==1)]
                payloads=await base.gather_batch(self.fetch,remaining)
                by_position=dict(zip(remaining,payloads))
                if first is not None and index==0: by_position[1]=first
                for position in positions:
                    payload=by_position[position]
                    if not self.terminal: self.terminal=self.add(position,payload)
                    elif any(r['level']>=275 for r in payload['ranks']):
                        raise RuntimeError('Target rows after terminal')
                self.report()
                if self.terminal: break
                if len(self.scan)%50==0:
                    print(f'Bishop segment {self.segment}: {len(self.rows)} cumulative rows',flush=True)
                await asyncio.sleep(.5)
            else: raise RuntimeError('500-page segment budget reached')
        self.stage='final_checks'
        count=math.ceil(len(self.rows)/10)
        for position in sorted({1,1+10*((count-1)//2),1+10*(count-1)}):
            await self.check_rows(position,'final_anchor')
            self.final_checks.append(position)
        boundary=len(self.rows)
        if not boundary: raise RuntimeError('Empty LV275 target is not a full stability observation')
        before=await self.fetch(boundary,'boundary_anchor')
        if core(before['ranks'][:1])!=core(self.rows[-1:]) or any(r['level']>=275 for r in before['ranks'][1:]):
            raise RuntimeError('Final boundary changed')
        after=await self.fetch(boundary+1,'boundary_anchor')
        if any(r['level']>=275 for r in after['ranks']): raise RuntimeError('Final next row still eligible')
        self.final_checks.extend([boundary,boundary+1])
        self.boundary=boundary
        self.status,self.stage='complete_class_observation','finished'


def combine(root):
    reports=[]
    for path in root.rglob('report.json'):
        r=json.loads(path.read_text())
        if r.get('scope')!=SCOPE: continue
        r['relative_directory']=str(path.parent.relative_to(root))
        for stage in ('start','end'):
            p=path.parent/f'environment-{stage}.json'
            r[f'environment_{stage}']=json.loads(p.read_text()) if p.exists() else None
        reports.append(r)
    reports.sort(key=lambda r:r['segment'])
    if not reports or [r['segment'] for r in reports]!=list(range(1,len(reports)+1)) or len(reports)>2:
        raise RuntimeError('Missing/duplicate segment reports')
    if len(reports)==2 and reports[1]['prior_rows']!=reports[0]['rows']:
        raise RuntimeError('Checkpoint chain mismatch')
    continuation=False
    gap,scan_gap=None,None
    if len(reports)==2:
        previous=reports[0]['last_refusal_at_utc']
        if previous and reports[1]['first_sent_at_utc']:
            gap=(datetime.fromisoformat(reports[1]['first_sent_at_utc'])-datetime.fromisoformat(previous)).total_seconds()
        if previous and reports[1]['first_scan_sent_at_utc']:
            scan_gap=(datetime.fromisoformat(reports[1]['first_scan_sent_at_utc'])-datetime.fromisoformat(previous)).total_seconds()
        continuation=reports[1]['handoff_valid'] and reports[1]['new_rows']>0
    latest=reports[-1]
    summary={'collection_complete':latest['status']=='complete_class_observation',
        'immediate_continuation_observed':continuation,
        'natural_refusal_to_next_request_seconds':gap,'natural_refusal_to_next_scan_seconds':scan_gap,
        'no_added_cooldown_test_passed':continuation and gap is not None and 0<=gap<300,
        'configured_artificial_cooldown_seconds':0,
        'rows':latest['rows'],'segments':reports,'sent_requests':sum(r['sent_requests'] for r in reports),
        'statuses':dict(sum((Counter(r['statuses']) for r in reports),Counter())),
        'caveat':'At most two collectors. Continuation success is not full Bishop or daily coverage; startup delay and IP change are confounded.'}
    checkpoint=root/latest['relative_directory']/'checkpoint.json.gz'
    if checkpoint.exists():
        seed=load_checkpoint(checkpoint)
        packed=gzip.compress(json.dumps(seed['rows'],separators=(',',':')).encode(),compresslevel=6,mtime=0)
        Path('bishop-characters.json.gz').write_bytes(packed)
        summary.update(compact_transfer_bytes=len(packed),compact_sha256=hashlib.sha256(packed).hexdigest())
    Path('bishop-summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary),flush=True)
    return 0 if summary['no_added_cooldown_test_passed'] else 2


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute',action='store_true')
    parser.add_argument('--segment',type=int,choices=(1,2),default=1)
    parser.add_argument('--seed',type=Path)
    parser.add_argument('--combine',type=Path)
    args=parser.parse_args()
    if args.combine: return combine(args.combine)
    if args.segment==2 and args.seed is None: parser.error('Second runner requires checkpoint')
    seed=load_checkpoint(args.seed) if args.seed else fresh_seed()
    if args.segment==2 and seed.get('previous_segment')!=1: parser.error('Expected first-segment checkpoint')
    if not args.execute:
        print(json.dumps({'dry_run':True,'job':'Bishop','segment':args.segment,'seed_rows':len(seed['rows']),
            'next_position':seed['next_position'],'extra_cooldown_seconds':0,'max_collectors':2,'max_scan_pages_per_runner':500})); return 0
    return asyncio.run(Bishop(seed,Path('bishop-data')/str(args.segment),args.segment).run_resume())


if __name__=='__main__': raise SystemExit(main())
