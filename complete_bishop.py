"""Bounded complete Bishop observation: at most eight collectors, no added cooldown."""
import argparse
import asyncio
import gzip
import hashlib
import json
import math
import os
from collections import Counter
from pathlib import Path
from resume_bishop import Bishop,SCOPE,fresh_seed
from resume_hero import digest
from matrix_probe import key

MAX_COLLECTORS=8


def checkpoint(path):
    data=json.loads(gzip.decompress(path.read_bytes()))
    rows=data['rows']
    if data['scope']!=SCOPE or not 0<len(rows)<=50000:
        raise RuntimeError('Invalid checkpoint scope/population')
    if len({key(r) for r in rows})!=len(rows) or any(r['level']<275 or r['jobName']!='Bishop' for r in rows):
        raise RuntimeError('Invalid checkpoint identities')
    if any(a['level']<b['level'] for a,b in zip(rows,rows[1:])):
        raise RuntimeError('Checkpoint ordering changed')
    if digest(rows)!=data['seed_core_sha256'] or data['next_position']!=1+10*math.ceil(len(rows)/10):
        raise RuntimeError('Checkpoint hash/cursor mismatch')
    if not isinstance(data['total_count'],int) or data['total_count']<len(rows):
        raise RuntimeError('Invalid totalCount')
    if len(rows)%10 and not data.get('terminal_observed'):
        raise RuntimeError('Partial checkpoint without boundary')
    return data


class Full(Bishop):
    def report(self):
        result=super().report()
        result.update(resumable=(self.status in ('incomplete_refusal','incomplete_budget') and self.handoff_valid and bool(self.rows)),
            max_collectors=MAX_COLLECTORS,initial_rows=self.seed['initial_rows'],initial_mode=self.seed['initial_mode'],
            strategy='Up to eight bounded Bishop collectors; first refusal stops, accurate checkpoint handoff, no extra cooldown')
        (self.out/'report.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
        return result

    async def run_resume(self):
        await super().run_resume()
        if self.status=='incomplete' and self.handoff_valid and self.error and '500-page segment budget' in self.error:
            self.status='incomplete_budget'
        report=self.report()
        if self.handoff_valid and self.rows and self.status in ('complete_class_observation','incomplete_refusal','incomplete_budget'):
            data={**self.seed,'rows':self.rows,'total_count':self.total,'seed_core_sha256':digest(self.rows),
                'next_position':report['next_position'],'previous_segment':self.segment,
                'terminal_observed':self.terminal,'refusal_at_utc':report['last_refusal_at_utc']}
            (self.out/'checkpoint.json.gz').write_bytes(gzip.compress(json.dumps(data,separators=(',',':')).encode(),mtime=0))
        print(json.dumps(report),flush=True)
        return 0 if self.status=='complete_class_observation' else 2


def combine(root):
    reports=[]
    for path in root.rglob('report.json'):
        r=json.loads(path.read_text())
        if r.get('scope')!=SCOPE: continue
        r['relative_directory']=str(path.parent.relative_to(root))
        for stage in ('start','end'):
            env=path.parent/f'environment-{stage}.json'
            r[f'environment_{stage}']=json.loads(env.read_text()) if env.exists() else None
        reports.append(r)
    reports.sort(key=lambda r:r['segment'])
    if not reports or len(reports)>MAX_COLLECTORS or [r['segment'] for r in reports]!=list(range(1,len(reports)+1)):
        raise RuntimeError('Missing or duplicate collector segments')
    for before,after in zip(reports,reports[1:]):
        if before['rows']!=after['prior_rows'] or not after['handoff_valid']:
            raise RuntimeError('Checkpoint chain mismatch')
    latest=reports[-1]
    summary={'scope':SCOPE,'collection_complete':latest['status']=='complete_class_observation',
        'initial_rows':reports[0]['initial_rows'],'initial_mode':reports[0]['initial_mode'],
        'rows':latest['rows'],'new_rows':latest['rows']-reports[0]['initial_rows'],
        'sent_requests':sum(r['sent_requests'] for r in reports),'segments':reports,
        'statuses':dict(sum((Counter(r['statuses']) for r in reports),Counter())),
        'caveat':'Finite snapshot checks; one class, no daily stability or all-GMS inference.'}
    packet=root/latest['relative_directory']/'checkpoint.json.gz'
    if packet.exists():
        seed=checkpoint(packet)
        packed=gzip.compress(json.dumps(seed['rows'],separators=(',',':')).encode(),compresslevel=6,mtime=0)
        Path('bishop-full-characters.json.gz').write_bytes(packed)
        summary.update(compact_transfer_bytes=len(packed),compact_sha256=hashlib.sha256(packed).hexdigest(),core_sha256=digest(seed['rows']))
    Path('bishop-full-summary.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps(summary),flush=True)
    return 0 if summary['collection_complete'] else 2


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--execute',action='store_true');p.add_argument('--segment',type=int,choices=range(1,9),default=1)
    p.add_argument('--seed',type=Path);p.add_argument('--combine',type=Path);p.add_argument('--inspect',type=Path)
    args=p.parse_args()
    if args.combine: return combine(args.combine)
    if args.inspect:
        r=json.loads(args.inspect.read_text()) if args.inspect.exists() else {}
        with open(os.environ['GITHUB_OUTPUT'],'a') as f:f.write('resumable='+str(bool(r.get('resumable'))).lower()+'\n')
        print(json.dumps({'status':r.get('status'),'resumable':r.get('resumable')}));return 0
    seed=checkpoint(args.seed) if args.seed else fresh_seed()
    if args.segment==1:
        seed['initial_rows'],seed['initial_mode']=len(seed['rows']),'resume' if args.seed else 'fresh'
    elif args.seed is None or seed.get('previous_segment')!=args.segment-1:
        p.error('Each next collector must use its predecessor checkpoint')
    if not args.execute:
        print(json.dumps({'dry_run':True,'scope':SCOPE,'segment':args.segment,'prior_rows':len(seed['rows']),
            'next_position':seed['next_position'],'max_collectors':8,'added_cooldown_seconds':0}));return 0
    return asyncio.run(Full(seed,Path('bishop-full-data')/str(args.segment),args.segment).run_resume())


if __name__=='__main__':raise SystemExit(main())
