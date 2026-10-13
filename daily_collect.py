"""One bounded task segment and compact stage export for the NAS."""
import argparse,asyncio,gzip,hashlib,json,math,os,time,zipfile
from pathlib import Path
import matrix_probe as base

FIELDS=('characterName','worldID','jobName','level','exp','rank','gap','legionLevel','raidPower')
base.MAX_PAGES,base.MAX_SECONDS,base.MAX_EXTRA,base.MAX_429=500,900,0,1


def packed(value):return gzip.compress(json.dumps(value,separators=(',',':')).encode(),mtime=0)
def digest(value):return hashlib.sha256(json.dumps(value,separators=(',',':')).encode()).hexdigest()
def compact(row):return {k:row[k] for k in FIELDS}


def validate(payload,position,task,total=None):
    count,rows=payload.get('totalCount'),payload.get('ranks')
    if type(count) is not int or count<0 or not isinstance(rows,list):raise RuntimeError('Invalid response schema')
    if payload.get('pageIndex')!=position:raise RuntimeError('Unexpected pageIndex')
    if total is not None and total!=count:raise RuntimeError('totalCount changed')
    if len(rows)!=max(0,min(10,count-position+1)):raise RuntimeError('Unexpected empty/short response')
    if task['type']=='job' and any(r['jobName']!=task['id'] for r in rows):raise RuntimeError('Unexpected class')
    if task['type']=='legion' and any(r['worldID']!=task['id'] for r in rows):raise RuntimeError('Unexpected world')
    field='level' if task['type']=='job' else 'legionLevel'
    if any(a[field]<b[field] for a,b in zip(rows,rows[1:])):raise RuntimeError('Page ordering changed')
    if len({base.key(r) for r in rows})!=len(rows):raise RuntimeError('Duplicate within page')
    return count


class Session:
    def __init__(self,session,task):self.session,self.task=session,task
    def get(self,url,**kwargs):
        kwargs['params'].update(type=self.task['type'],id=self.task['id'])
        return self.session.get(url,**kwargs)


class TaskCollector(base.Collector):
    def __init__(self,task,out,seed=None):
        super().__init__(task['key'],out);self.task=task;self.seed=seed
        self.segment=int(os.getenv('SEGMENT','1'));self.prior_rows=0;self.cursor=1;self.terminal=False
        self.handoff_valid=False;self.committed=[];self.previous_hash=None
        if seed:
            if seed['task']!=task or seed['rows_sha256']!=digest(seed['rows']):raise RuntimeError('Invalid checkpoint scope/hash')
            if seed['next_position']!=len(seed['rows'])+1 or len(seed['rows'])%10:raise RuntimeError('Invalid checkpoint cursor')
            self.rows=seed['rows'];self.seen={base.key(r) for r in self.rows};self.total=seed['total_count']
            if len(self.rows)!=len(self.seen):raise RuntimeError('Duplicate checkpoint')
            self.prior_rows=len(self.rows);self.cursor=seed['next_position'];self.previous_hash=seed['rows_sha256']

    def report(self):
        # A deep Legion checkpoint may contain hundreds of thousands of rows. Never serialize
        # its whole prefix every five pages: only the final checkpoint needs those records.
        from collections import Counter
        r={'started_at_utc':self.started,'updated_at_utc':base.stamp(),'elapsed_seconds':time.monotonic()-self.clock,
           'status':self.status,'error':self.error,'total_count':self.total,'rows':len(self.rows),
           'unique_characters':len(self.seen),'sent_requests':sum(bool(x.get('sent_at_utc')) for x in self.requests),
           'statuses':dict(Counter(str(x['status']) for x in self.requests)),
           'successful_scan_pages':len(self.committed),'max_concurrent_requests':self.max_active}
        snapshot=bool(self.error) and any(s in self.error for s in ('snapshot changed','totalCount changed','boundary changed','ordering changed','duplicate'))
        r.update(task=self.task,segment=self.segment,prior_rows=self.prior_rows,previous_hash=self.previous_hash,
                 cursor=len(self.rows)+1,terminal=self.terminal,handoff_valid=self.handoff_valid,
                 rows_sha256=digest(self.rows) if self.status!='running' else None,
                 committed_positions=self.committed,snapshot_changed=snapshot,
                 complete=self.status=='complete_class_observation',
                 resumable=self.handoff_valid and bool(self.rows) and not snapshot and self.error is not None and
                     ('429' in self.error or 'Page segment budget' in self.error),
                 omitted_image_urls=True)
        (self.out/'report.json').write_text(json.dumps(r,indent=2));return r

    async def check(self,position,phase):
        payload=await self.fetch(position,phase);expected=self.rows[position-1:position+9]
        if [compact(r) for r in payload['ranks'][:len(expected)]]!=expected:raise RuntimeError('snapshot changed')
        if len(expected)<10 and self.task['type']=='job' and any(r['level']>=275 for r in payload['ranks'][len(expected):]):
            raise RuntimeError('boundary changed')
        return payload

    def commit(self,position,payload):
        source=payload['ranks'];rows=[compact(r) for r in source if self.task['type']=='legion' or r['level']>=275]
        field='level' if self.task['type']=='job' else 'legionLevel'
        if self.rows and rows and self.rows[-1][field]<rows[0][field]:raise RuntimeError('Cross-page ordering changed')
        if any(base.key(r) in self.seen for r in rows):raise RuntimeError('Cross-page duplicate')
        self.rows.extend(rows);self.seen.update(base.key(r) for r in rows);self.committed.append(position)
        self.scan[position]=base.signature(payload)
        self.terminal=len(rows)<len(source) or position+len(source)>self.total

    async def collect(self):
        self.session=Session(self.session,self.task)
        if self.seed:
            count=math.ceil(len(self.rows)/10)
            for pos in sorted({1,1+10*((count-1)//2),1+10*(count-1)}):await self.check(pos,'handoff')
        self.handoff_valid=True
        first=await self.fetch(self.cursor,'scan')
        if self.total==0 and self.task['type']=='legion':raise RuntimeError('Unexpected empty Legion world')
        self.commit(self.cursor,first)
        pages=1;next_position=self.cursor+10
        while not self.terminal and pages<500:
            positions=[next_position+10*i for i in range(min(5,500-pages)) if next_position+10*i<=self.total]
            if not positions:raise RuntimeError('Missing terminal evidence')
            payloads=await base.gather_batch(self.fetch,positions)
            for pos,payload in zip(positions,payloads):
                if not self.terminal:self.commit(pos,payload)
                elif self.task['type']=='job' and any(r['level']>=275 for r in payload['ranks']):raise RuntimeError('boundary changed')
            pages+=len(positions);next_position+=10*len(positions);self.report()
            if not self.terminal:await asyncio.sleep(.5)
        if not self.terminal:raise RuntimeError('Page segment budget reached')
        count=math.ceil(len(self.rows)/10)
        if not self.rows:
            await self.check(1,'final_anchor')
        else:
            for pos in sorted({1,1+10*((count-1)//2),1+10*(count-1)}):await self.check(pos,'final_anchor')
            # Validate the actual final row and next row, including a partial target page.
            await self.check(len(self.rows),'boundary_anchor')
            after=await self.fetch(len(self.rows)+1,'boundary_anchor')
            if self.task['type']=='legion' and after['ranks']:raise RuntimeError('boundary changed')
            if self.task['type']=='job' and any(r['level']>=275 for r in after['ranks']):raise RuntimeError('boundary changed')
        self.boundary=len(self.rows);self.status='complete_class_observation'

    def audit_committed(self):
        reconstructed=[]
        requests={r['position']:r for r in self.requests if r['phase']=='scan' and r['status']==200 and 'body_bytes' in r}
        for pos in self.committed:
            r=requests[pos];path=self.out/f'{r["request_id"]:05d}-scan-{pos}.json'
            payload=json.loads(path.read_bytes());validate(payload,pos,self.task,self.total)
            reconstructed.extend(compact(row) for row in payload['ranks'] if self.task['type']=='legion' or row['level']>=275)
        if self.rows[self.prior_rows:]!=reconstructed:raise RuntimeError('Raw committed prefix audit failed')


async def execute(task,out,seed):
    base.validate=lambda payload,position,job,total=None:validate(payload,position,task,total)
    collector=TaskCollector(task,out,seed)
    await collector.run();collector.audit_committed();r=collector.report();r['raw_segment_audit_passed']=True
    (out/'report.json').write_text(json.dumps(r,indent=2))
    if r['complete'] or r['resumable']:
        state={'task':task,'total_count':collector.total,'rows':collector.rows,'rows_sha256':digest(collector.rows),
               'next_position':len(collector.rows)+1,'segment':collector.segment,'complete':r['complete']}
        (out/'checkpoint.json.gz').write_bytes(packed(state))
    return 0 if r['complete'] else 2


def export(root):
    tasks=json.loads(os.environ['TASKS']);reports=[];entries=[];next_tasks=[]
    with zipfile.ZipFile('daily-compact.zip','w',compression=zipfile.ZIP_DEFLATED) as z:
        for task in tasks:
            path=root/('state-'+task['key'])/'report.json'
            if not path.exists():raise RuntimeError('Missing task report: '+task['key'])
            r=json.loads(path.read_text());reports.append(r)
            if r['task']!=task or not r.get('raw_segment_audit_passed'):raise RuntimeError('Unaudited task')
            for phase in ('start','end'):
                r['environment_'+phase]=json.loads((path.parent/f'environment-{phase}.json').read_text())
            if r['complete']:
                packet=json.loads(gzip.decompress((path.parent/'checkpoint.json.gz').read_bytes()))
                if packet['rows_sha256']!=digest(packet['rows']):raise RuntimeError('Checkpoint checksum mismatch')
                rows=[row for row in packet['rows'] if row['level']>=275]
                data=packed(rows);filename=task['key']+'.json.gz';z.writestr(filename,data)
                entries.append({'task':task,'file':filename,'sha256':hashlib.sha256(data).hexdigest(),
                               'row_count':len(rows),'scanned_rows':len(packet['rows']),'complete':True,
                               'observed_at':r['updated_at_utc']})
            elif r['resumable'] and int(os.environ['SEGMENT'])<task['max_segments']:next_tasks.append(task)
        manifest={'schema_version':1,'region':'na','run_id':os.environ['GITHUB_RUN_ID'],
                  'request_id':os.environ['REQUEST_ID'],'day':os.environ['SNAPSHOT_DAY'],
                  'segment':int(os.environ['SEGMENT']),'requested_tasks':tasks,'entries':entries,
                  'reports':reports,'next_tasks':next_tasks}
        z.writestr('manifest.json',json.dumps(manifest,separators=(',',':')))
    print(json.dumps({'completed':len(entries),'requested':len(tasks),'remaining':len(next_tasks)}))


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--task');p.add_argument('--seed',type=Path);p.add_argument('--export',type=Path)
    args=p.parse_args()
    if args.export:export(args.export)
    else:
        task=json.loads(args.task);seed=json.loads(gzip.decompress(args.seed.read_bytes())) if args.seed else None
        if seed and seed.get('segment')!=int(os.environ['SEGMENT'])-1:raise RuntimeError('Wrong checkpoint segment')
        raise SystemExit(asyncio.run(execute(task,Path('daily-data')/task['key'],seed)))
