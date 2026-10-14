"""Bounded chains, contiguous Legion shards, and compact certified exports."""
import argparse, asyncio, gzip, hashlib, json, os, time, zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import daily_collect_v131 as old
import stability_v143 as stability
import matrix_probe as base

ORDER = (45, 70, 1, 19)
_original_budget=base.Collector.budget
def guarded_budget(self):
    _original_budget(self)
    if os.getenv('DEADLINE') and expired():raise RuntimeError('Daily deadline reached')
base.Collector.budget=guarded_budget

def read(path): return json.loads(Path(path).read_text())
def write(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
def output(**values):
    if os.getenv('GITHUB_OUTPUT'):
        with open(os.environ['GITHUB_OUTPUT'], 'a') as f:
            for k, v in values.items(): f.write(f'{k}={str(v).lower() if isinstance(v,bool) else v}\n')
def expired(): return datetime.now(timezone.utc) >= datetime.fromisoformat(os.environ['DEADLINE'])

def partitions(total, count=8):
    if type(total) is not int or total < 0: raise ValueError('Invalid boundary')
    count = min(count,total)
    return [(total*i//count+1,total*(i+1)//count) for i in range(count)]

def validate_response(payload,position,task,total=None):
    # Keep original response-length proof. Exact within-page repeats are allowed
    # only for Legion, where rank continuity is independently reconstructed.
    if task['type']!='legion':return old.validate(payload,position,task,total)
    count=payload.get('totalCount');rows=payload.get('ranks')
    if type(count) is not int or count<0 or not isinstance(rows,list):raise RuntimeError('Invalid response schema')
    if payload.get('pageIndex')!=position:raise RuntimeError('Unexpected pageIndex')
    if total is not None and total!=count:raise RuntimeError('totalCount changed')
    if len(rows)!=max(0,min(10,count-position+1)):raise RuntimeError('Unexpected empty/short response')
    seen={}
    for row in rows:
        if row['worldID']!=task['id']:raise RuntimeError('Unexpected world')
        identity=base.key(row);value=old.compact(row)
        if identity in seen and seen[identity]!=value:raise RuntimeError('Conflicting duplicate within page; data anomaly')
        seen[identity]=value
    if any(a['legionLevel']<b['legionLevel'] for a,b in zip(rows,rows[1:])):raise RuntimeError('Page ordering anomaly')
    return count

class JobCollector(old.TaskCollector):
    async def fetch(self,position,phase):
        payload=await super().fetch(position,phase)
        from active_avatar_urls import captured
        if not hasattr(self,'avatar_urls'):self.avatar_urls={}
        for row in captured(payload['ranks'],self.task,base.stamp()):
            self.avatar_urls[(row['name'].casefold(),row['world'])]=row
        return payload
    def report(self):
        r=super().report();r['snapshot_changed']=stability.suspect(r.get('error'))
        r['avatar_urls']=list(getattr(self,'avatar_urls',{}).values())
        r['failure_kind']='suspected_snapshot_update' if r['snapshot_changed'] else ('none' if not r['error'] else 'transport_or_data_anomaly')
        write(self.out/'report.json',r);return r

class RangeCollector(old.TaskCollector):
    def __init__(self,task,out,seed=None):
        super().__init__(task,out,None)
        self.lo,self.hi = task['start_rank'],task['end_rank']
        self.total=task['total_count']; self.seed=seed; self.cursor=max(1,self.lo-4)
        if seed:
            if seed['task']!=task or seed['rows_sha256']!=old.digest(seed['rows']): raise RuntimeError('Invalid shard checkpoint')
            self.rows=seed['rows']; self.prior_rows=len(self.rows); self.previous_hash=seed['rows_sha256']
            self.seen={base.key(r) for r in self.rows}
            if len(self.seen)!=len(self.rows) or any(not self.lo<=r['rank']<=self.hi or r['legionLevel']<4000 for r in self.rows):
                raise RuntimeError('Invalid shard prefix')
            if seed['next_position']!=self.next_position(): raise RuntimeError('Invalid shard cursor')

    def next_position(self):
        if not self.rows:return self.cursor
        last=self.rows[-1]['rank']
        members=sum(r['rank']==last for r in self.rows[-10:])
        if members==10:members=sum(r['rank']==last for r in self.rows)
        return max(1,last+members-5)

    def append_range(self,source,target):
        existing={base.key(r):r for r in target}
        for raw in source:
            row=old.compact(raw)
            if not self.lo<=row['rank']<=self.hi:continue
            if row['legionLevel']<4000:raise RuntimeError('boundary changed inside planned shard')
            key=base.key(row)
            if key in existing:
                if existing[key]!=row:raise RuntimeError('snapshot changed: conflicting identity overlap')
                continue
            # Different characters may legally share one display rank.
            if target and row['rank']<target[-1]['rank']:raise RuntimeError('Unexplained rank reversal; data anomaly')
            if target and target[-1]['legionLevel']<row['legionLevel']:raise RuntimeError('ordering changed')
            target.append(row);existing[key]=row

    async def anchor(self,rank,phase):
        payload=await self.fetch(max(1,rank-4),phase)
        expected={base.key(r):r for r in self.rows}
        matched=0
        for raw in payload['ranks']:
            row=old.compact(raw);key=base.key(row)
            if key in expected:
                if expected[key]!=row:raise RuntimeError('snapshot changed: conflicting identity anchor')
                matched+=1
        if not matched:raise RuntimeError('Missing shard identity anchor; data anomaly')
        return payload

    async def collect_legion(self):
        if self.seed:
            for rank in sorted({self.rows[0]['rank'],self.rows[len(self.rows)//2]['rank'],self.rows[-1]['rank']}):await self.anchor(rank,'handoff')
        self.handoff_valid=True
        terminal=False
        for _ in range(500):
            pos=self.next_position()
            payload=await self.fetch(pos,'scan');target=list(self.rows)
            self.append_range(payload['ranks'],target)
            new=target[len(self.rows):]
            terminal=any(r['rank']>self.hi for r in payload['ranks']) or pos+len(payload['ranks'])>self.total
            if not new and not terminal:
                if not self.rows and all(r['rank']<self.lo for r in payload['ranks']):
                    self.cursor=pos+5
                else:raise RuntimeError('Legion pagination made no progress; tie window requires investigation')
            self.rows.extend(new);self.seen.update(base.key(r) for r in new)
            self.committed.append(pos)
            if terminal:break
            if len(self.committed)%5==0:self.report()
            await asyncio.sleep(.1)
        if not terminal:raise RuntimeError('Page segment budget reached')
        if self.rows:
            for rank in sorted({self.rows[0]['rank'],self.rows[len(self.rows)//2]['rank'],self.rows[-1]['rank']}):await self.anchor(rank,'final_anchor')
        self.terminal=True;self.boundary=self.hi;self.status='complete_class_observation'

    def report(self):
        r=super().report();r['cursor']=self.next_position()
        r['snapshot_changed']=stability.suspect(r.get('error'))
        r['probe_positions']=sorted({max(1,self.lo-4),max(1,(self.lo+self.hi)//2-4),max(1,self.hi-4)})
        write(self.out/'report.json',r);return r

    def audit_committed(self):
        rebuilt=list(self.rows[:self.prior_rows])
        requests={r['position']:r for r in self.requests if r['phase']=='scan' and r['status']==200 and 'body_bytes' in r}
        for pos in self.committed:
            req=requests[pos];payload=read(self.out/f'{req["request_id"]:05d}-scan-{pos}.json')
            validate_response(payload,pos,self.task,self.total);self.append_range(payload['ranks'],rebuilt)
        if rebuilt!=self.rows:raise RuntimeError('Raw shard audit failed')

class PlanCollector(old.TaskCollector):
    def __init__(self,task,out):super().__init__(task,out);self.plan=None
    async def rank(self,rank,phase='plan'):
        payload=await self.fetch(max(1,rank-4),phase)
        # Competition ranks can tie and skip. A target within a skipped rank
        # belongs to the preceding tie group, not to the following character.
        eligible=[old.compact(r) for r in payload['ranks'] if r['rank']<=rank]
        if not eligible:raise RuntimeError('Missing planning interval; data anomaly')
        last=max(r['rank'] for r in eligible);group=[r for r in eligible if r['rank']==last]
        if len({r['legionLevel'] for r in group})!=1:raise RuntimeError('Inconsistent tied Legion levels; data anomaly')
        return sorted(group,key=lambda r:base.key(r))[0]
    async def collect_legion(self):
        first=await self.fetch(1,'plan')
        if not self.total or first['ranks'][0]['rank']!=1:raise RuntimeError('Unexpected empty/offset world')
        lo,hi=0,self.total+1
        while hi-lo>1:
            middle=(lo+hi)//2;row=await self.rank(middle)
            if row['legionLevel']>=4000:lo=middle
            else:hi=middle
            await asyncio.sleep(.5)
        anchors={}
        for rank in sorted({1,max(1,lo//2),max(1,lo),min(self.total,lo+1)}): anchors[str(rank)]=await self.rank(rank,'plan_anchor')
        if lo and anchors[str(lo)]['legionLevel']<4000:raise RuntimeError('boundary changed')
        if lo<self.total and anchors[str(lo+1)]['legionLevel']>=4000:raise RuntimeError('boundary changed')
        self.plan={'task':self.task,'total_count':self.total,'boundary':lo,'anchors':anchors,
                   'planned_at_utc':base.stamp(),'shards':[
                   {**self.task,'key':self.task['key']+f'-s{i}', 'start_rank':a,'end_rank':b,
                    'total_count':self.total,'plan_hash':old.digest(anchors)} for i,(a,b) in enumerate(partitions(lo))]}
        self.status='complete_class_observation'

def failures(task,error):
    return {'task':task,'complete':False,'resumable':False,'snapshot_changed':False,
            'raw_segment_audit_passed':False,'error':error,'status':'incomplete','rows':0,
            'updated_at_utc':base.stamp(),'statuses':{},'sent_requests':0}

async def execute(task,folder,seed):
    base.validate=lambda payload,pos,job,total=None:validate_response(payload,pos,task,total)
    collector=(RangeCollector if 'start_rank' in task else JobCollector)(task,folder,seed)
    await collector.run();collector.audit_committed();report=collector.report()
    report['raw_segment_audit_passed']=True
    first_429=report.get('sent_requests')==1 and report.get('statuses')=={'429':1} and not report.get('snapshot_changed')
    report['first_request_429']=first_429
    if first_429 and seed is not None:
        # No new scan was committed: retain the previously audited seed.
        # The next runner must still perform its normal handoff verification.
        if collector.rows!=seed['rows']:raise RuntimeError('Refused handoff modified seed')
        report['preserved_seed_after_first_429']=True
        report['resumable']=True
    for phase in ('start','end'):report['environment_'+phase]=read(folder/f'environment-{phase}.json')
    write(folder/'report.json',report)
    if report['complete'] or report['resumable'] or (collector.handoff_valid and collector.rows and network_error(report['error'])):
        packet={'task':task,'total_count':collector.total,'rows':collector.rows,'rows_sha256':old.digest(collector.rows),
                'next_position':collector.next_position() if isinstance(collector,RangeCollector) else len(collector.rows)+1,'complete':report['complete'],'segment':collector.segment}
        (folder/'checkpoint.json.gz').write_bytes(old.packed(packet))
    return report

def network_error(error):return bool(error) and error.startswith(('ClientOSError:','ClientConnectionError:','ServerDisconnectedError:','ServerTimeoutError:','ConnectionResetError:','TimeoutError:'))

def decide(report,control,task):
    control=dict(control);mode='done'
    if not report['complete'] and not any(str(code) in ('401','403') for code in report.get('statuses',{})):
        if stability.suspect(report.get('error')) and task['type']=='job' and control.get('resets',0)<1:
            control['resets']=control.get('resets',0)+1
            control.pop('not_before',None);control['stability_required']=True;control['stability_report']=report;mode='fresh'
        elif report.get('resumable'):mode='resume'
        elif report.get('first_request_429'):mode='fresh'
        elif network_error(report.get('error')) and control.get('network_retries',0)<1:
            control['network_retries']=1;mode='resume' if control.get('checkpoint_available') else 'fresh'
    control['mode']=mode;return control

def phase(task,number,incoming,out):
    out.mkdir(parents=True,exist_ok=True)
    previous=read(incoming/'control.json') if incoming and (incoming/'control.json').exists() else {
        'mode':'fresh','network_retries':task.get('network_retries',0),'resets':task.get('resets',0),
        'stability_required':task.get('stability_required',False),'stability_report':task.get('stability_report',{}),'history':[]}
    if incoming and not (incoming/'control.json').exists():raise RuntimeError('Missing preceding phase control')
    if previous.get('mode')=='done':raise RuntimeError('Finished task was scheduled again')
    os.environ['SEGMENT']=str(number)
    folder=out/'evidence'
    try:
        previous.pop('not_before',None)
        if previous.get('stability_required'):
            stability.watch(task,previous['stability_report'],out/'stability')
            previous.pop('stability_required',None);previous.pop('stability_report',None)
        if expired():raise RuntimeError('Daily deadline reached')
        seed=None
        if previous['mode']=='resume':
            seed=json.loads(gzip.decompress((incoming/'checkpoint.json.gz').read_bytes()))
        report=asyncio.run(execute(task,folder,seed))
    except Exception as exc:
        report=failures(task,f'{type(exc).__name__}: {exc}')
    checkpoint=folder/'checkpoint.json.gz'
    if checkpoint.exists():(out/'checkpoint.json.gz').write_bytes(checkpoint.read_bytes())
    write(out/'report.json',report)
    previous['checkpoint_available']=checkpoint.exists();control=decide(report,previous,task)
    control['history']=previous['history']+[report]
    if number>=task['max_segments'] or expired():control['mode']='done'
    write(out/'control.json',control)
    output(again=control['mode']!='done',artifact=f'state-{task["key"]}-p{number:03d}')

def finish(incoming,out,task):
    out.mkdir(parents=True,exist_ok=True)
    if not (incoming/'control.json').exists():
        report=failures(task,'Missing runner artifact');control={'mode':'done','history':[report]}
    else:control=read(incoming/'control.json');report=read(incoming/'report.json')
    report['runner_history']=control['history'];report['network_retries']=control.get('network_retries',0)
    report['resets']=control.get('resets',0);report['resumable']=False
    write(out/'report.json',report)
    if report['complete']:
        (out/'checkpoint.json.gz').write_bytes((incoming/'checkpoint.json.gz').read_bytes())

def plan(task,out,reset=None):
    out.mkdir(parents=True,exist_ok=True)
    collector=None
    try:
        if reset:stability.watch(task,read(reset),out/'stability')
        elif task.get('stability_required'):stability.watch(task,task.get('stability_report',{}),out/'stability')
        if expired():raise RuntimeError('Daily deadline reached')
        base.validate=lambda payload,pos,job,total=None:validate_response(payload,pos,task,total)
        collector=PlanCollector(task,out/'evidence');asyncio.run(collector.run())
        if collector.status!='complete_class_observation':raise RuntimeError(collector.error)
        result=collector.plan
        for shard in result['shards']:
            shard['key']+=('-a2' if reset else '-a1')
            for field in ('stability_required','stability_report','not_before'):shard.pop(field,None)
        result['planner_report']=collector.report()
        for p in ('start','end'):result['planner_report']['environment_'+p]=read(collector.out/f'environment-{p}.json')
        result['ok']=True
    except Exception as exc:
        report=collector.report() if collector is not None else failures(task,f'{type(exc).__name__}: {exc}')
        report['complete']=False;report['raw_segment_audit_passed']=False
        report['snapshot_changed']=stability.suspect(report.get('error'))
        if collector is not None:
            good=sorted({x['position'] for x in collector.requests if x.get('status')==200})
            report['probe_positions']=sorted({1,good[len(good)//2] if good else 1,good[-1] if good else 1})
        for phase in ('start','end'):
            path=out/'evidence'/f'environment-{phase}.json'
            if path.exists():report['environment_'+phase]=read(path)
        result={'task':task,'ok':False,'shards':[],'report':report}
    write(out/'plan.json',result);output(ok=result['ok'],matrix=json.dumps(result['shards'],separators=(',',':')))

def validate_competition_ranks(rows):
    # RANK() semantics: a tie group shares its first physical ordinal;
    # the next group starts after every member. This catches missing ties.
    index=0
    while index<len(rows):
        rank=rows[index]['rank']
        if rank!=index+1:raise RuntimeError('Competition rank coverage gap; incomplete world')
        end=index+1
        while end<len(rows) and rows[end]['rank']==rank:end+=1
        if len({r['legionLevel'] for r in rows[index:end]})!=1:raise RuntimeError('Inconsistent tied levels')
        index=end

def validate_shards(plan,packets):
    if len(packets)!=len(plan['shards']):raise RuntimeError('Missing shard')
    rows=[];histories=[]
    for task,(packet,report) in zip(plan['shards'],packets):
        if report['task']!=task or not report.get('complete') or report.get('raw_segment_audit_passed') is not True:raise RuntimeError('Incomplete shard')
        part=packet['rows']
        if packet['task']!=task or packet['rows_sha256']!=old.digest(part) or packet['total_count']!=plan['total_count']:raise RuntimeError('Shard scope/hash/count mismatch')
        if any(not task['start_rank']<=r['rank']<=task['end_rank'] or r['legionLevel']<4000 or r['worldID']!=task['id'] for r in part):raise RuntimeError('Shard rank gap/range mismatch')
        rows.extend(part);histories.extend(report['runner_history'])
    validate_competition_ranks(rows)
    if len(rows)!=plan['boundary'] or len({base.key(r) for r in rows})!=len(rows):raise RuntimeError('Merged rank coverage/identity mismatch')
    if any(a['legionLevel']<b['legionLevel'] for a,b in zip(rows,rows[1:])):raise RuntimeError('ordering changed across shards')
    for rank,row in plan['anchors'].items():
        if int(rank)<=plan['boundary']:
            match=[r for r in rows if base.key(r)==base.key(row)]
            if match!=[row]:raise RuntimeError('snapshot changed across planner and shards')
    return rows,histories

class VerifyCollector(old.TaskCollector):
    def __init__(self,plan,out):super().__init__(plan['task'],out);self.plan=plan;self.total=plan['total_count']
    async def collect_legion(self):
        for rank,expected in self.plan['anchors'].items():
            payload=await self.fetch(max(1,int(rank)-4),'world_final_anchor')
            found=[old.compact(r) for r in payload['ranks'] if base.key(r)==base.key(expected)]
            if len({old.digest(r) for r in found})!=1:raise RuntimeError('Missing world identity anchor; data anomaly')
            if found[0]!=expected:raise RuntimeError('snapshot changed: world final anchor')
        self.status='complete_class_observation'

def merge_world(planpath,incoming,out,attempt):
    out.mkdir(parents=True,exist_ok=True);p=read(planpath);task=p['task'];reports=[]
    r=p.get('report') or failures(task,'Incomplete shard collection');rows=[]
    try:
        if not p['ok']:raise RuntimeError(r['error'])
        packets=[]
        for shard in p['shards']:
            folder=incoming/('cohort-'+shard['key'])
            sr=read(folder/'report.json') if (folder/'report.json').exists() else failures(shard,'Missing shard artifact')
            reports.append(sr)
            if sr.get('complete'):
                packet=json.loads(gzip.decompress((folder/'checkpoint.json.gz').read_bytes()));packets.append((packet,sr))
        failures_in_shards=[sr for sr in reports if not sr.get('complete')]
        if failures_in_shards:raise RuntimeError('; '.join(sr.get('error') or 'Incomplete shard' for sr in failures_in_shards))
        rows,history=validate_shards(p,packets)
        if expired():raise RuntimeError('Daily deadline before world final validation')
        base.validate=lambda payload,pos,job,total=None:validate_response(payload,pos,task,total)
        verify=VerifyCollector(p,out/'evidence');asyncio.run(verify.run())
        if verify.status!='complete_class_observation':raise RuntimeError(verify.error)
        r=verify.report();r.update(complete=True,raw_segment_audit_passed=True,rows=len(rows),rows_sha256=old.digest(rows),
                                  runner_history=[p['planner_report']]+history+[verify.report()],legion_min_level=4000)
        for phase in ('start','end'):r['environment_'+phase]=read(verify.out/f'environment-{phase}.json')
        (out/'checkpoint.json.gz').write_bytes(old.packed({'task':task,'rows':rows,'rows_sha256':old.digest(rows),'total_count':p['total_count'],'complete':True}))
    except Exception as exc:
        r=failures(task,f'{type(exc).__name__}: {exc}')
        r['snapshot_changed']=stability.suspect(r['error']) or any(x.get('snapshot_changed') for x in reports)
        r['runner_history']=[p.get('planner_report',p.get('report',{}))]+[h for sr in reports for h in sr.get('runner_history',[sr])]
        refused=any(code in ('401','403') for sr in reports for h in sr.get('runner_history',[sr]) for code in h.get('statuses',{}))
        r['reset_required']=bool(r['snapshot_changed'] and attempt==1 and task.get('resets',0)<1 and not refused)
        r['probe_positions']=p.get('report',{}).get('probe_positions') or sorted({max(1,int(rank)-4) for rank in p.get('anchors',{'1':{}})})
        r['reset_policy']='bounded_anchor_stability'
    history=r.get('runner_history',[])
    counts=Counter()
    for h in history:counts.update(h.get('statuses',{}))
    if history:
        r['statuses']=dict(counts);r['sent_requests']=sum(h.get('sent_requests',0) for h in history)
    write(out/'report.json',r);output(reset=r.get('reset_required',False))

def export(root,tasks):
    reports=[];entries=[]
    with zipfile.ZipFile('daily-compact.zip','w',compression=zipfile.ZIP_DEFLATED) as z:
        for task in tasks:
            folder=root/('cohort-'+task['key'])
            report=read(folder/'report.json') if (folder/'report.json').exists() else failures(task,'Missing final cohort artifact')
            if report['task']!=task:raise RuntimeError('Unexpected final task scope')
            reports.append(report)
            if not report.get('complete'):continue
            if report.get('raw_segment_audit_passed') is not True:raise RuntimeError('Unaudited completed cohort')
            packet=json.loads(gzip.decompress((folder/'checkpoint.json.gz').read_bytes()))
            if packet['task']!=task or packet['rows_sha256']!=old.digest(packet['rows']) or len(packet['rows'])!=report['rows']:raise RuntimeError('Final checkpoint checksum mismatch')
            rows=[r for r in packet['rows'] if r['level']>=275];data=old.packed(rows);name=task['key']+'.json.gz';z.writestr(name,data)
            entries.append({'task':task,'file':name,'sha256':hashlib.sha256(data).hexdigest(),'row_count':len(rows),
                            'scanned_rows':len(packet['rows']),'complete':True,'observed_at':report['updated_at_utc']})
        manifest={'schema_version':1,'region':'na','run_id':os.environ['GITHUB_RUN_ID'],'request_id':os.environ['REQUEST_ID'],
                  'day':os.environ['SNAPSHOT_DAY'],'segment':1,'requested_tasks':tasks,'entries':entries,'reports':reports,'next_tasks':[]}
        z.writestr('manifest.json',json.dumps(manifest,separators=(',',':')))
    print(json.dumps({'completed':len(entries),'requested':len(tasks)}),flush=True)

def main():
    p=argparse.ArgumentParser();p.add_argument('action',choices=['phase','finish','select','plan','merge','export','prepare']);p.add_argument('--phase',type=int);p.add_argument('--attempt',type=int,default=1)
    args=p.parse_args();task=json.loads(os.getenv('TASK','{}'))
    if args.action=='phase':phase(task,args.phase,Path('incoming') if args.phase>1 or os.getenv('RESTORE_SEED')=='true' else None,Path('result'))
    elif args.action=='finish':finish(Path('incoming'),Path('cohort'),task)
    elif args.action=='select':
        results=json.loads(os.environ['NEEDS_JSON']);values=[v['outputs'].get('artifact') for name in sorted(results) for v in [results[name]] if v['outputs'].get('artifact')]
        output(artifact=values[-1] if values else 'missing')
    elif args.action=='plan':plan(task,Path('planning'),Path('reset/report.json') if os.getenv('RESET')=='true' else None)
    elif args.action=='merge':merge_world(Path('planning/plan.json'),Path('collected'),Path('cohort'),args.attempt)
    elif args.action=='export':export(Path('collected'),json.loads(os.environ['TASKS']))
    elif args.action=='prepare':
        tasks=json.loads(os.environ['TASKS'])
        if os.getenv('KIND')=='job':
            if not tasks or any(t['type']!='job' for t in tasks):raise RuntimeError('Invalid job tasks')
            output(matrix=json.dumps(tasks,separators=(',',':')))
        else:
            byworld={t['id']:t for t in tasks}
            if len(byworld)!=len(tasks) or any(t['type']!='legion' or t['id'] not in ORDER for t in tasks):raise RuntimeError('Invalid Legion tasks')
            for w in ORDER:output(**{f'world{w}':json.dumps(byworld.get(w,{}),separators=(',',':')),f'has{w}':w in byworld})

if __name__=='__main__':main()
