"""Bounded fixed-anchor watch. Equality is evidence of observed stability only."""
import asyncio,json,os,time
from datetime import datetime,timezone
from pathlib import Path

INTERVAL=600
ROUNDS=3
MAX_ROUNDS=12
MAX_REQUESTS=48

def suspect(error):
    # Duplicate/gap/missing/order anomalies alone are not update evidence.
    return bool(error) and any(x in error.lower() for x in ('snapshot changed','totalcount changed','boundary changed'))

def canonical(payload):
    fields=('characterName','worldID','jobName','level','exp','rank','gap','legionLevel','raidPower')
    rows=[];seen={}
    for r in payload['ranks']:
        value={k:r[k] for k in fields};key=(r['characterName'].casefold(),r['worldID'])
        if key in seen:
            if seen[key]!=value:raise RuntimeError('Conflicting duplicate in stability response')
            continue
        seen[key]=value;rows.append(value)
    return {'total_count':payload['totalCount'],'rows':rows}

def anchors(report,task):
    positions=report.get('probe_positions')
    if not positions:
        count=max(1,int(report.get('rows',0)))
        positions=[1,max(1,count//2-4),max(1,count-4),count+1]
    positions=sorted(set(int(p) for p in positions))
    if len(positions)>4 or not positions or min(positions)<1:raise RuntimeError('Invalid bounded anchor plan')
    return positions

async def observe(task,positions,out):
    # Fresh connection, no automatic HTTP or transport retry, no identity change.
    import aiohttp
    out.mkdir(parents=True,exist_ok=True);rows=[]
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30),cookie_jar=aiohttp.DummyCookieJar(),headers={'User-Agent':'GMSRankingResearch/6.0','Accept':'application/json'}) as session:
        session._retry_connection=False
        for position in positions:
            if datetime.now(timezone.utc)>=datetime.fromisoformat(os.environ['DEADLINE']):raise RuntimeError('Daily deadline reached during stability watch')
            async with session.get('https://www.nexon.com/api/maplestory/no-auth/ranking/v2/na',params={'type':task['type'],'id':task['id'],'reboot_index':0,'page_index':position},allow_redirects=False) as response:
                raw=await response.read()
                (out/f'{position}.json').write_bytes(raw)
                if response.status!=200:raise RuntimeError(f'HTTP {response.status}; stability watch stopped; Retry-After={response.headers.get("Retry-After")}')
                payload=json.loads(raw)
                if payload.get('pageIndex')!=position or type(payload.get('totalCount')) is not int or not isinstance(payload.get('ranks'),list):raise RuntimeError('Invalid stability response')
                expected=max(0,min(10,payload['totalCount']-position+1))
                if len(payload['ranks'])!=expected:raise RuntimeError('Unexpected empty/short stability response')
                if any((r['worldID']!=task['id'] if task['type']=='legion' else r['jobName']!=task['id']) for r in payload['ranks']):raise RuntimeError('Wrong stability scope')
                rows.append(canonical(payload))
            await asyncio.sleep(1)
    if not rows or any(r['total_count']<=0 for r in rows):raise RuntimeError('Unexpected zero population during stability watch')
    return rows

def watch(task,report,out,fetch=observe,sleep=time.sleep,now=lambda:datetime.now(timezone.utc)):
    out.mkdir(parents=True,exist_ok=True)
    positions=anchors(report,task);history=[];previous=None;streak=0;requests=0
    for round in range(1,MAX_ROUNDS+1):
        if now()>=datetime.fromisoformat(os.environ['DEADLINE']):raise RuntimeError('Daily deadline before stability confirmed')
        if requests+len(positions)>MAX_REQUESTS:raise RuntimeError('Stability request budget exhausted')
        if round>1:
            due=now().timestamp()+INTERVAL
            if due>=datetime.fromisoformat(os.environ['DEADLINE']).timestamp():raise RuntimeError('Daily deadline before next stability round')
            print(f'Stability watch: next round in {INTERVAL}s; identical rounds={streak}/{ROUNDS}',flush=True)
            while now().timestamp()<due:sleep(min(30,due-now().timestamp()))
        requests+=len(positions)
        try:current=asyncio.run(fetch(task,positions,out/f'round-{round:02d}'))
        except Exception as exc:
            history.append({'round':round,'error':str(exc),'at_utc':now().isoformat()})
            (out/'watch.json').write_text(json.dumps({'positions':positions,'history':history,'stable':False,'requests_reserved':requests},indent=2))
            raise
        coherent=len({x['total_count'] for x in current})==1
        streak=(streak+1 if current==previous else 1) if coherent else 0
        previous=current if coherent else None
        history.append({'round':round,'at_utc':now().isoformat(),'signature':current,'identical_rounds':streak})
        result={'positions':positions,'history':history,'stable':streak>=ROUNDS,'requests_reserved':requests,'interval_seconds':INTERVAL}
        (out/'watch.json').write_text(json.dumps(result,indent=2))
        print(f'Stability round {round}: identical rounds={streak}/{ROUNDS}; anchor requests={requests}',flush=True)
        if result['stable']:return result
    raise RuntimeError('Stability round budget exhausted; no full restart')
