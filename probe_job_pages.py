"""Bounded 100-page Bishop integrity observation; --execute to send requests."""
import argparse
import hashlib
import http.client
import json
import time
import urllib.parse
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--execute',action='store_true')
args=parser.parse_args()
if not args.execute:
    print(json.dumps({'dry_run':True,'job':'Bishop','world_type':'both','pages':100,'requests_max':102,'rps':.4,'positions':[1,11,21,'...',991],'anchor_rechecks':[1,991]}))
    raise SystemExit(0)
root=Path(__file__).resolve().parent/'job-page-integrity'
root.mkdir(exist_ok=True)
if (root/'refusal.json').exists():
    print('Prior refusal recorded; no requests sent')
    raise SystemExit(2)
start=datetime.now(timezone.utc)
out=root/start.strftime('%Y%m%dT%H%M%SZ')
out.mkdir()
clock_start=time.monotonic()
deadline=clock_start+1200
connection=http.client.HTTPSConnection('www.nexon.com',timeout=25)
last_send=None
observations=[]
characters=[]
anchors={}
total=None
error=None
status='running'

def stamp(): return datetime.now(timezone.utc).isoformat()
def key(row): return (str(row.get('characterName','')).casefold(),row.get('worldID'))
def signature(payload):
    compact=[[r.get(k) for k in ('characterName','worldID','level','exp','rank')] for r in payload['ranks']]
    return hashlib.sha256(json.dumps([payload['totalCount'],compact],separators=(',',':')).encode()).hexdigest()

def fetch(position,phase):
    global last_send
    if len(observations)>=102 or time.monotonic()>=deadline:
        raise RuntimeError('Request/time budget reached')
    e={'position':position,'phase':phase,'started_at_utc':stamp(),'status':None}
    started=time.monotonic()
    try:
        reused=connection.sock is not None
        connect_start=time.monotonic()
        if not reused: connection.connect()
        e['connect_ms']=round((time.monotonic()-connect_start)*1000,3)
        e['remote_address']=list(connection.sock.getpeername())
        e['local_address']=list(connection.sock.getsockname())
        if last_send is not None: time.sleep(max(0,last_send+2.5-time.monotonic()))
        if time.monotonic()>=deadline: raise RuntimeError('Deadline before GET')
        now=time.monotonic()
        e.update(sent_at_utc=stamp(),send_monotonic_seconds=now,send_gap_ms=round((now-last_send)*1000,3) if last_send is not None else None,connection_reused=reused)
        last_send=now
        path='/api/maplestory/no-auth/ranking/v2/na?'+urllib.parse.urlencode({'type':'job','id':'Bishop','reboot_index':0,'page_index':position})
        connection.request('GET',path,headers={'User-Agent':'GMSRankingResearch/6.0','Accept':'application/json','Accept-Encoding':'identity'})
        response=connection.getresponse()
        e.update(status=response.status,retry_after=response.getheader('Retry-After'),server=response.getheader('Server'))
        body=response.read()
        e['body_bytes']=len(body)
        e['response_ms']=round((time.monotonic()-now)*1000,3)
        (out/f'{len(observations)+1:03d}-{phase}-{position}.json').write_bytes(body)
        if response.will_close: connection.close()
        if response.status in (401,403,429):
            (root/'refusal.json').write_text(json.dumps(e,indent=2),encoding='utf-8')
        if response.status!=200: raise RuntimeError(f'HTTP {response.status}; no retry')
        payload=json.loads(body)
        e['total_count']=payload.get('totalCount')
        e['row_count']=len(payload.get('ranks',[]))
        if e['row_count']!=10: raise RuntimeError('Empty/short page; integrity not established')
        return payload
    except Exception as exc:
        e['error']=f'{type(exc).__name__}: {exc}'
        raise
    finally:
        e['total_ms']=round((time.monotonic()-started)*1000,3)
        observations.append(e)
        with (out/'requests.jsonl').open('a',encoding='utf-8') as file:
            file.write(json.dumps(e)+'\n')

def report():
    counts=Counter(key(r) for r in characters)
    gaps=[e['send_gap_ms'] for e in observations if e.get('send_gap_ms') is not None]
    elapsed=time.monotonic()-clock_start
    result={'started_at_utc':start.isoformat(),'updated_at_utc':stamp(),'status':status,'error':error,
        'scope':'Bishop both worlds, first 100 non-overlapping pages; NOT full class or LV275+ full collection',
        'configured_rps':.4,'total_count':total,'requests':len(observations),'successful_scan_pages':sum(e['phase']=='scan' and e.get('status')==200 and not e.get('error') for e in observations),
        'rows':len(characters),'unique_characters':len(counts),'duplicate_rows':len(characters)-len(counts),
        'levels':{'min':min((r.get('level',0) for r in characters),default=None),'max':max((r.get('level',0) for r in characters),default=None)},
        'statuses':dict(Counter(str(e['status']) for e in observations)),
        'minimum_send_gap_ms':min(gaps,default=None),'elapsed_seconds':round(elapsed,3),
        'effective_unique_characters_per_second':round(len(counts)/elapsed,3) if elapsed else None,
        'new_connections':sum(not e.get('connection_reused',False) for e in observations)}
    (out/'report.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result

try:
    for index in range(100):
        position=1+index*10
        payload=fetch(position,'scan')
        if total is None: total=payload['totalCount']
        if payload['totalCount']!=total: raise RuntimeError('totalCount changed; stop')
        if position in (1,991): anchors[position]=signature(payload)
        batch=payload['ranks']
        if any(r.get('jobName')!='Bishop' for r in batch):
            raise RuntimeError('Unexpected class; filter not verified')
        if any(key(r) in {key(x) for x in characters} for r in batch):
            raise RuntimeError('Duplicate character across non-overlapping pages')
        characters.extend(batch)
        report()
        if (index+1)%20==0: print(f'{index+1}/100 pages, {len(characters)} rows',flush=True)
    for position in (1,991):
        payload=fetch(position,'anchor')
        if signature(payload)!=anchors[position]: raise RuntimeError('Anchor changed; no consistent snapshot')
    if len({key(r) for r in characters})!=1000: raise RuntimeError('Expected 1000 unique rows')
    status='complete_sample'
except Exception as exc:
    error=f'{type(exc).__name__}: {exc}'
    status='incomplete'
finally:
    connection.close()
    final=report()
    final['saved']=str(out)
    print(json.dumps(final),flush=True)
raise SystemExit(0 if status=='complete_sample' else 2)
