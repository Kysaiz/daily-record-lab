"""Small optional URL sidecars; never part of ranking snapshot comparisons."""
import json,sqlite3,logging
from pathlib import Path
from datetime import datetime,timedelta,timezone
from urllib.parse import urlsplit

def valid_url(url):
    if not isinstance(url,str) or len(url)>8192:return False
    p=urlsplit(url)
    return p.scheme=='https' and bool(p.hostname) and p.hostname.endswith('.nexon.net') and not p.username and not p.password

def attach_targets(tasks,data,db):
    path=Path(data)/'activity.sqlite3'
    if not path.exists():return tasks
    active=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=5)
    try:
        cutoff=(datetime.now(timezone.utc)-timedelta(days=90)).isoformat()
        people=active.execute('SELECT name_key,world,name FROM active_characters WHERE last_queried_at>=?',(cutoff,)).fetchall()
    finally:active.close()
    grouped={}
    for key,world,name in people:
        row=db.execute('SELECT job FROM latest WHERE name_key=? AND world=?',(key,world)).fetchone()
        if row:grouped.setdefault(row[0],[]).append([key,world])
    result=[dict(t,avatar_targets=grouped[t['id']]) if t['type']=='job' and grouped.get(t['id']) else t for t in tasks]
    if len(json.dumps(result,separators=(',',':')).encode())>55000:
        logging.getLogger('daily').warning('Active avatar target list exceeds dispatch budget; URL fallback retained today')
        return tasks
    return result

def captured(rows,task,at):
    allowed={(n.casefold(),int(w)) for n,w in task.get('avatar_targets',[])}
    return [dict(name=r['characterName'],world=r['worldID'],url=r['characterImgURL'],observed_at=at)
        for r in rows if (r['characterName'].casefold(),r['worldID']) in allowed and r.get('level',0)>=275 and valid_url(r.get('characterImgURL'))]

def report_urls(report):
    urls={}
    for part in report.get('runner_history',[report]):
        for row in part.get('avatar_urls',[]):
            key=(row['name'].casefold(),row['world'])
            if key not in urls or row['observed_at']>urls[key]['observed_at']:urls[key]=row
    return list(urls.values())

def ingest(report,data,day):
    if report.get('task',{}).get('type')!='job' or not report.get('complete') or not report.get('raw_segment_audit_passed'):return 0
    allowed={(n.casefold(),int(w)) for n,w in report['task'].get('avatar_targets',[])}
    rows=report_urls(report)
    if not rows:return 0
    from activity import connection,BJT
    count=0
    with connection(data) as db:
        for r in rows:
            key=(r['name'].casefold(),r['world'])
            if key not in allowed or not valid_url(r.get('url')):raise ValueError('Unexpected active avatar URL sidecar')
            at=datetime.fromisoformat(r['observed_at'])
            if at.tzinfo is None or at.astimezone(BJT).date().isoformat()!=day:continue
            cursor=db.execute('''UPDATE active_characters SET last_url=?,url_observed_at=?
                WHERE name_key=? AND world=? AND (url_observed_at IS NULL OR url_observed_at<?)''',
                (r['url'],at.isoformat(),*key,at.isoformat()))
            count+=cursor.rowcount
    db.close()
    return count
