"""Restore only audited artifacts from the same day's interrupted collection."""
import gzip,json,os,sys
from pathlib import Path
import daily_collect_v131 as old
from collector_v143 import output,network_error

def restore(incoming,task):
    packet=json.loads(gzip.decompress((incoming/'checkpoint.json.gz').read_bytes()))
    report=json.loads((incoming/'report.json').read_text())
    if packet['task']!=task or report['task']!=task or packet['rows_sha256']!=old.digest(packet['rows']):raise RuntimeError('Seed scope/hash mismatch')
    if report.get('raw_segment_audit_passed') is not True or report.get('snapshot_changed') or not report.get('handoff_valid') or not (report.get('resumable') or network_error(report.get('error'))):raise RuntimeError('Seed is not an audited resumable prefix')
    prior=json.loads((incoming/'control.json').read_text())
    history=prior.get('history',[])
    control={'mode':'resume','network_retries':max(task.get('network_retries',0),prior.get('network_retries',0)),
             'resets':max(task.get('resets',0),prior.get('resets',0)),'history':history,
             'repair_snapshot_only':True}
    (incoming/'control.json').write_text(json.dumps(control))

def repair_plan(folder,task):
    plan=json.loads((folder/'plan.json').read_text())
    if not plan.get('ok') or plan['task']!=task:raise RuntimeError('Original plan scope mismatch')
    failed=[]
    for shard in plan['shards']:
        p=Path('collected')/('cohort-'+shard['key'])/'report.json';report=json.loads(p.read_text())
        if report['task']!=shard:raise RuntimeError('Shard scope mismatch')
        if not report.get('complete'):failed.append(shard)
    if len(failed)!=1:raise RuntimeError('Repair expects exactly one interrupted shard')
    shard=failed[0]
    expected={'legion45-s4-a1':'state-legion45-s4-a1-p007','legion70-s1-a1':'state-legion70-s1-a1-p001'}
    if shard['key'] not in expected:raise RuntimeError('Unexpected interrupted shard')
    output(shard=json.dumps(shard,separators=(',',':')),seed_artifact=expected[shard['key']])

def job_seeds(tasks,day):
    # Preserve the old explicit repair scope; add only this authorized Hero repair.
    legacy={'job066':'state-job066-p003','job074':''}
    if {t['key'] for t in tasks}==set(legacy):return legacy
    if day=='2026-10-31' and len(tasks)==1 and tasks[0]['key']=='job044' and tasks[0]['type']=='job' and tasks[0]['id']=='Hero':
        return {'job044':'state-job044-p003'}
    raise RuntimeError('Unexpected repair job list')


if __name__=='__main__':
    task=json.loads(os.environ['TASK'])
    if sys.argv[1]=='seed':restore(Path('incoming'),task)
    elif sys.argv[1]=='plan':repair_plan(Path('planning'),task)
    elif sys.argv[1]=='jobs':
        tasks=json.loads(os.environ['TASKS']);seeds=job_seeds(tasks,os.environ.get('SNAPSHOT_DAY',''))
        output(matrix=json.dumps([{'task':t,'seed_artifact':seeds[t['key']]} for t in tasks],separators=(',',':')))
