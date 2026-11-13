"""单份目录备份包的封装及完整性校验。"""
import tarfile, io, json, hashlib
from .objects import encode

def create_archive(store, digests, metadata, path):
    unique=sorted(set(digests));index=dict(metadata,objects=unique)
    entries={'index.json':encode(index)}
    entries.update({'objects/'+d:store.get(d) for d in unique})
    with tarfile.open(path,'w') as archive:
        for name,raw in entries.items():
            info=tarfile.TarInfo(name);info.size=len(raw);info.mtime=0;info.mode=0o600
            archive.addfile(info,io.BytesIO(raw))
    return index

def read_archive(path):
    with tarfile.open(path,'r:') as archive:
        names=archive.getnames()
        if len(set(names))!=len(names):raise ValueError('备份包路径重复')
        for member in archive.getmembers():
            if not member.isfile() or (member.name!='index.json' and not member.name.startswith('objects/')):
                raise ValueError('备份包成员非法')
        index=json.loads(archive.extractfile('index.json').read());result={}
        if set(names)!={'index.json',*('objects/'+d for d in index['objects'])}:raise ValueError('包清单不符')
        for digest in index['objects']:
            raw=archive.extractfile('objects/'+digest).read()
            if hashlib.sha256(raw).hexdigest()!=digest:raise ValueError('备份对象损坏')
            result[digest]=raw
    return index,result

def inspect(data,store,root):
    from .snapshot import Snapshots
    s=Snapshots(store);heads={name:s.create(value['rows'],value['schema']) for name,value in data['tables'].items()}
    baseline=data['baseline'];selected={name:heads[version] for name,version in baseline['table_heads'].items()}
    digests=[]
    for head in selected.values():digests.extend([head,store.json(head)['data']])
    path=root/'full.tar'
    create_archive(store,digests,dict(baseline,table_heads=selected),path)
    index,objects=read_archive(path)
    return {'backup_id':index['backup_id'],'base_commit':index['base_commit'],'objects':len(objects),'table_heads':index['table_heads']}
