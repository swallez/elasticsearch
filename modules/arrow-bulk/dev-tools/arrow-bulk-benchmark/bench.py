import json, subprocess, sys, time, statistics, urllib.request, base64
from concurrent.futures import ThreadPoolExecutor
ES="http://localhost:9200"; AUTH=base64.b64encode(b"elastic-admin:elastic-password").decode()
ROWS=200_000
def req(method, path, body=None):
    r=urllib.request.Request(ES+path, data=body, method=method, headers={"Authorization":"Basic "+AUTH,"Content-Type":"application/json"})
    try: return urllib.request.urlopen(r).read()
    except Exception as e: return str(e).encode()
def create(name, shards=1, mode="columnar", refresh="1s", durability="request"):
    req("DELETE","/"+name)
    props={"@timestamp":"date","host":"keyword","level":"keyword","service":"keyword","region":"keyword","message":"keyword","status":"integer","bytes":"long","duration_ms":"double","user_id":"long","success":"boolean","bucket":"integer"}
    body={"settings":{"index.number_of_shards":shards,"index.number_of_replicas":0,"index.mode":mode,"index.batch_indexing":True,"index.refresh_interval":refresh,"index.translog.durability":durability},
          "mappings":{"dynamic":"strict","properties":{k:{"type":v} for k,v in props.items()}}}
    req("PUT","/"+name,json.dumps(body).encode())
def stream(file, index):
    t=time.perf_counter()
    out=subprocess.run(["curl","-s","-o","/dev/null","-w","%{http_code}","-u","elastic-admin:elastic-password","-XPOST",f"{ES}/_arrow/{index}/_bulk","-H","Content-Type: application/vnd.apache.arrow.stream","-H","Transfer-Encoding: chunked","--data-binary","@"+file],capture_output=True,text=True).stdout
    assert out=="200", out
    return time.perf_counter()-t
def count(index):
    req("POST",f"/{index}/_refresh"); return json.loads(req("GET",f"/{index}/_count"))["count"]
def run(file, index, streams, rounds=3, fresh=False, shards=1):
    results=[]
    for r in range(rounds+1):          # first round is warm-up
        if fresh: create(index, shards=shards)
        t=time.perf_counter()
        with ThreadPoolExecutor(streams) as ex: list(ex.map(lambda _: stream(file,index), range(streams)))
        wall=time.perf_counter()-t
        if r>0: results.append(streams*ROWS/wall)
    req("DELETE","/"+index)
    return statistics.median(results), min(results), max(results)
def line(label, res): print(f"{label:<44} median {res[0]/1000:7.0f}k rows/s   (min {res[1]/1000:.0f}k, max {res[2]/1000:.0f}k)", flush=True)
def refresh_param():
    for label,params in (("no refresh param",""),("refresh=true","?refresh=true"),("refresh=wait_for","?refresh=wait_for")):
        create("rfp")
        def one():
            t=time.perf_counter()
            out=subprocess.run(["curl","-s","-o","/dev/null","-w","%{http_code}","-u","elastic-admin:elastic-password","-XPOST",f"{ES}/_arrow/rfp/_bulk{params}","-H","Content-Type: application/vnd.apache.arrow.stream","-H","Transfer-Encoding: chunked","--data-binary","@b10k.arrow"],capture_output=True,text=True).stdout
            assert out=="200",out
            return time.perf_counter()-t
        one(); ts=[one() for _ in range(3)]; m=statistics.median(ts)
        print(f"{label:<44} median {ROWS/m/1000:7.0f}k rows/s   ({m:.2f}s per 200k rows)", flush=True)
        req("DELETE","/rfp")

if __name__=="__main__":
    exp=sys.argv[1]
    if exp=="ids":
        for f in ("b10k_ids","b1k_ids"):
            create("idx"); line(f"1 stream WITH _id column, 1 shard, {f[:-4]}", run(f+".arrow","idx",1,fresh=True))
        line("1 stream WITH _id column, 4 shards, b10k_ids", run("b10k_ids.arrow","idx",1,fresh=True,shards=4))
    if exp=="refreshparam": refresh_param()
    if exp=="all":
        import subprocess as sp
        for e in ("batchsize","concurrency","shards","durability","refresh","ids","refreshparam"):
            print(f"=== {e}", flush=True); sp.run([sys.executable,"bench.py",e])
    if exp=="batchsize":
        for f in ("b1k","b10k","b50k"):
            create("bs_"+f); line(f"1 stream, 1 shard, record batch {f[1:]}", run(f+".arrow","bs_"+f,1))
    if exp=="concurrency":
        for n in (1,2,4,8):
            create("cc"); line(f"{n} streams, 1 shard, b10k", run("b10k.arrow","cc",n))
        for n in (1,4):
            create("cc"); line(f"{n} streams, 1 shard, b1k", run("b1k.arrow","cc",n))
    if exp=="shards":
        for sh in (1,4):
            for n in (1,4):
                create("sh",shards=sh); line(f"{n} streams, {sh} shard(s), b10k", run("b10k.arrow","sh",n))
    if exp=="durability":
        for d in ("request","async"):
            for f in ("b1k","b10k"):
                create("du",durability=d); line(f"1 stream, translog {d}, record batch {f[1:]}", run(f+".arrow","du",1))
    if exp=="refresh":
        for r in ("1s","-1"):
            create("rf",refresh=r); line(f"1 stream, refresh_interval {r}, b10k", run("b10k.arrow","rf",1))
