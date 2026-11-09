import sys, subprocess, time, statistics, threading, http.client, base64, json
sys.path.insert(0,'.'); import bench
AUTH="Basic "+base64.b64encode(b"elastic-admin:elastic-password").decode()
def setting(fork):
    bench.req("PUT","/_cluster/settings",json.dumps({"persistent":{"arrow.bulk.fork_parsing":fork}}).encode())
def one(file, index):
    out=subprocess.run(["curl","-s","-o","/dev/null","-w","%{http_code}","-u","elastic-admin:elastic-password","-XPOST",f"{bench.ES}/_arrow/{index}/_bulk","-H","Content-Type: application/vnd.apache.arrow.stream","-H","Transfer-Encoding: chunked","--data-binary","@"+file],capture_output=True,text=True).stdout
    assert out=="200",out
def throughput():
    # (label, file, shards, streams, fresh index for every request: documents with ids)
    configs=[("1 stream, 10k-row batches","b10k.arrow",1,1,False),("1 stream, 1k-row batches","b1k.arrow",1,1,False),
             ("1 stream, 50k-row batches","b50k.arrow",1,1,False),("4 streams, 10k-row batches","b10k.arrow",1,4,False),
             ("1 stream WITH _id, 10k-row batches","b10k_ids.arrow",1,1,True)]
    from concurrent.futures import ThreadPoolExecutor
    res={(c[0],m):[] for c in configs for m in ("forked","inline")}
    for fork in (True,False):               # warm-up of both paths
        setting(fork)
        for c in configs:
            bench.create("ab",shards=c[2]); one(c[1],"ab")
    for p in range(3):
        for c in configs:
            for fork in ((True,False) if p%2==0 else (False,True)):   # alternate the order
                setting(fork); label=c[0]
                bench.create("ab",shards=c[2])
                for _ in range(5):
                    if c[4]: bench.create("ab",shards=c[2])
                    t=time.perf_counter()
                    with ThreadPoolExecutor(c[3]) as ex: list(ex.map(lambda _: one(c[1],"ab"), range(c[3])))
                    res[(label,"forked" if fork else "inline")].append(c[3]*bench.ROWS/(time.perf_counter()-t))
    print(f"{'throughput (rows/s)':<42}{'forked':>10}{'inline':>10}{'forked/inline':>15}")
    for c in configs:
        a=statistics.median(res[(c[0],'forked')]); b=statistics.median(res[(c[0],'inline')])
        print(f"{c[0]:<42}{a/1000:>9.0f}k{b/1000:>9.0f}k{a/b:>14.2f}x")
def latency(fork, seconds=10, probes=28, streams=2):
    setting(fork); bench.create("lat"); stop=time.time()+seconds; lat=[]; lock=threading.Lock()
    def stream():
        while time.time()<stop: one("b50k.arrow","lat")
    def probe():
        c=http.client.HTTPConnection("localhost",9200); mine=[]
        while time.time()<stop:
            t=time.perf_counter(); c.request("GET","/",headers={"Authorization":AUTH}); c.getresponse().read(); mine.append((time.perf_counter()-t)*1000); time.sleep(0.002)
        with lock: lat.extend(mine)
    ths=[threading.Thread(target=stream) for _ in range(streams)]+[threading.Thread(target=probe) for _ in range(probes)]
    [t.start() for t in ths]; [t.join() for t in ths]
    lat.sort(); q=lambda p: lat[min(len(lat)-1,int(len(lat)*p))]
    return len(lat), q(0.5), q(0.99), q(0.999), lat[-1]
def latencies():
    print(f"\\nlatency of GET / on {28} connections while {2} streams of 50k-row batches run (ms)")
    print(f"{'mode':<10}{'requests':>10}{'p50':>8}{'p99':>8}{'p99.9':>8}{'max':>8}")
    for rnd in range(2):
        for fork in ((True,False) if rnd%2==0 else (False,True)):
            n,p50,p99,p999,mx=latency(fork)
            print(f"{'forked' if fork else 'inline':<10}{n:>10}{p50:>8.1f}{p99:>8.1f}{p999:>8.1f}{mx:>8.1f}", flush=True)
if __name__=="__main__":
    throughput(); latencies()
    setting(None) if False else bench.req("PUT","/_cluster/settings",json.dumps({"persistent":{"arrow.bulk.fork_parsing":None}}).encode())
    bench.req("DELETE","/ab"); bench.req("DELETE","/lat")
