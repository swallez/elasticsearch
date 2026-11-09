import sys, subprocess, time, statistics
sys.path.insert(0,'.'); import bench
def one(file, index, params=""):
    out=subprocess.run(["curl","-s","-o","/dev/null","-w","%{http_code}","-u","elastic-admin:elastic-password","-XPOST",f"{bench.ES}/_arrow/{index}/_bulk{params}","-H","Content-Type: application/vnd.apache.arrow.stream","-H","Transfer-Encoding: chunked","--data-binary","@"+file],capture_output=True,text=True).stdout
    assert out=="200",out
# (label, file, shards, params, fresh index for every request: needed when documents have ids)
configs=[("1 shard,  auto (new default)","b10k.arrow",1,"",False),
         ("2 shards, auto (new default)","b10k.arrow",2,"",False),
         ("2 shards, bulk_rows=10000 (old)","b10k.arrow",2,"?bulk_rows=10000",False),
         ("4 shards, auto (new default)","b10k.arrow",4,"",False),
         ("4 shards, bulk_rows=10000 (old)","b10k.arrow",4,"?bulk_rows=10000",False),
         ("8 shards, auto (new default)","b10k.arrow",8,"",False),
         ("8 shards, bulk_rows=10000 (old)","b10k.arrow",8,"?bulk_rows=10000",False),
         ("4 shards, WITH _id, auto (new default)","b10k_ids.arrow",4,"",True),
         ("4 shards, WITH _id, bulk_rows=10000 (old)","b10k_ids.arrow",4,"?bulk_rows=10000",True)]
res={c[0]:[] for c in configs}
for label,file,shards,params,fresh in configs:   # warm-up
    for _ in range(3):
        bench.create("ab",shards=shards); one(file,"ab",params)
for p in range(3):
    for label,file,shards,params,fresh in configs:
        bench.create("ab",shards=shards)
        for _ in range(6):
            if fresh: bench.create("ab",shards=shards)
            t=time.perf_counter(); one(file,"ab",params); res[label].append(bench.ROWS/(time.perf_counter()-t))
print(f"{'1 stream, 10k-row record batches':<46}{'median':>9}{'p25':>8}{'p75':>8}")
for label,v in res.items():
    q=statistics.quantiles(v,n=4)
    print(f"{label:<46}{statistics.median(v)/1000:>8.0f}k{q[0]/1000:>7.0f}k{q[2]/1000:>7.0f}k")
bench.req("DELETE","/ab")
