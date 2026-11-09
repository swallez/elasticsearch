import sys, time, statistics
sys.path.insert(0,'.'); import bench
def go(label, file, shards, streams=1, durability="request", refresh="1s"):
    bench.create("sw", shards=shards, durability=durability, refresh=refresh)
    bench.line(label, bench.run(file, "sw", streams, rounds=4))
print("=== number of shards (1 stream, b10k)")
for sh in (1,2,4,8): go(f"{sh} shard(s)", "b10k.arrow", sh)
print("=== 4 shards, other conditions (1 stream)")
go("4 shards, translog async", "b10k.arrow", 4, durability="async")
go("1 shard,  translog async", "b10k.arrow", 1, durability="async")
go("4 shards, record batch 50k", "b50k.arrow", 4)
go("1 shard,  record batch 50k", "b50k.arrow", 1)
go("4 shards, refresh disabled", "b10k.arrow", 4, refresh="-1")
print("=== several streams")
go("4 streams, 1 shard", "b10k.arrow", 1, streams=4)
go("4 streams, 4 shards", "b10k.arrow", 4, streams=4)
bench.req("DELETE","/sw")
