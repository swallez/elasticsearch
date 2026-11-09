import json, subprocess, sys, time, threading
sys.path.insert(0,'.'); import bench
pid=subprocess.run("jps | awk '/Elasticsearch/{print $1; exit}'",shell=True,capture_output=True,text=True).stdout.strip()
def create(name, mode):
    bench.req("DELETE","/"+name)
    body={"settings":{"index.number_of_shards":1,"index.number_of_replicas":0,"index.mode":mode,"index.batch_indexing":True},
          "mappings":{"properties":{"ints":{"type":"integer"},"strings":{"type":"text"}}}}
    bench.req("PUT","/"+name,json.dumps(body).encode())
def send(index, chunked):
    cmd=["curl","-s","-o","/dev/null","-w","%{http_code}","-m","120","-u","elastic-admin:elastic-password","-XPOST",f"{bench.ES}/_arrow/{index}/_bulk","-H","Content-Type: application/vnd.apache.arrow.stream"]
    if chunked: cmd+=["-H","Transfer-Encoding: chunked"]
    cmd+=["--data-binary","@big.arrow"]
    t=time.perf_counter(); out=subprocess.run(cmd,capture_output=True,text=True).stdout; return out, time.perf_counter()-t
for mode in ("columnar","standard"):
    for chunked in (False, True):
        for attempt in range(3):
            create("big",mode)
            dumped=[]
            def watchdog():
                time.sleep(5)
                f=f"/tmp/esdump_{mode}_{'chunked' if chunked else 'plain'}_{attempt}.txt"
                subprocess.run(f"jcmd {pid} Thread.print > {f} 2>&1",shell=True); dumped.append(f)
            th=threading.Thread(target=watchdog,daemon=True); th.start()
            code,t=send("big",chunked)
            print(f"{mode:9} {'chunked ' if chunked else 'plain   '} attempt {attempt}: HTTP {code} in {t:6.2f}s", flush=True)
bench.req("DELETE","/big")
