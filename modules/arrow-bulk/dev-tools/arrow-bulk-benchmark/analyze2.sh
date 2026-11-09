B=$(cat base_$1.txt); WALL=$2
T=$(awk '{t+=$NF} END{print t}' $B.collapsed)
echo "== $1: $T CPU samples (~$(python3 -c "print(round($T*0.01/$WALL,1))") cores busy on average over ${WALL}s wall)"
s() { awk -v pat="$2" -v label="$1" '{n=$NF; t+=n; if ($0 ~ pat) m+=n} END{printf "  %-52s %5d  %5.1f%%\n", label, m, 100*m/t}' $B.collapsed; }
s "Arrow->ESCF conversion (ArrowToEscf)"            'ArrowToEscf'
s "Arrow IPC parse incl. conversion (parse total)"  'ArrowIncrementalParser\.parse'
s "IndexRequest/BulkRequest building (nextArrowBatch minus convert)" 'ArrowBulkParser\.nextArrowBatch'
s "routing (BatchModeRouter/tsid/IndexRouting)"     'BatchModeRouter|ColumnarTsidCalculator|IndexRouting'
s "scatter (EscfBatchScatterer)"                    'EscfBatchScatterer'
s "HTTP/netty"                                      'netty'
s "bulk coordination (TransportBulkAction etc.)"    'TransportBulkAction|BulkOperation|TransportAbstractBulkAction'
s "shard bulk (TransportShardBulkAction)"           'TransportShardBulkAction'
s "mapColumnBatch (columnar mapping)"               'mapColumnBatch'
s "Lucene IndexingChain"                            'IndexingChain'
s "translog (add/write)"                            'Translog\.add|TranslogWriter'
s "translog sync/fsync (force)"                     'FileChannel.*force|FileDispatcherImpl.force|TranslogWriter.*sync|AsyncIOProcessor'
s "EscfBatch.data()/serialize"                      'EscfBatchCodec|EscfBatch\.data'
s "GC (G1)"                                         'G1'
s "JIT compile threads"                             'CompileBroker|C2Compiler|C1Compiler'
