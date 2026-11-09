B=$(cat base_$1.txt)
share() { awk -v pat="$2" -v label="$1" '{n=$NF; t+=n; if ($0 ~ pat) m+=n} END{printf "  %-50s %5d  %5.1f%%\n", label, m, 100*m/t}' $B.collapsed; }
echo "== $1: total CPU samples $(awk '{t+=$NF} END{print t}' $B.collapsed)"
echo " coordinator (parse + convert + route):"
share "Arrow->ESCF conversion (ArrowToEscf)"          'ArrowToEscf'
share "Arrow IPC parse excl. conversion (parse total)" 'ArrowIncrementalParser\.parse'
share "JSON bulk parse (BulkRequestParser)"            'BulkRequestParser'
share "JSON->ESCF encode (BulkBatchEncoders/EscfEncoder)" 'BulkBatchEncoders|EscfEncoder|SourceBatchEncodeHelper'
share "routing (BatchModeRouter/tsid/IndexRouting)"    'BatchModeRouter|ColumnarTsidCalculator|IndexRouting'
share "scatter (EscfBatchScatterer)"                   'EscfBatchScatterer'
share "HTTP receive (netty)"                           'netty'
echo " primary / replica shard:"
share "mapColumnBatch (columnar mapping)"              'mapColumnBatch'
share "applyIndexOperationBatchOnPrimary (all)"        'applyIndexOperationBatchOnPrimary'
share "per-doc fallback (DocumentParser / InternalEngine.index)" 'DocumentParser\.parseDocument|InternalEngine\.index\b'
share "translog write"                                 'Translog|TranslogWriter'
share "ESCF serialization (EscfBatchCodec/data())"     'EscfBatchCodec'
share "Lucene indexing (IndexingChain)"                'IndexingChain'
echo " JVM:"
share "GC (G1)"                                        'G1'
share "JIT compiler threads"                           'CompileBroker|C2Compiler|C1Compiler|Compile::'
