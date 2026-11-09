#!/bin/bash
cd "$(dirname "$0")"
exec java --add-opens=java.base/java.nio=ALL-UNNAMED -Darrow.allocation.manager.type=Unsafe -cp ".:$(cat cp.txt)" Gen "$@" 2>&1 | grep -v "SLF4J\|WARNING"
