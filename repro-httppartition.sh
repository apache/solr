#!/bin/zsh
cd /Users/markmiller/solr-ref
OUT=/Users/markmiller/solr-ref/.repro-logs/httppartition
mkdir -p "$OUT"
GW=/Users/markmiller/solr-ref/gradlew
XML=solr/core/build/test-results/test/TEST-org.apache.solr.cloud.HttpPartitionTest.xml
PASS=0; FAIL=0
for i in $(seq 1 6); do
  SEED=$(od -An -N8 -tx1 /dev/urandom | tr -d " \n" | tr "a-f" "A-F")
  echo "=== iter $i seed=$SEED ===" >> "$OUT/summary.txt"
  "$GW" -p /Users/markmiller/solr-ref :solr:core:test --tests "org.apache.solr.cloud.HttpPartitionTest" \
     -Ptests.nightly=true -Ptests.seed=$SEED --no-daemon > "$OUT/run-$i.console.log" 2>&1
  if [ -f "$XML" ]; then
    line=$(grep -o 'tests="[0-9]*" skipped="[0-9]*" failures="[0-9]*" errors="[0-9]*"' "$XML" | head -1)
    echo "  result: $line" >> "$OUT/summary.txt"
    if echo "$line" | grep -q 'failures="0" errors="0"'; then
      PASS=$((PASS+1))
    else
      FAIL=$((FAIL+1))
      mkdir -p "$OUT/fail-$i-$SEED"
      cp "$XML" "$OUT/fail-$i-$SEED/" 2>/dev/null
      cp solr/core/build/test-results/test/outputs/OUTPUT-*.txt "$OUT/fail-$i-$SEED/" 2>/dev/null
    fi
  else
    echo "  result: NO XML (build error?)" >> "$OUT/summary.txt"
    FAIL=$((FAIL+1))
  fi
done
echo "=== DONE pass=$PASS fail=$FAIL ===" >> "$OUT/summary.txt"
