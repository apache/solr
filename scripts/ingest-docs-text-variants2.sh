#!/bin/bash
#
# /*
#  * Licensed to the Apache Software Foundation (ASF) under one or more
#  * contributor license agreements.  See the NOTICE file distributed with
#  * this work for additional information regarding copyright ownership.
#  * The ASF licenses this file to You under the Apache License, Version 2.0
#  * (the "License"); you may not use this file except in compliance with
#  * the License.  You may obtain a copy of the License at
#  *
#  *     http://www.apache.org/licenses/LICENSE-2.0
#  *
#  * Unless required by applicable law or agreed to in writing, software
#  * distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.
#  */
#

# =============================================================================
# Script to ingest documents with text field variants into a Solr collection
#
# Each document contains:
#   - 5 text values shared across 3 field types: text1_s, text1_txt_en, text1_txt_sort (through text5_*)
#   - 10 additional large filler _txt_en fields: filler1_txt_en through filler10_txt_en
#   - Total: 25 text fields per document (15 shared + 10 filler)
#   - Shared text sizes: uniform random between MIN-MAX (under 32KB DV limit)
#   - Filler text sizes: uniform random between MIN_TXT_EN-MAX_TXT_EN (no DV limit, can be large)
#
# Usage:
#   ./ingest-docs-text-variants.sh [SOLR_URL] [COLLECTION] [START_ID] [NUM_THREADS] \
#       [TOTAL_DOCS] [BATCH_SIZE] [MIN_TEXT_SIZE] [MAX_TEXT_SIZE] [MIN_TXT_EN_SIZE] [MAX_TXT_EN_SIZE]
#
# Parameters:
#   SOLR_URL        - Solr base URL (default: http://localhost:8983/solr)
#   COLLECTION      - Collection name (default: test)
#   START_ID        - Starting document ID (default: 0)
#   NUM_THREADS     - Number of parallel threads (default: 10)
#   TOTAL_DOCS      - Total documents to ingest (default: 10000000)
#   BATCH_SIZE      - Documents per batch (default: 100)
#   MIN_TEXT_SIZE   - Min text size for shared fields (default: 1024)
#   MAX_TEXT_SIZE   - Max text size for shared fields (default: 30720, under 32KB DV limit)
#   MIN_TXT_EN_SIZE - Min text size for filler _txt_en fields (default: 1024)
#   MAX_TXT_EN_SIZE - Max text size for filler _txt_en fields (default: 102400, ~100KB, no DV limit)
# =============================================================================

set -e

# Ensure output is unbuffered and locale handles binary input safely
export BASH_XTRACEFD=2
export LC_ALL=C

SOLR_URL="${1:-http://localhost:8983/solr}"
COLLECTION="${2:-test}"
START_ID="${3:-0}"
NUM_THREADS="${4:-10}"
TOTAL_DOCS="${5:-10000000}"
# Reduced batch size due to larger document sizes (up to ~450KB per doc)
BATCH_SIZE="${6:-100}"

# Text size range for _s and _txt_sort fields: 1KB to 30KB (under 32KB docValue limit)
MIN_TEXT_SIZE="${7:-1024}"
MAX_TEXT_SIZE="${8:-30720}"

# Text size range for _txt_en fields: 1KB to 100KB (no docValue limit, stored only)
MIN_TXT_EN_SIZE="${9:-1024}"
MAX_TXT_EN_SIZE="${10:-102400}"

# Pre-generated block size (max of both maximums)
FIELD_SIZE=$((MAX_TEXT_SIZE > MAX_TXT_EN_SIZE ? MAX_TEXT_SIZE : MAX_TXT_EN_SIZE))

END_ID=$((START_ID + TOTAL_DOCS))
DOCS_PER_THREAD=$((TOTAL_DOCS / NUM_THREADS))

# Pre-calculate common values to avoid spawning 'date' per document
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

echo "Ingesting $TOTAL_DOCS documents into $SOLR_URL/$COLLECTION"
echo "Each document has 25 text fields:"
echo "  - 15 shared fields (5 values × 3 types: _s, _txt_en, _txt_sort)"
echo "  - 10 filler _txt_en fields (filler1_txt_en through filler10_txt_en)"
echo "Shared text sizes: ${MIN_TEXT_SIZE}-${MAX_TEXT_SIZE} bytes (under 32KB DV limit)"
echo "Filler text sizes: ${MIN_TXT_EN_SIZE}-${MAX_TXT_EN_SIZE} bytes (no DV limit)"
echo "Parallel threads: $NUM_THREADS"

generate_text() {
    local size=$1
    # Use redirection and /dev/urandom safely
    tr -dc 'a-zA-Z0-9 ' < /dev/urandom | head -c "$size"
}
export -f generate_text

# Generate random size between MIN and MAX (uniform distribution)
random_text_size() {
    echo $((MIN_TEXT_SIZE + RANDOM % (MAX_TEXT_SIZE - MIN_TEXT_SIZE + 1)))
}
export -f random_text_size

echo "Pre-generating random text blocks (30KB each for maximum coverage)..."
TEXT_BLOCK_1=$(generate_text $FIELD_SIZE)
echo "  Generated block 1"
TEXT_BLOCK_2=$(generate_text $FIELD_SIZE)
echo "  Generated block 2"
TEXT_BLOCK_3=$(generate_text $FIELD_SIZE)
echo "  Generated block 3"
TEXT_BLOCK_4=$(generate_text $FIELD_SIZE)
echo "  Generated block 4"
TEXT_BLOCK_5=$(generate_text $FIELD_SIZE)
echo "  Generated block 5"
echo "Text blocks ready."

# Function to create a single document JSON
# Generates 5 text values shared across all 3 field types (_s, _txt_en, _txt_sort)
# Plus 10 additional large filler _txt_en fields (no DV limit)
# Plus non-textual fields from original script
create_document() {
    local id=$1

    # Non-textual fields (from original script)
    local r_int=$((RANDOM % 5))
    local r_dec=$((RANDOM % 100))
    local views=$((RANDOM * RANDOM % 1000000))

    # Generate 5 text values shared across _s, _txt_en, and _txt_sort fields (under 32KB DV limit)
    local size1=$((MIN_TEXT_SIZE + RANDOM % (MAX_TEXT_SIZE - MIN_TEXT_SIZE)))
    local start1=$((RANDOM % (FIELD_SIZE - size1 + 1)))
    local text1="${TEXT_BLOCK_1:$start1:$size1}"

    local size2=$((MIN_TEXT_SIZE + RANDOM % (MAX_TEXT_SIZE - MIN_TEXT_SIZE)))
    local start2=$((RANDOM % (FIELD_SIZE - size2 + 1)))
    local text2="${TEXT_BLOCK_2:$start2:$size2}"

    local size3=$((MIN_TEXT_SIZE + RANDOM % (MAX_TEXT_SIZE - MIN_TEXT_SIZE)))
    local start3=$((RANDOM % (FIELD_SIZE - size3 + 1)))
    local text3="${TEXT_BLOCK_3:$start3:$size3}"

    local size4=$((MIN_TEXT_SIZE + RANDOM % (MAX_TEXT_SIZE - MIN_TEXT_SIZE)))
    local start4=$((RANDOM % (FIELD_SIZE - size4 + 1)))
    local text4="${TEXT_BLOCK_4:$start4:$size4}"

    local size5=$((MIN_TEXT_SIZE + RANDOM % (MAX_TEXT_SIZE - MIN_TEXT_SIZE)))
    local start5=$((RANDOM % (FIELD_SIZE - size5 + 1)))
    local text5="${TEXT_BLOCK_5:$start5:$size5}"

    # Generate 10 large filler _txt_en fields (no docValues, can be much larger)
    local filler_size1=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start1=$((RANDOM % (FIELD_SIZE - filler_size1 + 1)))
    local filler1="${TEXT_BLOCK_1:$filler_start1:$filler_size1}"

    local filler_size2=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start2=$((RANDOM % (FIELD_SIZE - filler_size2 + 1)))
    local filler2="${TEXT_BLOCK_2:$filler_start2:$filler_size2}"

    local filler_size3=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start3=$((RANDOM % (FIELD_SIZE - filler_size3 + 1)))
    local filler3="${TEXT_BLOCK_3:$filler_start3:$filler_size3}"

    local filler_size4=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start4=$((RANDOM % (FIELD_SIZE - filler_size4 + 1)))
    local filler4="${TEXT_BLOCK_4:$filler_start4:$filler_size4}"

    local filler_size5=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start5=$((RANDOM % (FIELD_SIZE - filler_size5 + 1)))
    local filler5="${TEXT_BLOCK_5:$filler_start5:$filler_size5}"

    local filler_size6=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start6=$((RANDOM % (FIELD_SIZE - filler_size6 + 1)))
    local filler6="${TEXT_BLOCK_1:$filler_start6:$filler_size6}"

    local filler_size7=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start7=$((RANDOM % (FIELD_SIZE - filler_size7 + 1)))
    local filler7="${TEXT_BLOCK_2:$filler_start7:$filler_size7}"

    local filler_size8=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start8=$((RANDOM % (FIELD_SIZE - filler_size8 + 1)))
    local filler8="${TEXT_BLOCK_3:$filler_start8:$filler_size8}"

    local filler_size9=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start9=$((RANDOM % (FIELD_SIZE - filler_size9 + 1)))
    local filler9="${TEXT_BLOCK_4:$filler_start9:$filler_size9}"

    local filler_size10=$((MIN_TXT_EN_SIZE + RANDOM % (MAX_TXT_EN_SIZE - MIN_TXT_EN_SIZE)))
    local filler_start10=$((RANDOM % (FIELD_SIZE - filler_size10 + 1)))
    local filler10="${TEXT_BLOCK_5:$filler_start10:$filler_size10}"

    # Output document JSON
    # All 3 field types (_s, _txt_en, _txt_sort) share the same text values
    # Plus 10 large filler _txt_en fields
    echo "{
  \"id\": \"doc_$id\",
  \"text1_s\": \"$text1\",
  \"text1_txt_en\": \"$text1\",
  \"text1_txt_sort\": \"$text1\",
  \"text2_s\": \"$text2\",
  \"text2_txt_en\": \"$text2\",
  \"text2_txt_sort\": \"$text2\",
  \"text3_s\": \"$text3\",
  \"text3_txt_en\": \"$text3\",
  \"text3_txt_sort\": \"$text3\",
  \"text4_s\": \"$text4\",
  \"text4_txt_en\": \"$text4\",
  \"text4_txt_sort\": \"$text4\",
  \"text5_s\": \"$text5\",
  \"text5_txt_en\": \"$text5\",
  \"text5_txt_sort\": \"$text5\",
  \"filler1_txt_en\": \"$filler1\",
  \"filler2_txt_en\": \"$filler2\",
  \"filler3_txt_en\": \"$filler3\",
  \"filler4_txt_en\": \"$filler4\",
  \"filler5_txt_en\": \"$filler5\",
  \"filler6_txt_en\": \"$filler6\",
  \"filler7_txt_en\": \"$filler7\",
  \"filler8_txt_en\": \"$filler8\",
  \"filler9_txt_en\": \"$filler9\",
  \"filler10_txt_en\": \"$filler10\",
  \"category_s\": \"category_$((id % 100))\",
  \"author_s\": \"author_$((id % 500))\",
  \"tags_ss\": [\"tag_$((id % 50))\", \"tag_$((id % 30))\", \"tag_$((id % 20))\"],
  \"created_dt\": \"$TIMESTAMP\",
  \"views_i\": $views,
  \"rating_f\": $r_int.$r_dec
}"
}

export -f create_document

ingest_range() {
    local thread_id=$1
    local range_start=$2
    local range_end=$3
    local thread_docs=$((range_end - range_start))
    local current_id=$range_start
    local batch_num=0
    local docs_ingested=0
    local thread_start_time=$(date +%s)

    echo "[Thread $thread_id] Starting: $range_start to $((range_end - 1))"

    while [ $current_id -lt $range_end ]; do
        # Generate fresh random blocks for this batch to ensure higher entropy
        local TEXT_BLOCK_1=$(generate_text $FIELD_SIZE)
        local TEXT_BLOCK_2=$(generate_text $FIELD_SIZE)
        local TEXT_BLOCK_3=$(generate_text $FIELD_SIZE)
        local TEXT_BLOCK_4=$(generate_text $FIELD_SIZE)
        local TEXT_BLOCK_5=$(generate_text $FIELD_SIZE)

        batch_num=$((batch_num + 1))
        batch_start=$current_id
        batch_end=$((current_id + BATCH_SIZE))

        if [ $batch_end -gt $range_end ]; then
            batch_end=$range_end
        fi

        # Write batch JSON directly to temp file
        temp_file=$(mktemp)
        printf '[' > "$temp_file"

        # Loop optimization: minimize checks inside loop
        create_document $batch_start >> "$temp_file"
        for ((i=batch_start+1; i<batch_end; i++)); do
            printf ',' >> "$temp_file"
            create_document $i >> "$temp_file"
        done

        printf ']' >> "$temp_file"

        # Send batch to Solr (added timeouts, increased max-time for larger payloads)
        response=$(curl -s -S --connect-timeout 10 --max-time 600 -w "\n%{http_code}" -X POST \
            "$SOLR_URL/$COLLECTION/update?commit=false" \
            -H "Content-Type: application/json" \
            -d @"$temp_file")

        rm -f "$temp_file"

        http_code=$(echo "$response" | tail -n1)

        if [ "$http_code" != "200" ]; then
            echo "[Thread $thread_id] Error: HTTP $http_code on batch $batch_num"
            # Print error response safely
            echo "$response" | head -n -1
        fi

        current_id=$batch_end
        docs_ingested=$((current_id - range_start))

        # Progress update every 10 batches (reduced from 20 due to larger docs)
        if [ $((batch_num % 10)) -eq 0 ]; then
            elapsed=$(($(date +%s) - thread_start_time))
            if [ $elapsed -gt 0 ]; then
                rate=$((docs_ingested / elapsed))
                pct=$((docs_ingested * 100 / thread_docs))
                echo "[Thread $thread_id] Progress: $docs_ingested / $thread_docs ($pct%) - $rate docs/sec"
            fi
        fi
    done

    echo "[Thread $thread_id] COMPLETED."
}

export -f ingest_range
export SOLR_URL COLLECTION BATCH_SIZE TIMESTAMP TEXT_BLOCK_1 TEXT_BLOCK_2 TEXT_BLOCK_3 TEXT_BLOCK_4 TEXT_BLOCK_5
export MIN_TEXT_SIZE MAX_TEXT_SIZE MIN_TXT_EN_SIZE MAX_TXT_EN_SIZE FIELD_SIZE

start_time=$(date +%s)
pids=()

echo "Starting $NUM_THREADS parallel threads..."

for ((t=0; t<NUM_THREADS; t++)); do
    thread_start=$((START_ID + t * DOCS_PER_THREAD))
    thread_end=$((thread_start + DOCS_PER_THREAD))

    if [ $t -eq $((NUM_THREADS - 1)) ]; then
        thread_end=$END_ID
    fi

    ingest_range $t $thread_start $thread_end 2>&1 &
    pids+=($!)
done

echo "Waiting for threads..."
failed=0
for pid in "${pids[@]}"; do
    if ! wait $pid; then
        failed=1
    fi
done

if [ $failed -eq 1 ]; then
    echo "One or more threads failed!"
    exit 1
fi

echo "Committing..."
curl -s --connect-timeout 10 -X POST "$SOLR_URL/$COLLECTION/update?commit=true" -H "Content-Type: application/json" -d '{}' > /dev/null

total_time=$(($(date +%s) - start_time))
echo "Ingestion complete in ${total_time}s"

