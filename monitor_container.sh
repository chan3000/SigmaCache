#!/bin/bash

# --- CONFIGURATION ---
if [ -z "$1" ]; then
    echo "Usage: ./monitor_container.sh <container_name_or_id> [output_file]"
    echo "  container_name_or_id: Name or ID of container to monitor"
    echo "  output_file: Optional custom log filename (default: auto-generated)"
    exit 1
fi

CONTAINER_NAME=$1

# Use custom filename if provided, otherwise generate one
if [ -z "$2" ]; then
    TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
    OUTPUT_FILE="${CONTAINER_NAME}_full_metrics_${TIMESTAMP}.log"
else
    OUTPUT_FILE="$2"
fi

# --- STEP 1: FIND ALL PIDS (PARENT + CHILDREN) ---
echo "--- Mapping PIDs for container: $CONTAINER_NAME ---" >&2

# 1. Get the Parent PID (The Entrypoint)
PARENT_PID=$(docker inspect -f '{{.State.Pid}}' "$CONTAINER_NAME" 2>/dev/null)

if [ -z "$PARENT_PID" ] || [ "$PARENT_PID" == "0" ]; then
    echo "Error: Container not found or not running." >&2
    exit 1
fi

# 2. Get all Child PIDs (The Workers)
# We use pgrep -P to find children of the parent
CHILD_PIDS=$(pgrep -P "$PARENT_PID" | tr '\n' ',')

# 3. Combine them into one comma-separated list
ALL_PIDS="${PARENT_PID},${CHILD_PIDS%,}"

echo "Monitoring Parent PID: $PARENT_PID" >&2
echo "Monitoring Child PIDs: ${CHILD_PIDS%,}" >&2
echo "Combined Target List: $ALL_PIDS" >&2
echo "Logging to: $OUTPUT_FILE" >&2
echo "---------------------------------------------------" >&2

# --- STEP 2: SETUP SIGNAL HANDLER ---
# This ensures pidstat stops cleanly when script receives SIGTERM
PIDSTAT_PID=""

cleanup() {
    echo "Received termination signal, stopping pidstat..." >&2
    if [ ! -z "$PIDSTAT_PID" ]; then
        kill $PIDSTAT_PID 2>/dev/null
        wait $PIDSTAT_PID 2>/dev/null
    fi
    echo "Monitor stopped." >&2
    exit 0
}

trap cleanup SIGTERM SIGINT

# --- STEP 3: START PIDSTAT IN BACKGROUND ---
# -p $ALL_PIDS : Monitors every process in the list
# -u, -r, -d   : CPU, Memory, Disk I/O
# -h           : Horizontal format
# 1            : 1-second interval
LC_NUMERIC=C pidstat -p "$ALL_PIDS" -u -r -d -h 1 > "$OUTPUT_FILE" 2>&1 &

PIDSTAT_PID=$!

echo "Monitor PID: $PIDSTAT_PID" >&2
echo "Ready. Monitoring in background..." >&2

# --- STEP 4: KEEP SCRIPT ALIVE ---
# This is critical! The script must stay alive or pidstat dies
# We wait for pidstat to exit (either naturally or via our cleanup handler)
wait $PIDSTAT_PID

echo "pidstat exited." >&2