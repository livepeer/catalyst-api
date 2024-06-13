#!/bin/bash

# pod-mon (aka pod-monitor) is used to log all system level metrics. This script is called
# periodically via catalyst-api so that resource usage is logged "in-line" with catalyst-api logs.
# This should make debug usage issues in relation to what catalyst-api is currently doing.

loglevel=1 # 0=debug, 1=info
script=$(basename "$0")
pidfile="/var/run/${script}"

# lock so that multiple instances of this script cannot run in parallel
exec 200>"$pidfile"
flock -n 200 || exit 1
pid=$$
echo $pid 1>&200

log () {
  local msg="$1"
  local level="$2"
  if [ -z "$level" ] || [ "$level" -ge "$loglevel" ] ; then
    echo "$msg" | sed 's/^/[pod-mon] /' 2>&1
  fi
}

# List of process names to monitor
processes=("MistController" "MistProcLivepeer" "MistUtilLoad" "MistOutWebRTC" "MistInDTSC" "MistOutDTSC" "MistOutFLV" "MistInFLV" "MistInBuffer" "MistOutHTTPTS" "catalyst-api" "catalyst-uploader")

# Function to calculate CPU and MEM usage for a specific process
calculate_usage() {
  local process_name=$1

  # Get the output of ps aux filtered by the process name, skipping the header row and capturing only the CPU and MEM columns
  ps aux | grep "$process_name" | grep -v grep | awk '{
    cpu+=$3; mem+=$4; count+=1; if ($3 > max_cpu) max_cpu=$3
  } END {
    print cpu, mem, count, max_cpu
  }'
}

# Get the number of CPU cores
num_cores=$(grep -c ^processor /proc/cpuinfo)

# Initialize the output as an empty string
output="["

# Loop through each process and calculate the usage
for process in "${processes[@]}"; do
  output_data=$(calculate_usage "$process")

  cpu=$(echo "$output_data" | awk '{print $1}')
  mem=$(echo "$output_data" | awk '{print $2}')
  count=$(echo "$output_data" | awk '{print $3}')
  max_cpu=$(echo "$output_data" | awk '{print $4}')

  # Set defaults if count is not a number
  if ! [[ "$count" =~ ^[0-9]+$ ]]; then
    count=0
  fi

  if [ "$count" -gt 0 ]; then
    avg_cpu_per_core=$(awk "BEGIN {print $cpu / $num_cores}")
    avg_mem=$(awk "BEGIN {print $mem / $count}")
  else
    avg_cpu_per_core=0
    avg_mem=0
    max_cpu=0
  fi

  # Append the JSON-like string to the output string
  output+="{\"process\": \"$process\", \"avg_cpu_per_core\": $avg_cpu_per_core, \"max_cpu_per_core\": $max_cpu, \"avg_mem_per_process\": $avg_mem}, "
done

# Remove the trailing comma and space, then close the JSON array
output=${output%, }
output+="]"

# Print the final JSON string
log "$output"
