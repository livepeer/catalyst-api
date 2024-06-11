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

# get cpu/mem usage
top=$(top -b -n 1 -H -o %CPU | head -n 15)
free=$(free -h)
# get disk usage
df=$(df -h)
tmpusage=$(du -ch --max-depth=1 /tmp | sort -hr)

log "System usage --------"
log "$top"
log "Mem usage --------"
log "$free"
log "Disk usage --------"
log "$df"
log "/tmp usage --------"
log "$tmpusage"
