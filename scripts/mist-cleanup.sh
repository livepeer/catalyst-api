#!/bin/bash

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
  if [ "$level" -ge "$loglevel" ]; then
    echo $$ - "$(date)" - "mist-cleanup" - "$msg" 2>&1
  fi
}

containsElement () {
  local e match="$1"
  shift
  for e; do [[ "$e" == "$match" ]] && return 0; done
  return 1
}

SEMS=$(ls -tr /dev/shm/sem.MstTRKS* /dev/shm/sem.MstUser* /dev/shm/sem.MstInpt* /dev/shm/sem.MstPull* 2> /dev/null)
PULS=$(ls -tr /dev/shm/sem.MstPull* 2> /dev/null)
#sem.MstPull_golive+Preview
PAGS=$(find /dev/shm/ -name "MstData*" -o -name "MstMeta*" -o -name "MstTrak*" 2> /dev/null)
STAT=$(ls -tr /dev/shm/MstSTATEgolive* 2> /dev/null)
ACTS=$(ps -C MistInBuffer -o command= | awk ' { print $3 } ' | sort | uniq)
DTPL=$(ps -C MistInDTSC -o command= | awk ' { print $3 } ' | sort | uniq)
DATA_DEL=0
STATS_DEL=0
SEMS_DEL=0

log "Starting..." 0

#Check for input semaphores without data
for S in $SEMS ; do
  STRM=${S:20}
  if containsElement "$STRM" "$ACTS" ; then
    log "Yes: ${STRM} (${S})" 0
  else
    log "SEM_INPUT: ${S}" 1
    rm "${S}"
    SEMS_DEL=$(( SEMS_DEL + 1 ))
  fi
done

#Check for pull semaphores without data
for S in $PULS ; do
  STRM=${S:21}
  if containsElement "$STRM" "$DTPL" ; then
    log "Yes: ${STRM} (${S})" 0
  else
    log "SEM_PULL: ${S}" 1
    rm "${S}"
    SEMS_DEL=$(( SEMS_DEL + 1 ))
  fi
done

#Check for data pages without buffer
for P in $PAGS; do
  FILE=${P%@*} #Remove everything after the '@'
  STRM=${FILE:16} #Strip the beginning
  if containsElement "$STRM" "$ACTS" ; then
    log "Yes: ${STRM} (${P})" 0
  else
    if (( $# != 0 )); then
      log "DATA: ${P}" 1
    fi
    rm "${P}"
    DATA_DEL=$(( DATA_DEL + 1 ))
  fi
done

#Check for state pages without buffer
for P in $STAT ; do
  STRM=${P:17} #Strip the beginning
  if containsElement "$STRM" "$ACTS" ; then
    log "Yes: ${STRM} (${P})" 0
  else
    log "STATE: ${P}" 1
    rm "${P}"
    STATS_DEL=$(( STATS_DEL + 1 ))
  fi
done

log "Done. Deleted ${DATA_DEL} data pages, ${SEMS_DEL} semaphores, ${STATS_DEL} state pages" 1
