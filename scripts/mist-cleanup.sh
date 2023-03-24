#!/bin/bash -e

script=$(basename "$0")
pidfile="/var/run/${script}"

# lock so that multiple instances of this script cannot run in parallel
exec 200>"$pidfile"
flock -n 200 || exit 1
pid=$$
echo $pid 1>&200

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

#Check for input semaphores without data
for S in $SEMS ; do
  STRM=${S:20}
  if containsElement "$STRM" "$ACTS" ; then
    echo "Yes: ${STRM} (${S})" > /dev/null
  else
    echo "SEM_INPUT: ${S}"
    rm "${S}"
  fi
done

#Check for pull semaphores without data
for S in $PULS ; do
  STRM=${S:21}
  if containsElement "$STRM" "$DTPL" ; then
    echo "Yes: ${STRM} (${S})" > /dev/null
  else
    #Silenced, since this one is mostly harmless.
    #echo "SEM_PULL: ${S}"
    rm "${S}"
  fi
done

#Check for data pages without buffer
for P in $PAGS; do
  FILE=${P%@*} #Remove everything after the '@'
  STRM=${FILE:16} #Strip the beginning
  if containsElement "$STRM" "$ACTS" ; then
    echo "Yes: ${STRM} (${P})" > /dev/null
  else
    if (( $# != 0 )); then
      echo "DATA: ${P}"
    fi
    rm "${P}"
    DATA_DEL=$(( DATA_DEL + 1 ))
  fi
done

#Check for state pages without buffer
for P in $STAT ; do
  STRM=${P:17} #Strip the beginning
  if containsElement "$STRM" "$ACTS" ; then
    echo "Yes: ${STRM} (${P})" > /dev/null
  else
    echo "STATE: ${P}"
    rm "${P}"
  fi
done

echo "Deleted ${DATA_DEL} data pages"
