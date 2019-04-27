#!/bin/bash

shard=$1    # which shard is this
config=$2   # path to config file
cmd=$3      # command to run
logdir=$4   # log directory

if [ "$#" -ne 4 ]; then
  echo "Usage: $0 shard configpath command logdir" >&2
  exit 1
fi

i=0
for n in `cat $config | grep replica | awk '{print $2}'`
do
  server=$(echo $n | cut -d':' -f1)
  command="ssh -p 2324 -i /home/maxdml/.ssh/nostromo $server \"$cmd -c $config -i $i > $logdir/$shard.replica$i.log 2>&1 &\""
  eval $command
  let i=$i+1
done
