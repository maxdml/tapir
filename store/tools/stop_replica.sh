#!/bin/bash

config=$1   # path to config file

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 configpath" >&2
  exit 1
fi

for n in `cat $config | grep replica | awk '{print $2}'`
do
  server=$(echo $n | cut -d':' -f1)
  command="ssh -p 2324 -i /home/maxdml/.ssh/nostromo $server \"pkill -TERM server; rm ~/*bin\""
  #echo $command
  eval $command
done
