#!/bin/bash

echo "nodes_number;run_number;time;"

for file in speed_up/*; do
  time=$(grep "Time to process" "$file" | sed -E 's/.*Time to process all files: ([0-9.]+) seconds.*/\1/')
  if [ -n "$time" ]; then
    if [[ "$file" =~ speed_up_analizes_([0-9]+)_([0-9]+)\.log ]]; then
      nodes="${BASH_REMATCH[1]}"
      run="${BASH_REMATCH[2]}"
      echo "$nodes;$run;$time;"
    else
      echo "\"Formato do ficheiro desconhecido: $file\""
    fi
  fi
done
