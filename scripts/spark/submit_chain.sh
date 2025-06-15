#!/bin/bash
#SBATCH --job-name=speed-up
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=48:00:00
#SBATCH --account=F202500001HPCVLABEPICUREa
#SBATCH --partition=normal-arm
#SBATCH --output=speed-up-%j.out
#SBATCH --error=speed-up-%j.err

master_node="$1"
spark_job="$2"

previous_jobid=""

for i in $(seq 0 19); do
    output_prefix="$3_$i"

    if [ -z "$previous_jobid" ]; then
        # Submete o primeiro job normalmente
        jobid=$(sbatch --parsable run-spark.sh "$master_node" "$spark_job" "$output_prefix")
    else
        # Submete o job com dependência no anterior
        jobid=$(sbatch --parsable --dependency=afterok:$previous_jobid run-spark.sh "$master_node" "$spark_job" "$output_prefix")
    fi

    echo "Submetido job $i com jobid=$jobid"
    previous_jobid=$jobid
done

echo "Jobs submetidos!!."