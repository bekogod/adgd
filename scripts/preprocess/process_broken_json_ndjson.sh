#!/bin/bash
#SBATCH --job-name=p-logs      # create a short name for your job
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=02:00:00          # total run time limit (HH:MM:SS)
#SBATCH --account=F202500001HPCVLABEPICUREa
#SBATCH --partition=normal-arm
#SBATCH --output=p_logs_%j.out
#SBATCH --error=process_logs_test.err   # std err



input_file="$1"
day=$(echo "$input_file" | grep -oE '[0-9]{4}\.[0-9]{2}\.[0-9]{2}')

temp_file="PRJ-Cleaned-NoArray/tmp-cleaned-${day}.json"
output_file="PRJ-Cleaned-NoArray/logstash-${day}.json"

# Registar hora inicial
start_time=$(date +%s)

echo "Ficheiro: $input_file"

tac "$input_file" | awk '
    BEGIN {found = 0}
    {
        gsub(/\[\]/, ",");  # Replace empty brackets with commas
        if (!found) {
            if ($0 ~ /^[[:space:]]*},[[:space:]]*$/) {
                found = 1
                print "}"   # Close the last valid JSON object
            }
        }
        if (found) {
            print
        }
    }
' | tac  > "$temp_file"

echo "]" >> "$temp_file"

rm $temp_file
jq -c '.[]' "$temp_file" > "$output_file"

rm $temp_file

# Registar hora final
end_time=$(date +%s)

# Calcular o tempo total
execution_time=$((end_time - start_time))

echo "Guardado em: $output_file, demorou $execution_time segundos"