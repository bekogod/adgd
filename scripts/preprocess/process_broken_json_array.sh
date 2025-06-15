#!/bin/bash
#SBATCH --job-name=process-logs      # create a short name for your job
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=02:00:00          # total run time limit (HH:MM:SS)
#SBATCH --account=F202500001HPCVLABEPICUREa
#SBATCH --partition=normal-arm
#SBATCH --error=process_logs_test.err   # std err

# Loop para os dias 05 a 28 de fevereiro
# PROCESS_DAYS=("2025.03.09")

# for day in "${PROCESS_DAYS[@]}"; do
input_file="$1"
day=$(echo "$input_file" | grep -oE '[0-9]{4}\.[0-9]{2}\.[0-9]{2}')

output_file="PRJ-Cleaned-Array/logstash-${day}.json"
temp_file="PRJ-Cleaned-Array/tmp-cleaned-${day}.json"

# Registar hora inicial
start_time=$(date +%s)

echo "Corrigindo: $input_file"

# Etapa 1: Corrigir entradas mal concatenadas — ][ → ,
awk '{gsub(/\]\[/, ","); print}' "$input_file" > "$temp_file"

# Etapa 2: Remover a entrada JSON incompleta e fechar corretamente
tac "$temp_file" | awk '
    BEGIN {found = 0}
    !found {
        if ($0 ~ /^[[:space:]]*},[[:space:]]*$/) {
            found = 1
            print "}"   # fecha o último objecto JSON válido
            next
        } else {
            next
        }
    }
    found { print }
' | tac  > "$output_file"

echo "]" >> "$output_file"

# Registar hora final
end_time=$(date +%s)

# Calcular o tempo total
execution_time=$((end_time - start_time))

echo "Guardado em: $output_file, demorou $execution_time segundos"