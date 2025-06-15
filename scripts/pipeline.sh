#!/bin/bash

# Submeter o cluster do Spark
echo "Iniciar o Cluster..."
start_job_id=$(sbatch --parsable ./spark/start-spark.sh)
echo "Cluster submetido com ID: $start_job_id"

# Aguardar até que o cluster esteja alocado
echo "Aguardar que o cluster esteja ativo..."
while true; do
    job_state=$(squeue -j "$start_job_id" -h -o "%T")
    if [[ "$job_state" == "RUNNING" ]]; then
        echo "Cluster ativo."
        break
    fi
    sleep 5
done

# Obter o primeiro nó alocado ao cluster
first_node=$(scontrol show job "$start_job_id" | awk '/BatchHost=/{print $1}' | cut -d= -f2)
echo "Primeiro nó do cluster: $first_node"

# Submeter o job da query
echo "Submeter run-spark.sh no nó $first_node..."
run_job_id=$(sbatch --parsable ./spark/run-spark.sh "$first_node" "spark_querys.py" "spark")
echo "Job run-spark.sh submetido com ID: $run_job_id"

# Aguardar a conclusão do job 
echo "A aguardar a conclusão do job $run_job_id..."
while true; do
    job_state=$(scontrol show job "$run_job_id" 2>/dev/null | awk -F= '/JobState=/{print $2}' | cut -d' ' -f1)

    if [[ "$job_state" == "COMPLETED" ]]; then
        echo "Job $run_job_id concluído com sucesso."
        break
    elif [[ "$job_state" == "FAILED" || "$job_state" == "CANCELLED" || "$job_state" == "TIMEOUT" ]]; then
        echo "Job $run_job_id terminou com erro: $job_state"
        exit 1
    elif [[ -z "$job_state" ]]; then
        echo "Job $run_job_id já não encontrado pelo SLURM. Pode ter terminado ou expirado."
        break
    fi

    sleep 5
done
echo "Job $run_job_id concluído."

echo "A terminar o cluster (job $start_job_id)..."
scancel "$start_job_id"


# Executar o modelo
echo "Executar o model_tabnet.sh..."
model_job_id=$(sbatch --parsable ./pytorch/model_tabnet.sh "pipeline_model")

while true; do
    job_state=$(scontrol show job "$model_job_id" 2>/dev/null | awk -F= '/JobState=/{print $2}' | cut -d' ' -f1)

    if [[ "$job_state" == "COMPLETED" ]]; then
        echo "Job $model_job_id concluído com sucesso."
        break
    elif [[ "$job_state" == "FAILED" || "$job_state" == "CANCELLED" || "$job_state" == "TIMEOUT" ]]; then
        echo "Job $model_job_id terminou com erro: $job_state"
        exit 1
    elif [[ -z "$job_state" ]]; then
        echo "Job $model_job_id já não encontrado pelo SLURM. Pode ter terminado ou expirado."
        break
    fi

    sleep 5
done
