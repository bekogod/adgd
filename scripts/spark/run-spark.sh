#!/bin/bash
#SBATCH --job-name=SC-2
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=4
#SBATCH --time=24:00:00
#SBATCH --account=F202500001HPCVLABEPICUREa
#SBATCH --partition=normal-arm
#SBATCH --output=start_client.out
#SBATCH --error=start_client.err

echo "==== Job iniciado às $(date) ===="

echo "A carregar módulos..."
source /share/env/module_select.sh
module purge
module load "Java/17.0.6"
module load "Python/3.12.3-GCCcore-13.3.0"

export SPARK_HOME="/projects/F202500001HPCVLABEPICURE/mca57510/adgd/spark-3.5.5-bin-hadoop3"
export PATH="${SPARK_HOME}/bin:${PATH}"

echo "A ativar ambiente virtual..."
source /projects/F202500001HPCVLABEPICURE/mca57510/adgd/env-spark/bin/activate

master_node="$1"
spark_job="$2"
output_file="speed_up_repart/$3"
echo "Master node definido: ${master_node}"
echo "Output File is: ${output_file}"

echo "A iniciar o job Spark para ${spark_job}..."
$SPARK_HOME/bin/spark-submit \
  --master "spark://${master_node}:7077" \
  /projects/F202500001HPCVLABEPICURE/mca57510/adgd/${spark_job} > ${output_file}.log

echo "Job Spark Completo. Verifica o ficheiro ${output_file}.log para detalhes."
echo "==== Job terminado às $(date) ===="
