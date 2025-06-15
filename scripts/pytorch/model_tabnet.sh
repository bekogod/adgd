#!/bin/sh
#SBATCH --job-name=model # Job name
#SBATCH --nodes=16 # Number of nodes
#SBATCH --ntasks-per-node=1 # Tasks per node
#SBATCH --output=ddp_model%j.out # Output file
#SBATCH --error=ddp_model%j.err # Error file
#SBATCH --account=F202500001HPCVLABEPICUREa # Account for resource allocation
#SBATCH --time=02:00:00 # Time limit
#SBATCH --partition=normal-arm # ARM partition
source /share/env/module_select.sh
ml Python/3.10.4-GCCcore-11.3.0
# Activate the virtual environment
source /projects/F202500001HPCVLABEPICURE/mca57510/adgd/env-spark/bin/activate
echo "SJN: $SLURM_JOB_NODELIST"
# Get the list of nodes
#nodes=($(scontrol -a show hostnames ${SLURM_JOB_NODELIST} | sort | uniq ))
nodes=($(scontrol -a show hostnames ${SLURM_JOB_NODELIST} | sort | uniq))
echo "Nodes: ${nodes[@]}"
# Get the head node IP address
head_node_ip=$(srun --nodes=1 --ntasks=1 hostname --ip-address | uniq)
echo "Head node IP: $head_node_ip"
# Set environment variables for distributed training
export MASTER_ADDR=$head_node_ip
export MASTER_PORT=29500
export WORLD_SIZE=$SLURM_NTASKS
export RANK=$SLURM_PROCID
export LOCAL_RANK=$SLURM_LOCALID
echo "MASTER_ADDR: $MASTER_ADDR"
echo "MASTER_PORT: $MASTER_PORT"
echo "SLURM_RANK: $SLURM_PROCID"
echo "SLURM_LOCALID: $SLURM_LOCALID"
export LOGLEVEL=INFO
export TORCH_CPP_LOG_LEVEL=INFO
export TORCH_DISTRIBUTED_DEBUG=INFO
# Train the model using distributed training
# srun torchrun --nnodes=4 --nproc_per_node=1 --rdzv_id $RANDOM --rdzv_backend=c10d --rdzv_endpoint=$MASTER_ADDR:$MASTER_PORT --local_addr=$MASTER_ADDR --rdzv_conf=is_host=$(if ((SLURM_NODEID)); then echo -1; else echo 1; fi) model.py &

# Train the model using distributed training with multiline command
srun torchrun \
  --nnodes=16 \
  --nproc_per_node=1 \
  --rdzv_id=$RANDOM \
  --rdzv_backend=c10d \
  --rdzv_endpoint=$MASTER_ADDR:$MASTER_PORT \
  --local_addr=$MASTER_ADDR --rdzv_conf=is_host=$(if ((SLURM_NODEID)); then echo -1; else echo 1; fi) \
    teste_tabnet.py \
    --data_path /projects/F202500001HPCVLABEPICURE/mca57510/adgd/DATA/speedupcheck_32.parquet \
    --target state_index \
    --continuous_cols cpu_hours,elapsed,@queue_wait,ntasks,time_limit,total_cpus,total_nodes,year,month,day,day_of_week,hour,minute,second,severity_index,exit_code_index,qos_index \
    --categorical_cols host,partition \
    --epochs 5 \
    --batch_size 4096 \
    --num_classes 6 \
    --run_name "$1" \
    --output_dir ./modelo_tabnet_8_1 &


wait
