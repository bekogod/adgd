import argparse
import pandas as pd
from pytorch_tabular import TabularModel
from pytorch_tabular.config import DataConfig, TrainerConfig, OptimizerConfig, ExperimentConfig
from pytorch_tabular.models import TabNetModelConfig

def main(args):
    print(f"A carregar: {args.data_path}")
    df = pd.read_parquet(args.data_path)
    print(f"Dados carregados com {df.shape[0]} linhas e {df.shape[1]} colunas.")

    target = args.target
    continuous_cols = args.continuous_cols.split(",")
    categorical_cols = args.categorical_cols.split(",") if args.categorical_cols else []

    print(f"Target: {target}")
    print(f"Colunas contínuas: {continuous_cols}")
    print(f"Colunas categóricas: {categorical_cols}")

    data_config = DataConfig(
        target=[target],
        continuous_cols=continuous_cols,
        categorical_cols=categorical_cols,
    )
    

    trainer_config = TrainerConfig(
        max_epochs=args.epochs,
        batch_size=args.batch_size
    )
    
    optimizer_config = OptimizerConfig()

    model_config = TabNetModelConfig(
        task="classification",
        metrics=["accuracy","confusionmatrix"],
        learning_rate=1e-3
    )
    
    ## defaults do tabnet
    # n_d=8,
    # n_a=8,
    # n_steps=3,
    # head="LinearHead",  # Linear Head
    # head_config=head_config,  # Linear Head Config
    
    EXP_PROJECT_NAME = "pytorch-tabular-tabnet"

    experiment_config = ExperimentConfig(
        project_name=EXP_PROJECT_NAME,
        run_name=args.run_name,
        exp_watch="all",
        log_target="tensorboard",
    )

    tabular_model = TabularModel(
        data_config=data_config,
        model_config=model_config,
        optimizer_config=optimizer_config,
        trainer_config=trainer_config,
        experiment_config=experiment_config,
        verbose=True
        
    )
    

    print("Iniciar o treino...")
    tabular_model.fit(train=df)
    
    print("Treino concluído!")

    if args.output_dir:
        tabular_model.save_model(args.output_dir)
        print(f"Modelo salvo em: {args.output_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_path", type=str, required=True, help="Caminho para o ficheiro Parquet")
    parser.add_argument("--target", type=str, default="state_index")
    parser.add_argument("--continuous_cols", type=str, required=True,
                        help="Colunas contínuas separadas por vírgula")
    parser.add_argument("--categorical_cols", type=str, default="partition",
                        help="Colunas categóricas separadas por vírgula")
    parser.add_argument("--epochs", type=int, default=10)
    parser.add_argument("--batch_size", type=int, default=512)
    parser.add_argument("--num_classes", type=int, default=6)
    parser.add_argument("--run_name", type=str, default="Default-run")
    parser.add_argument("--output_dir", type=str, default=None, help="Diretório para salvar o modelo")
    
    args = parser.parse_args()

    main(args)
