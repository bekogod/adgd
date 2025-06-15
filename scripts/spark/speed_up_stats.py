import pandas as pd

df = pd.read_csv("speed_up.csv", sep=';')

# Calcula média do tempo agrupado pelo número de nós
# mean_times = df.groupby('nodes_number')['time'].mean().reset_index()
stats = df.groupby('nodes_number')['time'].agg(['mean', 'std', 'min', 'max', 'count']).reset_index()

print(stats)