import pandas as pd
import os

def csv_to_parquet(input_folder: str, output_folder: str):
    """
    Converte todos os arquivos CSV em uma pasta para Parquet.
    
    :param input_folder: Caminho da pasta contendo arquivos CSV.
    :param output_folder: Caminho da pasta onde os arquivos Parquet serão salvos.
    """
    try:
        # Criar a pasta de saída se não existir
        os.makedirs(output_folder, exist_ok=True)
        
        # Listar todos os arquivos CSV na pasta de entrada
        for file in os.listdir(input_folder):
            if file.endswith(".csv"):
                csv_file = os.path.join(input_folder, file)
                output_file = os.path.join(output_folder, os.path.splitext(file)[0] + ".parquet")
                
                # Lendo o arquivo CSV
                df = pd.read_csv(csv_file)
                
                # Salvando como Parquet
                df.to_parquet(output_file, engine='pyarrow', index=False)
                print(f"Arquivo convertido com sucesso: {output_file}")
    except Exception as e:
        print(f"Erro ao converter CSV para Parquet: {e}")

# Exemplo de uso
csv_to_parquet("caminho_de_entrada_csv", "caminho_de_saida_parquet")
