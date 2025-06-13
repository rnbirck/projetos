# %%
import os
import zipfile

ano = 2025
mes = 5
mes_formatado = f"{mes:02}"

DOWNLOAD_DIR = "../data/cnpj/"
directory_name = f"{ano}-{mes_formatado}/"
directory_path = os.path.join(DOWNLOAD_DIR, directory_name)

os.makedirs(directory_path, exist_ok=True)


# %%
def extrair_e_renomear_zip(caminho_zip, diretorio_destino):
    """
    Extrai o conteúdo de um ZIP e tenta renomear o PRIMEIRO item listado
    para o nome base do ZIP. Assume que tudo vai dar certo.
    """
    print(f"Processando: {os.path.basename(caminho_zip)}")
    nome_base_zip = os.path.splitext(os.path.basename(caminho_zip))[0]
    novo_nome_arquivo_csv = f"{nome_base_zip}.csv"
    novo_caminho_desejado = os.path.join(diretorio_destino, novo_nome_arquivo_csv)

    try:
        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:
            # Pega o nome do PRIMEIRO item da lista interna do ZIP
            nome_original_interno = zip_ref.namelist()[0]

            # Extrai tudo
            print("  Extraindo...")
            zip_ref.extractall(diretorio_destino)

        # Constrói o caminho original do item (baseado no primeiro nome da lista)
        caminho_original_extraido = os.path.join(
            diretorio_destino, nome_original_interno.strip("/")
        )

        # Renomeia
        print(
            f"  Renomeando '{nome_original_interno.strip('/')}' para '{nome_base_zip}.csv'..."
        )
        os.rename(caminho_original_extraido, novo_caminho_desejado)
        print("  Renomeado.")

    except Exception as e:
        # Captura qualquer erro (ZIP não encontrado, vazio, corrompido, erro ao renomear, etc.)
        print(f"  ERRO ao processar {os.path.basename(caminho_zip)}: {e}")


arquivo = range(0, 10)
for i in arquivo:
    nome_arquivo_zip = f"Estabelecimentos{i}.zip"

    caminho_zip_completo = os.path.join(directory_path, nome_arquivo_zip)

    # Verifica se o ARQUIVO ZIP específico existe
    if os.path.exists(caminho_zip_completo):
        print(f"Processando {nome_arquivo_zip}...")
        # Chama a função passando o CAMINHO COMPLETO DO ZIP e o diretório de destino
        extrair_e_renomear_zip(caminho_zip_completo, directory_path)

    else:
        print(f"Arquivo {nome_arquivo_zip} não encontrado em {directory_path}")

nome_simples_zip = "Simples.zip"
caminho_simples_zip = os.path.join(directory_path, nome_simples_zip)
if os.path.exists(caminho_simples_zip):
    print(f"Processando {nome_simples_zip}...")
    extrair_e_renomear_zip(caminho_simples_zip, directory_path)
else:
    print(f"Arquivo {nome_simples_zip} não encontrado em {directory_path}")


print("\nProcesso de extração finalizado.")
