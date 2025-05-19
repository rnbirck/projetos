import subprocess
import os
import time

# Lista das UFs
ufs_para_processar = [
    "AC",
    "AL",
    "AP",
    "AM",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MT",
    "MS",
    "MG",
    "PA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
    "MA",
    "PB",
    "PR",
    "PE",
    "PI",
    "RJ",
    "RN",
    "RS",
    "RO",
    "RR",
    "SC",
    "SP",
    "SE",
    "TO",
]

caminho_script_principal = "oportunidades.py"

if not os.path.exists(caminho_script_principal):
    print(f"Erro: Script principal '{caminho_script_principal}' não encontrado.")
    exit()

print(f"Iniciando processamento em lote para as UFs: {', '.join(ufs_para_processar)}")
print("-" * 30)

tempo_inicio_total = time.time()
erros = {}

for uf in ufs_para_processar:
    print(f"Executando script para UF: {uf}...")
    tempo_inicio_uf = time.time()
    try:
        # Monta o comando para executar o script principal

        comando = ["python", caminho_script_principal, "--uf", uf]

        # Executa o comando
        # stdout=subprocess.PIPE e stderr=subprocess.PIPE capturam a saída
        # text=True decodifica a saída como texto
        # check=True lança uma exceção se o script retornar um erro (exit code != 0)
        resultado = subprocess.run(
            comando,
            check=True,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )

        tempo_fim_uf = time.time()
        print(
            f"UF: {uf} processada com sucesso em {tempo_fim_uf - tempo_inicio_uf:.2f} segundos."
        )

    except subprocess.CalledProcessError as e:
        tempo_fim_uf = time.time()
        print(
            f"ERRO ao processar UF: {uf} após {tempo_fim_uf - tempo_inicio_uf:.2f} segundos."
        )
        print(f"Comando executado: {' '.join(e.cmd)}")
        print(f"Código de retorno: {e.returncode}")
        print("Erro (stderr):")
        print(e.stderr)
        # Opcional: imprimir stdout mesmo em caso de erro
        # print("Saída (stdout) antes do erro:")
        # print(e.stdout)
        erros[uf] = e.stderr  # Armazena o erro
    except Exception as e:
        tempo_fim_uf = time.time()
        print(
            f"ERRO inesperado ao processar UF: {uf} após {tempo_fim_uf - tempo_inicio_uf:.2f} segundos."
        )
        print(str(e))
        erros[uf] = str(e)  # Armazena o erro

    print("-" * 30)


tempo_fim_total = time.time()
print("\n--- Processamento em Lote Concluído ---")
print(f"Tempo total de execução: {tempo_fim_total - tempo_inicio_total:.2f} segundos.")

if erros:
    print("\nResumo dos erros:")
    for uf, erro in erros.items():
        print(f"  UF: {uf}")
        # print(f"    Erro: {erro[:500]}...")
else:
    print("\nTodos os UFs foram processados sem erros reportados.")
