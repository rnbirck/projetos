# %%
import pandas as pd
import numpy as np
import utils
import importlib
import os

importlib.reload(utils)

print("Iniciando ranqueamento")
arquivo = "municipios_2019"
colunas_de_renomeacao = "descricao"  # pode ser ou "descricao" ou "coluna"

base = pd.read_excel(
    f"bases/base_{arquivo}.xlsx",
    sheet_name="base_ranqueamento",
    engine="calamine",
)

classificacao = pd.read_excel(
    f"bases/base_{arquivo}.xlsx",
    sheet_name="classificacao",
    engine="calamine",
)

dicionario_tipo_classificacao = (
    classificacao[["var", "ordem"]].set_index("var")["ordem"].to_dict()
)

dicionario_bloco = {}
if "bloco" in classificacao.columns and "var" in classificacao.columns:
    try:
        dicionario_bloco = (
            classificacao[["var", "bloco"]].set_index("var")["bloco"].to_dict()
        )
    except Exception:
        dicionario_bloco = {}

# Colunas Valor
colunas_valor = classificacao.query("tipo == 'valor'")["var"].tolist()

df_valor = base[["id"] + colunas_valor].copy()

df_valor_nota_1 = utils.nota_valor_1(df=df_valor, colunas=colunas_valor)

df_valor_nota_2 = utils.nota_valor_2(df=df_valor, colunas=colunas_valor)

df_valor_nota_3 = utils.nota_valor_3(df=df_valor, colunas=colunas_valor)

df_valor_notas = pd.concat(
    [
        df_valor_nota_1.set_index("id"),
        df_valor_nota_2.set_index("id"),
        df_valor_nota_3.set_index("id"),
    ],
    axis=1,
)
df_valor_final = utils.nota_final(df=df_valor_notas, colunas=colunas_valor)

# Colunas Taxa
colunas_taxa = classificacao.query("tipo == 'taxa'")["var"].tolist()

df_taxa = base[["id"] + colunas_taxa].copy()

df_taxa_nota_1 = utils.nota_taxa_1(df=df_taxa, colunas=colunas_taxa)

df_taxa_nota_2 = utils.nota_taxa_2(df=df_taxa, colunas=colunas_taxa)

df_taxa_nota_3 = utils.nota_taxa_3(df=df_taxa, colunas=colunas_taxa)

df_taxa_notas = pd.concat(
    [
        df_taxa_nota_1.set_index("id"),
        df_taxa_nota_2.set_index("id"),
        df_taxa_nota_3.set_index("id"),
    ],
    axis=1,
)

df_taxa_final = utils.nota_final(df=df_taxa_notas, colunas=colunas_taxa)

colunas_participacao = classificacao.query("tipo == 'participacao'")["var"].tolist()

df_participacao = base[["id"] + colunas_participacao].copy()

df_participacao_nota_1 = utils.nota_participacao_1(
    df=df_participacao, colunas=colunas_participacao
)

df_participacao_nota_2 = utils.nota_participacao_2(
    df=df_participacao, colunas=colunas_participacao
)

df_participacao_nota_3 = utils.nota_participacao_3(
    df=df_participacao, colunas=colunas_participacao
)

df_participacao_notas = pd.concat(
    [
        df_participacao_nota_1.set_index("id"),
        df_participacao_nota_2.set_index("id"),
        df_participacao_nota_3.set_index("id"),
    ],
    axis=1,
)

df_participacao_final = utils.nota_final(
    df=df_participacao_notas, colunas=colunas_participacao
)

# Concatenando o DataFrame base com os DataFrames de notas
df_variaveis_notas = pd.concat(
    [
        base.set_index("id"),
        df_valor_final,
        df_taxa_final,
        df_participacao_final,
    ],
    axis=1,
).reset_index()

# Ajustando as colunas de notas para as colunas de variaveis invertidas

notas_invertidas = {5: -1, 3: 1, 1: 1, -1: 5, np.nan: np.nan}
prefixo_nota = "nota_"
colunas_nota_para_inverter = []

for coluna in df_variaveis_notas.columns:
    if coluna.startswith(prefixo_nota):
        var_original = coluna[len(prefixo_nota) :]
        if dicionario_tipo_classificacao.get(var_original) == "invertido":
            colunas_nota_para_inverter.append(coluna)

for coluna in colunas_nota_para_inverter:
    df_variaveis_notas[coluna] = df_variaveis_notas[coluna].map(notas_invertidas)

# Calcular as médias e notas para os blocos, caso definidos:
df_variaveis_blocos_notas = utils.calcular_notas_bloco(
    df_variaveis_notas, dicionario_bloco
)

# Separando todas as colunas de var
todas_colunas_base = classificacao["var"].tolist()

# Ordenando o DataFrame com as variaveis e notas
df_reordenada = utils.ordenar_df_com_notas(
    df=df_variaveis_blocos_notas,
    colunas_base=todas_colunas_base,
    sort_key_func=utils.sort_key,
    id_col="id",
)

# Renomeando e criando o dataframe final
df_final = utils.renomear_colunas_mapeadas(
    df=df_reordenada,
    map_df=classificacao,
    map_from_col="var",
    map_to_col=f"{colunas_de_renomeacao}",
)
# Calculando a média geral das médias dos blocos, se existir
colunas_nota_bloco = [col for col in df_final.columns if col.startswith("media_bloco_")]
if colunas_nota_bloco:
    nome_nova_coluna = "Média Geral dos Blocos"
    try:
        df_final[nome_nova_coluna] = df_final[colunas_nota_bloco].mean(
            axis=1, skipna=True
        )
    except Exception:
        if nome_nova_coluna in df_final.columns:
            del df_final[nome_nova_coluna]

# Calculando a média geral das notas dos blocos, se existir
colunas_nota_bloco = [col for col in df_final.columns if col.startswith("nota_bloco_")]
if colunas_nota_bloco:
    nome_nova_coluna = "Média Geral das Notas dos Blocos"
    try:
        df_final[nome_nova_coluna] = df_final[colunas_nota_bloco].mean(
            axis=1, skipna=True
        )
    except Exception:
        if nome_nova_coluna in df_final.columns:
            del df_final[nome_nova_coluna]

df_final = df_final.round(3)

# Mapear colunas para blocos
colunas_por_bloco = utils.mapear_colunas_para_blocos_excel(
    df_final, classificacao, colunas_de_renomeacao
)

# Definir cores para os blocos (pode passar sua paleta personalizada aqui se quiser)
mapa_cores = utils.definir_cores_para_blocos_excel(colunas_por_bloco)

# Iniciar o ExcelWriter e escrever os dados (sem cabeçalho)
caminho_saida = "resultados"
if not os.path.exists(caminho_saida):
    os.makedirs(caminho_saida)
arquivo_saida_excel = os.path.join(caminho_saida, f"ranqueamento_{arquivo}.xlsx")
nome_da_planilha = "ranqueamento"

writer, workbook, worksheet = utils.iniciar_excel_e_escrever_dados(
    df_final, arquivo_saida_excel, nome_da_planilha
)

# Aplicar formatação aos cabeçalhos e colunas
if writer:
    utils.formatar_cabecalhos_e_colunas_excel(
        worksheet, workbook, df_final, colunas_por_bloco, mapa_cores
    )

    # Salvar e fechar o arquivo Excel
    try:
        writer.close()
        print(f"Arquivo Excel formatado salvo com sucesso em: {arquivo_saida_excel}")
    except Exception as e_save:
        print(f"ERRO CRÍTICO ao salvar o arquivo Excel: {e_save}")
else:
    print("ERRO: Não foi possível iniciar o escritor de Excel. Arquivo não gerado.")
