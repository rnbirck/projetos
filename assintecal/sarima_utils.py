import pandas as pd
import numpy as np
from statsmodels.tsa.statespace.sarimax import SARIMAX


# Funcao de preparacao de dados


def aggregate_monthly_data_exp(
    df_raw,  # DataFrame com os dados brutos
    tradutor_sh6,  # DataFrame com os dados de tradutor,
    tradutor_calcado,  # DataFrame com os dados de tradutor de calcados
):
    """
    Agrega dados brutos mensais de exportacao, com tradutores e
    calcula o valor e pares totais por mês.
    """
    try:
        # Garantir tipo correto para marge
        if "CO_NCM" in tradutor_sh6.columns:
            tradutor_sh6["CO_NCM"] = tradutor_sh6["CO_NCM"].astype(int)
        if "CO_NCM" in df_raw.columns:
            df_raw["CO_NCM"] = df_raw["CO_NCM"].astype(int)

        # Merge, Filtro, Agregacao:

        df_aggregated = (
            df_raw.merge(tradutor_sh6, how="left", on="CO_NCM")
            .merge(tradutor_calcado, how="left", on="CO_SH6")
            .dropna(subset=["tipo_calcado"])
            .groupby(["CO_ANO", "CO_MES"])
            .agg(valor=("VL_FOB", "sum"), pares=("QT_ESTAT", "sum"))
            .reset_index()
            .assign(
                data=lambda x: pd.to_datetime(
                    x["CO_ANO"].astype(str) + "-" + x["CO_MES"].astype(str) + "-01"
                )
            )
            .sort_values("data")
            .drop(columns=["CO_ANO", "CO_MES"])
        )
        return df_aggregated

    except KeyError as e:
        print(f"Erro de coluna durante agregacao: {e}. Verifique nomes")
        return None
    except Exception as e:
        print(f"Erro inesperado durante agregacao: {e}")
        return None


# Funcao de formatacao para o modelo SARIMA


def format_data_for_sarima(
    df_aggregated, target_col_name, model_col_name="y", index_name="ds", freq="MS"
):
    """
    Formata os dados para o modelo SARIMA, renomeando colunas e definindo o index.
    """
    if target_col_name not in df_aggregated.columns:
        print(f"Coluna {target_col_name} não encontrada no DataFrame.")
        return None

    try:
        df_model = (
            df_aggregated[["data", target_col_name]]
            .rename(columns={target_col_name: model_col_name, "data": index_name})
            .set_index(index_name)
            .asfreq(freq)
        )

        if df_model.empty:
            print("DataFrame vazio após formatação.")
            return None

        return df_model
    except Exception as e:
        print(f"Erro inesperado durante formatação: {e}")
        return None


# Funcao de ajuste de outliers


def adjust_outliers_interpolate(
    df_model,  # DataFrame com índice 'ds' e coluna 'y'
    value_col="y",
    start_date=None,
    end_date=None,
):
    """
    Ajusta outliers em um período específico por interpolação linear.
    Retorna um novo DataFrame com a coluna interpolada ('y_interpolated').
    Se start_date ou end_date não forem fornecidos, ou em caso de erro,
    retorna uma cópia do DataFrame original.
    """
    # Retorna cópia se datas não forem fornecidas
    if start_date is None or end_date is None:
        print(
            "AVISO (adjust_outliers_interpolate): Datas não fornecidas. Retornando cópia original."
        )
        return df_model.copy()

    # Validar formato das datas
    try:
        pd.to_datetime(start_date)
        pd.to_datetime(end_date)
    except ValueError:
        print(
            f"ERRO Crítico (adjust_outliers_interpolate): Formato inválido para datas ('{start_date}', '{end_date}')."
        )
        return df_model.copy()  # Retorna cópia do original em caso de erro

    df_adj = df_model.copy()
    interpolated_col_name = f"{value_col}_interpolated"

    try:
        # --- Lógica principal ---
        original_nan_count = df_adj[value_col].isnull().sum()

        # Usar .loc para definir NaN é mais seguro
        indices_to_null = df_adj.loc[start_date:end_date].index
        if not indices_to_null.empty:
            df_adj.loc[indices_to_null, value_col] = np.nan
            num_replaced = df_adj[value_col].isnull().sum() - original_nan_count
            if num_replaced < 0:
                num_replaced = len(indices_to_null)  # Aproximação se já havia NaNs
        else:
            num_replaced = 0
            print(
                f"AVISO (adjust_outliers_interpolate): Nenhuma data encontrada no intervalo {start_date} a {end_date}."
            )

        # Cria a coluna interpolada
        df_adj[interpolated_col_name] = df_adj[value_col].interpolate(method="linear")

        # Preenche NaNs restantes (início/fim ou se interpolação falhou)
        missing_after_interp = df_adj[interpolated_col_name].isnull().sum()
        if missing_after_interp > 0:
            print(
                f"AVISO (adjust_outliers_interpolate): Preenchendo {missing_after_interp} NaNs restantes com bfill/ffill."
            )
            df_adj[interpolated_col_name] = (
                df_adj[interpolated_col_name]
                .fillna(method="bfill")
                .fillna(method="ffill")
            )

        # Verificação final de NaNs
        if df_adj[interpolated_col_name].isnull().any():
            print(
                f"AVISO Crítico (adjust_outliers_interpolate): NaNs ainda presentes em '{interpolated_col_name}'!"
            )

        # Retornar o DataFrame completo com a coluna original e a interpolada
        return df_adj.copy()

    except KeyError:
        print(
            f"ERRO Crítico (adjust_outliers_interpolate): Datas '{start_date}' ou '{end_date}' inválidas para indexação."
        )
        return df_model.copy()  # Retorna cópia do original
    except Exception as e:
        print(f"ERRO Crítico (adjust_outliers_interpolate): Erro inesperado: {e}")
        return df_model.copy()  # Retorna cópia do original


# Funcao de previsao SARIMA


def get_sarima_prediction(
    df_input,  # DataFrame pronto para modelagem (saída de format_data ou adjust_outliers)
    value_col,  # Nome da coluna a ser usada ('y' ou 'y_interpolated')
    order=(0, 1, 1),  # Ordem nao sazonal (p, d, q)
    seasonal_order=(0, 1, 1, 12),  # Ordem sazonal (P, D, Q, m)
    steps=1,  # Número de passos para previsão
    alpha=(0.90),
):
    """
    Ajusta um modelo SARIMAX aos dados fornecidos e retorna a previsao
    pontual e os intervalos de confiaca para o proximo mes.
    """
    if value_col not in df_input.columns:
        print(f"Coluna {value_col} não encontrada no DataFrame.")
        return None
    if df_input[value_col].isnull().any():
        print(f"Coluna {value_col} contém NaNs. Preencha antes de usar.")
        return None

    print(
        f"Ajustando SARIMA{order}x{seasonal_order} aos dados da coluna '{value_col}'..."
    )

    try:
        # Criar e ajustar o modelo SARIMAX
        model = SARIMAX(
            df_input[value_col],
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        results = model.fit(disp=False)

        forecast = results.get_forecast(steps=steps)

        # Obter o DataFrame de resumo com intervalos de confianca
        forecast_summary = forecast.summary_frame(alpha=alpha)

        # Extrair resultados (para o primeiro passo, se houver mais)
        next_pred_date = forecast_summary.index[0]
        point_forecast = forecast_summary["mean"].iloc[0]
        lower_ci = forecast_summary["mean_ci_lower"].iloc[0]
        upper_ci = forecast_summary["mean_ci_upper"].iloc[0]

        # Retornar resultados em um dicionário
        return {
            "data": next_pred_date.strftime("%Y-%m-%d"),
            "cenario_pessimista": lower_ci,
            "cenario_base": point_forecast,
            "cenario_otimista": upper_ci,
        }
    except Exception as e:
        print(f"Erro ao ajustar o modelo SARIMA: {e}")
        return None


def run_complete_forecast_pipeline(
    # Todos os parametros necessarios para o pipeline de previsao
    df_raw,  # DataFrame com os dados brutos
    tradutor_sh6,  # DataFrame com os dados de tradutor,
    tradutor_calcado,  # DataFrame com os dados de tradutor de calcados
    target_col_name,  # Nome da coluna alvo (valor ou pares)
    outlier_start=None,
    outlier_end=None,  # Intervalo de datas para ajuste de outliers
    sarima_order=(0, 1, 1),  # Ordem não sazonal (p, d, q)
    sarima_seasonal_order=(0, 1, 1, 12),  # Ordem sazonal (P, D, Q, m)
    forecast_steps=1,  # Número de passos para previsão
    forecast_alpha=0.05,  # Nível de confiança para intervalos de previsão
):
    """Executa o pipeline completo de previsão SARIMA."""

    # 1. Agregar os dados
    df_aggregated = aggregate_monthly_data_exp(df_raw, tradutor_sh6, tradutor_calcado)
    if df_aggregated is None:
        print("Erro na agregação dos dados. Pipeline interrompido.")
        return None, None  # Return None para previsao e historico

    # Guardar para calcular a variacao YoY
    df_historical = df_aggregated.copy()

    # 2. Formatar os dados para o modelo SARIMA

    df_model = format_data_for_sarima(
        df_aggregated,
        target_col_name=target_col_name,
    )
    if df_model is None:
        print("Erro na formatação dos dados. Pipeline interrompido.")
        return None, df_historical  # Retorna historico mesmo se falhar na previsao

    # 3. Ajustar outliers (se necessário)

    if outlier_start and outlier_end:
        df_model = adjust_outliers_interpolate(
            df_model,
            value_col="y",
            start_date=outlier_start,
            end_date=outlier_end,
        )
        if df_model is None:
            print("Erro no ajuste de outliers. Pipeline interrompido.")
            return None, df_historical

    # 4. Obter previsão SARIMA

    sarima_results = get_sarima_prediction(
        df_model,
        value_col="y_interpolated" if outlier_start and outlier_end else "y",
        order=sarima_order,
        seasonal_order=sarima_seasonal_order,
        steps=forecast_steps,
        alpha=forecast_alpha,
    )
    if sarima_results is None:
        print("Erro na previsão SARIMA. Pipeline interrompido.")
        return None, df_historical

    print("Previsão SARIMA concluída com sucesso.")

    # 5. Pos processamento

    resultado = pd.DataFrame(
        {
            "data": pd.to_datetime([sarima_results["data"]]),
            "cenario_pessimista": [sarima_results["cenario_pessimista"]],
            "cenario_base": [sarima_results["cenario_base"]],
            "cenario_otimista": [sarima_results["cenario_otimista"]],
        }
    )

    df_combined = pd.concat(
        [
            df_historical[["data", target_col_name]]
            .assign(
                taxa_mensal=lambda x: (
                    x[target_col_name] / x[target_col_name].shift(12) - 1
                )
                * 100
            )
            .query("data.dt.year >= 2024"),
            (
                resultado[
                    ["data", "cenario_base", "cenario_pessimista", "cenario_otimista"]
                ]
                .assign(data=lambda x: pd.to_datetime(x["data"]))
                .query("data.dt.year >= 2024")
            ),
        ]
    ).assign(
        data=lambda x: x["data"].dt.strftime("%Y-%m"),
        cenario_base=lambda x: (x["cenario_base"] / x[target_col_name].shift(12) - 1)
        * 100,
        cenario_pessimista=lambda x: (
            x["cenario_pessimista"] / x[target_col_name].shift(12) - 1
        )
        * 100,
        cenario_otimista=lambda x: (
            x["cenario_otimista"] / x[target_col_name].shift(12) - 1
        )
        * 100,
    )

    return df_combined
