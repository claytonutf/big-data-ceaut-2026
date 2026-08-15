#--- Como executar (1):
'''
Selecionar com botão direito o diretório kafka-pyspark-dashboard e clicar em Copy Path

Abrir um novo CMD

digitar:
cd <pressionar ctrl+v>

conda activate ceaut

streamlit run dashboard.py
'''


import streamlit as st
import pandas as pd
from pathlib import Path
from streamlit_autorefresh import st_autorefresh


# ============================================================
# CONFIGURAÇÕES
# ============================================================

PARQUET_PATH = Path("data/parquet")

st.set_page_config(
    page_title="Monitoramento Industrial",
    page_icon="🏭",
    layout="wide"
)


# ============================================================
# ATUALIZAÇÃO AUTOMÁTICA
# ============================================================

# Atualiza o dashboard a cada 2 segundos
st_autorefresh(
    interval=2000,
    key="dashboard_refresh"
)


# ============================================================
# FUNÇÃO PARA CARREGAR OS DADOS
# ============================================================

@st.cache_data(ttl=2)
def carregar_dados():

    # --------------------------------------------------------
    # Verifica se o diretório existe
    # --------------------------------------------------------

    if not PARQUET_PATH.exists():

        return pd.DataFrame()


    # --------------------------------------------------------
    # Localiza arquivos Parquet
    # --------------------------------------------------------

    arquivos = list(
        PARQUET_PATH.glob("*.parquet")
    )


    if not arquivos:

        return pd.DataFrame()


    # --------------------------------------------------------
    # Lista para armazenar os DataFrames válidos
    # --------------------------------------------------------

    dataframes = []


    # --------------------------------------------------------
    # Lê os arquivos individualmente
    # --------------------------------------------------------

    for arquivo in arquivos:

        try:

            # Ignora arquivos vazios
            if arquivo.stat().st_size == 0:
                continue


            df_temp = pd.read_parquet(
                arquivo
            )


            # Só adiciona se possuir registros
            if not df_temp.empty:

                dataframes.append(
                    df_temp
                )


        except Exception:

            # O arquivo pode estar sendo escrito pelo Spark.
            # Nesse caso, simplesmente ignoramos nesta leitura.
            continue


    # --------------------------------------------------------
    # Nenhum arquivo válido
    # --------------------------------------------------------

    if not dataframes:

        return pd.DataFrame()


    # --------------------------------------------------------
    # Junta todos os arquivos
    # --------------------------------------------------------

    df = pd.concat(
        dataframes,
        ignore_index=True
    )


    # --------------------------------------------------------
    # Remove possíveis duplicidades
    # --------------------------------------------------------

    df = df.drop_duplicates()


    # --------------------------------------------------------
    # Converte timestamp
    # --------------------------------------------------------

    if "timestamp" in df.columns:

        df["timestamp"] = pd.to_datetime(
            df["timestamp"],
            errors="coerce"
        )


    return df

# ============================================================
# TÍTULO
# ============================================================

st.title(
    "🏭 Monitoramento Industrial"
)

st.markdown(
    """
    **Universidade Tecnológica Federal do Paraná**  
    **Especialização em Automação Industrial (CEAUT)**  
    **Professor Clayton Kossoski**
    """
)

st.caption(
    "Monitoramento em tempo real com "
    "Kafka + PySpark Structured Streaming + Streamlit"
)


# ============================================================
# CARREGA DADOS
# ============================================================

df = carregar_dados()


# ============================================================
# VERIFICAÇÃO
# ============================================================

if df.empty:

    st.warning(
        "Nenhum dado disponível."
    )

    st.info(
        "Aguardando dados processados pelo PySpark..."
    )

    st.stop()


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.header(
    "⚙️ Filtros"
)


# ============================================================
# FILTRO DE MÁQUINA
# ============================================================

if "machine_id" in df.columns:

    maquinas = sorted(
        df["machine_id"]
        .dropna()
        .unique()
    )

    maquinas_selecionadas = st.sidebar.multiselect(
        "Máquina",
        maquinas,
        default=maquinas
    )

    if maquinas_selecionadas:

        df = df[
            df["machine_id"].isin(
                maquinas_selecionadas
            )
        ]


# ============================================================
# FILTRO DE STATUS
# ============================================================

if "status" in df.columns:

    status_disponiveis = sorted(
        df["status"]
        .dropna()
        .unique()
    )

    status_selecionados = st.sidebar.multiselect(
        "Status",
        status_disponiveis,
        default=status_disponiveis
    )

    if status_selecionados:

        df = df[
            df["status"].isin(
                status_selecionados
            )
        ]


# ============================================================
# ATUALIZAÇÃO
# ============================================================

st.sidebar.divider()

st.sidebar.success(
    "🔄 Atualização automática: 2 segundos"
)


if st.sidebar.button(
    "🔄 Atualizar agora"
):

    st.cache_data.clear()

    st.rerun()


# ============================================================
# INFORMAÇÕES DO STREAMING
# ============================================================

st.sidebar.divider()

st.sidebar.metric(
    "Registros",
    f"{len(df):,}"
)


if "timestamp" in df.columns:

    ultimo_timestamp = (
        df["timestamp"]
        .dropna()
        .max()
    )

    if pd.notna(ultimo_timestamp):

        st.sidebar.caption(
            "Último evento:"
        )

        st.sidebar.write(
            ultimo_timestamp.strftime(
                "%d/%m/%Y %H:%M:%S"
            )
        )


# ============================================================
# KPIs
# ============================================================

st.subheader(
    "📊 Indicadores"
)


col1, col2, col3, col4 = st.columns(4)


# ============================================================
# TOTAL DE MÁQUINAS
# ============================================================

if "machine_id" in df.columns:

    total_maquinas = (
        df["machine_id"]
        .nunique()
    )

else:

    total_maquinas = 0


# ============================================================
# PRODUÇÃO
# ============================================================

if "production_count" in df.columns:

    producao_total = (
        pd.to_numeric(
            df["production_count"],
            errors="coerce"
        )
        .fillna(0)
        .sum()
    )

else:

    producao_total = 0


# ============================================================
# ALERTAS
# ============================================================

if "machine_alert" in df.columns:

    total_alertas = int(
        df["machine_alert"]
        .fillna(False)
        .astype(bool)
        .sum()
    )

else:

    # Caso o Spark ainda não tenha criado machine_alert,
    # consideramos WARNING e ERROR como alertas.

    if "status" in df.columns:

        total_alertas = int(
            df["status"]
            .isin(
                [
                    "WARNING",
                    "ERROR"
                ]
            )
            .sum()
        )

    else:

        total_alertas = 0


# ============================================================
# TEMPERATURA MÉDIA
# ============================================================

if "temperature" in df.columns:

    temperatura_media = (
        pd.to_numeric(
            df["temperature"],
            errors="coerce"
        )
        .mean()
    )

else:

    temperatura_media = 0


# ============================================================
# EXIBE KPIs
# ============================================================

col1.metric(
    "🏭 Máquinas",
    total_maquinas
)

col2.metric(
    "📦 Produção",
    f"{int(producao_total):,}"
)

col3.metric(
    "🚨 Alertas",
    total_alertas
)

col4.metric(
    "🌡️ Temperatura média",
    f"{temperatura_media:.1f} °C"
)


st.divider()


# ============================================================
# TEMPERATURA E VIBRAÇÃO
# ============================================================

col1, col2 = st.columns(2)


# ============================================================
# TEMPERATURA POR MÁQUINA
# ============================================================

with col1:

    st.subheader(
        "🌡️ Temperatura por Máquina"
    )

    if {
        "machine_id",
        "temperature"
    }.issubset(df.columns):

        temperatura = (
            df
            .groupby("machine_id")[
                "temperature"
            ]
            .mean()
            .sort_values(
                ascending=False
            )
        )

        st.bar_chart(
            temperatura
        )


# ============================================================
# VIBRAÇÃO POR MÁQUINA
# ============================================================

with col2:

    st.subheader(
        "📳 Vibração por Máquina"
    )

    if {
        "machine_id",
        "vibration"
    }.issubset(df.columns):

        vibracao = (
            df
            .groupby("machine_id")[
                "vibration"
            ]
            .mean()
            .sort_values(
                ascending=False
            )
        )

        st.bar_chart(
            vibracao
        )


# ============================================================
# PRODUÇÃO E ENERGIA
# ============================================================

col1, col2 = st.columns(2)


# ============================================================
# PRODUÇÃO POR MÁQUINA
# ============================================================

with col1:

    st.subheader(
        "📦 Produção por Máquina"
    )

    if {
        "machine_id",
        "production_count"
    }.issubset(df.columns):

        producao = (
            df
            .groupby("machine_id")[
                "production_count"
            ]
            .sum()
            .sort_values(
                ascending=False
            )
        )

        st.bar_chart(
            producao
        )


# ============================================================
# CONSUMO DE ENERGIA
# ============================================================

with col2:

    st.subheader(
        "⚡ Consumo de Energia"
    )

    if {
        "machine_id",
        "power_consumption"
    }.issubset(df.columns):

        energia = (
            df
            .groupby("machine_id")[
                "power_consumption"
            ]
            .mean()
            .sort_values(
                ascending=False
            )
        )

        st.bar_chart(
            energia
        )


st.divider()


# ============================================================
# EVOLUÇÃO DA TEMPERATURA
# ============================================================

st.subheader(
    "📈 Evolução da Temperatura"
)


if {
    "timestamp",
    "machine_id",
    "temperature"
}.issubset(df.columns):

    temperatura_tempo = (
        df[
            [
                "timestamp",
                "machine_id",
                "temperature"
            ]
        ]
        .dropna()
        .sort_values(
            "timestamp"
        )
    )


    if not temperatura_tempo.empty:

        temperatura_tempo = (
            temperatura_tempo
            .pivot(
                index="timestamp",
                columns="machine_id",
                values="temperature"
            )
        )

        st.line_chart(
            temperatura_tempo
        )


# ============================================================
# EVOLUÇÃO DA VIBRAÇÃO
# ============================================================

st.subheader(
    "📈 Evolução da Vibração"
)


if {
    "timestamp",
    "machine_id",
    "vibration"
}.issubset(df.columns):

    vibracao_tempo = (
        df[
            [
                "timestamp",
                "machine_id",
                "vibration"
            ]
        ]
        .dropna()
        .sort_values(
            "timestamp"
        )
    )


    if not vibracao_tempo.empty:

        vibracao_tempo = (
            vibracao_tempo
            .pivot(
                index="timestamp",
                columns="machine_id",
                values="vibration"
            )
        )

        st.line_chart(
            vibracao_tempo
        )


st.divider()


# ============================================================
# ALERTAS
# ============================================================

st.subheader(
    "🚨 Alertas Detectados"
)


# ============================================================
# IDENTIFICA ALERTAS
# ============================================================

if "machine_alert" in df.columns:

    alertas = df[
        df["machine_alert"] == True
    ].copy()

else:

    if "status" in df.columns:

        alertas = df[
            df["status"].isin(
                [
                    "WARNING",
                    "ERROR"
                ]
            )
        ].copy()

    else:

        alertas = pd.DataFrame()


# ============================================================
# EXIBE ALERTAS
# ============================================================

if alertas.empty:

    st.success(
        "✅ Nenhum alerta detectado."
    )

else:

    quantidade_alertas = len(
        alertas
    )

    st.error(
        f"⚠️ {quantidade_alertas} "
        f"registro(s) com alerta."
    )


    colunas_alertas = [
        "timestamp",
        "machine_id",
        "machine_type",
        "temperature",
        "vibration",
        "power_consumption",
        "tool_wear",
        "status",
        "error_code",
        "machine_alert"
    ]


    colunas_existentes = [
        coluna
        for coluna in colunas_alertas
        if coluna in alertas.columns
    ]


    st.dataframe(
        alertas[
            colunas_existentes
        ]
        .sort_values(
            "timestamp",
            ascending=False
        ),
        use_container_width=True,
        hide_index=True
    )


# ============================================================
# STATUS DAS MÁQUINAS
# ============================================================

st.subheader(
    "🏭 Status das Máquinas"
)


if {
    "machine_id",
    "status"
}.issubset(df.columns):


    # --------------------------------------------------------
    # Seleciona o registro mais recente de cada máquina
    # --------------------------------------------------------

    if "timestamp" in df.columns:

        status_maquinas = (
            df
            .sort_values(
                "timestamp"
            )
            .groupby(
                "machine_id"
            )
            .tail(1)
        )

    else:

        status_maquinas = (
            df
            .groupby(
                "machine_id"
            )
            .tail(1)
        )


    # --------------------------------------------------------
    # Colunas
    # --------------------------------------------------------

    colunas_status = [
        "machine_id",
        "status"
    ]


    if "timestamp" in status_maquinas.columns:

        colunas_status.insert(
            0,
            "timestamp"
        )


    if "temperature" in status_maquinas.columns:

        colunas_status.append(
            "temperature"
        )


    if "vibration" in status_maquinas.columns:

        colunas_status.append(
            "vibration"
        )


    if "tool_wear" in status_maquinas.columns:

        colunas_status.append(
            "tool_wear"
        )


    # --------------------------------------------------------
    # Exibe
    # --------------------------------------------------------

    st.dataframe(
        status_maquinas[
            colunas_status
        ]
        .sort_values(
            "machine_id"
        ),
        use_container_width=True,
        hide_index=True
    )


st.divider()


# ============================================================
# ÚLTIMOS REGISTROS
# ============================================================

st.subheader(
    "📡 Últimos Dados Recebidos"
)


if "timestamp" in df.columns:

    ultimos = (
        df
        .sort_values(
            "timestamp",
            ascending=False
        )
        .head(20)
    )

else:

    ultimos = df.head(20)


st.dataframe(
    ultimos,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# TODOS OS DADOS
# ============================================================

with st.expander(
    "🔎 Visualizar todos os dados processados"
):

    if "timestamp" in df.columns:

        dados_completos = (
            df
            .sort_values(
                "timestamp",
                ascending=False
            )
        )

    else:

        dados_completos = df


    st.dataframe(
        dados_completos,
        use_container_width=True,
        hide_index=True
    )


# ============================================================
# RODAPÉ
# ============================================================

st.divider()


st.caption(
    f"Registros carregados: {len(df):,} | "
    f"Atualização automática: 2 segundos | "
    f"Última atualização: "
    f"{pd.Timestamp.now().strftime('%d/%m/%Y %H:%M:%S')}"
)