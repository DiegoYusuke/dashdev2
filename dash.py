# dash.py

import streamlit as st
import pandas as pd
from pathlib import Path
import datetime

# --- Constantes de Configuração ---
DATA_PATH = Path("D:/Dev2/migracao_sheets/data")
FILE_PATHS = {
    "atendimentos": DATA_PATH / "dados-atendimentos.parquet",
    "feedbacks": DATA_PATH / "dados-feedbacks.parquet",
    "mapeamentos": DATA_PATH / "dados-mapeamentos.parquet",
    "reunioes": DATA_PATH / "dados-reunioes.parquet",
}
DATE_COLUMN = 'Data' # Nome padrão da coluna de data a ser usada

# --- Funções de Carregamento e Preparação ---

@st.cache_data # Cacheia o resultado para performance
def load_single_file(file_path, date_column):
    """Carrega e prepara um único arquivo Parquet."""
    try:
        df = pd.read_parquet(file_path)
        if date_column not in df.columns:
            st.warning(f"Coluna '{date_column}' não encontrada em {file_path.name}. Datas não serão processadas.")
            # Adiciona uma coluna de data vazia para evitar erros posteriores, ou retorna df como está.
            # Por simplicidade, vamos retornar como está, mas a filtragem de data falhará.
            # Uma abordagem melhor seria padronizar as colunas antes.
            return df
            # Ou: df[date_column] = pd.NaT
            # return df

        # Converte a coluna de data
        df[date_column] = pd.to_datetime(df[date_column], format='%d/%m/%Y', errors='coerce')
        # Remove linhas com datas inválidas (NaT) após a conversão
        original_rows = len(df)
        df = df.dropna(subset=[date_column])
        if len(df) < original_rows:
            st.warning(f"{original_rows - len(df)} linhas removidas de {file_path.name} devido a datas inválidas.")
        return df

    except FileNotFoundError:
        st.error(f"Erro: Arquivo não encontrado em {file_path}")
        return pd.DataFrame() # Retorna DataFrame vazio seguro
    except Exception as e:
        st.error(f"Erro ao carregar ou processar {file_path}: {e}")
        return pd.DataFrame()

def load_all_data(file_paths_dict, date_column):
    """Carrega todos os dataframes definidos em file_paths_dict."""
    data_frames = {}
    for key, path in file_paths_dict.items():
        data_frames[key] = load_single_file(path, date_column)
    return data_frames

def get_date_range(data_frames_dict, date_column):
    """Calcula a data mínima e máxima geral a partir dos dataframes."""
    all_dates = []
    for key, df in data_frames_dict.items():
        # Verifica se o dataframe não está vazio e possui a coluna de data
        if not df.empty and date_column in df.columns:
            all_dates.append(df[date_column])

    if not all_dates: # Se nenhuma data válida foi encontrada
        today = datetime.date.today()
        return today, today # Retorna hoje como min e max

    # Concatena todas as séries de datas válidas
    concatenated_dates = pd.concat(all_dates).dropna()

    if concatenated_dates.empty:
        today = datetime.date.today()
        return today, today

    # Calcula min e max e converte para date (sem a parte de hora)
    min_date = concatenated_dates.min().date()
    max_date = concatenated_dates.max().date()
    return min_date, max_date

# --- Funções de Interface do Usuário (UI) ---

def display_sidebar_filters(min_date, max_date):
    """Cria e exibe os filtros na barra lateral."""
    st.sidebar.header("Filtros")

    # Previne erro se min_date for maior que max_date (caso de dados vazios ou únicos)
    if min_date > max_date:
        max_date = min_date

    date_col1, date_col2 = st.sidebar.columns(2)
    with date_col1:
        start_date = st.date_input("Data Início", value=min_date, min_value=min_date, max_value=max_date, key='start_date')
    with date_col2:
        end_date = st.date_input("Data Fim", value=max_date, min_value=min_date, max_value=max_date, key='end_date')

    # Validação
    if start_date > end_date:
        st.sidebar.error("Erro: A data de início não pode ser posterior à data de fim.")
        st.stop() # Para a execução do script se a data for inválida

    return start_date, end_date

def display_main_content(filtered_data_dict, start_date, end_date):
    """Exibe o conteúdo principal da página (título, métricas, etc.)."""
    st.title("📊 Dashboard")
    st.markdown("Lendo dados de Atendimento, Reuniões, Feedbacks e Mapeamento.")
    st.markdown("---")

    st.subheader(f"Resumo do Período: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")

    col1, col2, col3, col4 = st.columns(4)

    # Garante que as chaves existem antes de tentar acessá-las
    def get_metric_value(data_dict, key):
        df = data_dict.get(key)
        return df.shape[0] if df is not None and not df.empty else 0

    with col1:
        st.metric(label="Reuniões", value=get_metric_value(filtered_data_dict, "reunioes"))
    with col2:
        st.metric(label="Feedbacks", value=get_metric_value(filtered_data_dict, "feedbacks"))
    with col3:
        st.metric(label="Atendimentos", value=get_metric_value(filtered_data_dict, "atendimentos"))
    with col4:
        st.metric(label="Mapeamentos", value=get_metric_value(filtered_data_dict, "mapeamentos"))

    st.markdown("---")

    # Placeholders para conteúdo futuro
    # st.subheader("Gráficos Detalhados")
    # st.write("(Gráficos serão adicionados aqui)")
    # st.subheader("Tabelas de Dados")
    # st.write("(Tabelas serão adicionadas aqui)")


# --- Funções de Lógica de Negócio ---

def filter_data_by_date(data_frames_dict, start_date, end_date, date_column):
    """Filtra todos os dataframes pelo intervalo de datas fornecido."""
    filtered_dict = {}
    # Converte datas de input para datetime para comparação (início do dia de start_date até fim do dia de end_date)
    start_datetime = pd.to_datetime(start_date)
    end_datetime = pd.to_datetime(end_date) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)

    for key, df in data_frames_dict.items():
        if not df.empty and date_column in df.columns:
            # Aplica o filtro de data
            mask = (df[date_column] >= start_datetime) & (df[date_column] <= end_datetime)
            filtered_dict[key] = df.loc[mask]
        else:
            # Mantém um DataFrame vazio se o original estava vazio ou sem coluna de data
            filtered_dict[key] = pd.DataFrame(columns=df.columns)

    return filtered_dict

# --- Função Principal ---

def main():
    """Função principal que orquestra a execução do dashboard."""
    # Configuração da página (pode ser a primeira chamada no script)
    st.set_page_config(
        page_title="Dashboard de Atividades",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # 1. Carregar os dados
    all_data = load_all_data(FILE_PATHS, DATE_COLUMN)

    # 2. Obter o intervalo de datas geral
    min_date, max_date = get_date_range(all_data, DATE_COLUMN)

    # 3. Exibir filtros na sidebar e obter seleção do usuário
    start_date, end_date = display_sidebar_filters(min_date, max_date)

    # 4. Filtrar os dados com base na seleção
    filtered_data = filter_data_by_date(all_data, start_date, end_date, DATE_COLUMN)

    # 5. Exibir o conteúdo principal (métricas, etc.)
    display_main_content(filtered_data, start_date, end_date)

# --- Ponto de Entrada do Script ---
if __name__ == "__main__":
    main()