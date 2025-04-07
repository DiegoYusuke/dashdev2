import gspread
import pandas as pd

def extract_data_from_sheet(sheet_url, tab_index):

    try:
        # Autentica com o Google Sheets usando OAuth 2.0
        gc = gspread.oauth()

        # Abre a planilha pela URL
        sh = gc.open_by_url(sheet_url)

        # Seleciona a aba
        worksheet = sh.get_worksheet(tab_index)

        # Obtém todos os valores da aba
        data = worksheet.get_all_values()

        # Cria um DataFrame do pandas
        df = pd.DataFrame(data)

        # Define a primeira linha como cabeçalhos
        df.columns = df.iloc[0]

        # Remove a primeira linha (cabeçalhos) e redefine o índice
        df = df[1:].reset_index(drop=True)

        return df

    except Exception as e:
        print(f"Erro ao extrair dados da planilha: {e}")
        return None  # Retorna None em caso de erro

if __name__ == "__main__":
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1Tbze4MTLIq8F-jrSpWr3yPtjUo9KUaTcj9FnjFHuTvA/edit?gid=2122327024#gid=2122327024"
    TABS_TO_EXTRACT = [0, 1, 2, 3, 4]  # Substitua com os índices corretos das abas
    TAB_NAMES = {
        0: "dados-atendimentos",
        1: "dados-mapeamentos",
        2: "dados-reunioes",
        3: "dados-feedbacks",
        4: "status-clientes"
    }

    for tab_index in TABS_TO_EXTRACT:
        df_result = extract_data_from_sheet(SHEET_URL, tab_index)
        if df_result is not None:
            # Crie um nome de arquivo para a aba
            parquet_file = f"{TAB_NAMES[tab_index]}.parquet"

            # Salve o DataFrame como um arquivo Parquet
            df_result.to_parquet(parquet_file, index=False)
            print(f"Dados da aba {TAB_NAMES[tab_index]} salvos em {parquet_file}")

    print("Extração e salvamento concluídos.")