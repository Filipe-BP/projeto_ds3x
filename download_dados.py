import requests
import os

# URLs de download dos arquivos
icc_url = "https://www.fecomercio.com.br/upload/file/2024/12/16/icc_link_download_202412.xlsx"
icf_url = "https://www.fecomercio.com.br/upload/file/2024/12/18/icf_link_download_202412.xlsx"

# Função para baixar os arquivos
def baixar_arquivo(url, nome_arquivo):
    try:
        # Realiza a requisição GET para baixar o arquivo
        response = requests.get(url)
        response.raise_for_status()  # Verifica se a requisição foi bem-sucedida (status 200)

        # Cria o diretório de downloads se não existir
        if not os.path.exists("downloads"):
            os.makedirs("downloads")

        # Salva o conteúdo do arquivo
        with open(f"downloads/{nome_arquivo}", "wb") as f:
            f.write(response.content)
        print(f"{nome_arquivo} baixado com sucesso!")
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar {nome_arquivo}: {e}")

# Baixar os arquivos ICC e ICF
baixar_arquivo(icc_url, "icc.xlsx")
baixar_arquivo(icf_url, "icf.xlsx")