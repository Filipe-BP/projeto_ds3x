# Projeto de Processamento e Carregamento de Dados

Este projeto automatiza o processamento e carregamento de dados para o BigQuery a partir de arquivos Excel. O fluxo do projeto envolve o download de arquivos, processamento de dados em duas etapas (`raw` e `trusted`), e carga desses dados em tabelas do BigQuery. Além disso, gera arquivos processados em formato Excel para análise posterior.

Estrutura do Projeto

/service_account
    └── service_account.json   # Arquivo de credenciais do Google Cloud
/downloads
    └── icc.xlsx               # Arquivo Excel para processar ICC
    └── icf.xlsx               # Arquivo Excel para processar ICF
/processado
    └── icc_raw.xlsx           # Dados processados para ICC (raw)
    └── icf_raw.xlsx           # Dados processados para ICF (raw)
    └── icc_trusted.xlsx       # Dados processados para ICC (trusted)
    └── icf_trusted.xlsx       # Dados processados para ICF (trusted)
    └── icf_icc_refined.xlsx   # Dados combinados e refinados de ICC e ICF
  
Requisitos

Para rodar o projeto, você precisa de:

- As bibliotecas listadas no arquivo `requirements.txt`

### Instalando as Dependências

Crie um ambiente virtual e instale as dependências usando o `requirements.txt`:


python -m venv venv
source venv/bin/activate   # Para sistemas Unix/Linux
venv\Scripts\activate      # Para sistemas Windows
pip install -r requirements.txt


O arquivo `requirements.txt` contém as seguintes dependências:


pandas
google-cloud-bigquery
google-auth
openpyxl


## Arquivos Principais

### `download_dados.py`

Este script é responsável por baixar os arquivos `icc.xlsx` e `icf.xlsx` para o diretório `/downloads`. O script não está incluso no seu código atual, mas o processo seria a automação do download dos arquivos de uma fonte definida.

### `inserir_dados.py`

Este script processa os arquivos `icc.xlsx` e `icf.xlsx`, executa transformações nos dados, e os carrega no BigQuery.

#### Fluxo do Script `inserir_dados.py`:

1. **Leitura dos Arquivos Excel**: 
   - Os arquivos `icc.xlsx` e `icf.xlsx` são lidos e processados.
   
2. **Processamento dos Dados**:
   - Os dados são limpos e transformados, com a remoção de colunas desnecessárias e renomeação das colunas.
   - Os dados são organizados em uma estrutura específica (incluindo os tipos de dados e valores) e preparados para carregamento.

3. **Carregamento no BigQuery**:
   - Os dados são carregados em tabelas no BigQuery em duas etapas: `raw` e `trusted`.
   - Consultas SQL são realizadas no BigQuery para refinar e combinar os dados, criando a tabela `icf_icc_refined`.

4. **Geração de Arquivos Excel Processados**:
   - Os dados `raw`, `trusted` e `refined` são salvos localmente em arquivos Excel para revisão e uso posterior.

#### Funções do Script `inserir_dados.py`:

- **processar_arquivo_raw(input_path, tipo)**: Processa os arquivos `icc.xlsx` ou `icf.xlsx` no formato raw, renomeando colunas e aplicando transformações.
  
- **limpar_nomes_colunas(df)**: Limpa os nomes das colunas para garantir que sejam compatíveis com os requisitos do BigQuery.

- **carregar_no_bigquery(df, tabela_nome)**: Carrega os dados processados para uma tabela no BigQuery.

- **obter_dados(query)**: Executa uma consulta SQL no BigQuery para obter dados refinados.

## Como Rodar

1. **Configuração do Ambiente**:
   - Certifique-se de que o arquivo de credenciais (`service_account.json`) esteja localizado no diretório `/service_account`.
   - Execute o arquivo download_dados.py para baixar os dados.


   python download_dados.py


2. **Executando o Script**:
   - Após configurar o ambiente, execute o script `inserir_dados.py` para processar e carregar os dados:


     python inserir_dados.py


3. **Verificação dos Resultados**:
   - Os arquivos processados (`icc_raw.xlsx`, `icf_raw.xlsx`, `icc_trusted.xlsx`, `icf_trusted.xlsx`, `icf_icc_refined.xlsx`) serão salvos no diretório `/processado`.
   - As tabelas correspondentes serão carregadas no BigQuery.

## Considerações Finais

- O script `inserir_dados.py` utiliza a API do BigQuery para carregar os dados. Certifique-se de que as credenciais da conta de serviço tenham permissão adequada para escrever nas tabelas do BigQuery.
- O processo de carregamento pode ser automatizado, dependendo das necessidades do projeto, como agendamento de execução com ferramentas de automação como o `cron` ou `Airflow`.