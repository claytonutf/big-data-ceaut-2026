# 🏭 Dashboard de Monitoramento Industrial

Dashboard desenvolvido em **Python + Streamlit** para visualização e monitoramento de dados industriais processados por **Apache Kafka** e **PySpark Structured Streaming**.

A aplicação lê arquivos **Parquet** gerados pelo processamento do PySpark, apresenta indicadores de produção, temperatura, vibração, consumo de energia e alertas das máquinas, além de realizar atualização automática a cada **2 segundos**.

---

## 📋 Sumário

- [Sobre o projeto](#-sobre-o-projeto)
- [Tecnologias utilizadas](#-tecnologias-utilizadas)
- [Arquitetura](#-arquitetura)
- [Estrutura do projeto](#-estrutura-do-projeto)
- [Pré-requisitos](#-pré-requisitos)
- [Instalação](#-instalação)
- [Execução](#-execução)
- [Funcionamento](#-funcionamento)
- [Atualização automática](#-atualização-automática)
- [Filtros](#-filtros)
- [Indicadores](#-indicadores)
- [Visualizações](#-visualizações)
- [Detecção de alertas](#-detecção-de-alertas)
- [Status das máquinas](#-status-das-máquinas)
- [Dados exibidos](#-dados-exibidos)
- [Estrutura dos dados](#-estrutura-dos-dados)
- [Cache](#-cache)
- [Tratamento de arquivos Parquet](#-tratamento-de-arquivos-parquet)
- [Possíveis problemas](#-possíveis-problemas)
- [Tecnologias e responsabilidades](#-tecnologias-e-responsabilidades)
- [Autor](#-autor)

---

## 📌 Sobre o projeto

O **Dashboard de Monitoramento Industrial** tem como objetivo apresentar, de forma visual e praticamente em tempo real, informações provenientes de um fluxo de dados industriais.

A solução faz parte de uma arquitetura baseada em:

- **Apache Kafka** para transmissão dos eventos;
- **PySpark Structured Streaming** para processamento dos dados;
- **Parquet** para armazenamento dos dados processados;
- **Pandas** para manipulação dos dados no dashboard;
- **Streamlit** para construção da interface web.

O dashboard é executado localmente utilizando o comando:

```bash
streamlit run dashboard.py
```

A aplicação foi desenvolvida no contexto da:

> **Universidade Tecnológica Federal do Paraná**  
> **Especialização em Automação Industrial (CEAUT)**

---

## 🛠️ Tecnologias utilizadas

| Tecnologia | Utilização |
|---|---|
| Python | Linguagem de programação |
| Streamlit | Construção do dashboard |
| Pandas | Manipulação e análise dos dados |
| PyArrow / Parquet | Leitura dos arquivos Parquet |
| Streamlit Autorefresh | Atualização automática da interface |
| Apache Kafka | Transmissão dos eventos |
| Apache Spark | Processamento dos dados |
| PySpark Structured Streaming | Processamento contínuo dos eventos |

---

## 🏗️ Arquitetura

O fluxo geral da aplicação pode ser representado da seguinte forma:

```text
┌─────────────────┐
│ Fonte de dados  │
│    industriais  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apache Kafka   │
│     Topic       │
└────────┬────────┘
         │
         ▼
┌──────────────────────────┐
│ PySpark Structured       │
│ Streaming                │
│                          │
│ Processamento dos dados  │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Arquivos Parquet         │
│ data/parquet/*.parquet   │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Dashboard Streamlit      │
│                          │
│ Pandas + Streamlit       │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│ Visualização em tempo    │
│ quase real               │
└──────────────────────────┘
```

O dashboard não realiza diretamente o consumo dos eventos do Kafka. Sua responsabilidade é carregar os dados processados pelo PySpark e armazenados no diretório:

```text
data/parquet/
```

---

## 📁 Estrutura do projeto

Uma possível estrutura para o projeto é:

```text
kafka-pyspark-dashboard/
│
├── data/
│   └── parquet/
│       ├── arquivo-001.parquet
│       ├── arquivo-002.parquet
│       └── ...
│
├── dashboard.py
│
├── producer-kafka.py
├── consumer-kafka.py
│
├── processamento-pyspark.py
│
└── README.md
```

O arquivo principal do dashboard é:

```text
dashboard.py
```

Os dados processados pelo Spark devem estar disponíveis em:

```text
data/parquet/
```

---

# 🔧 Pré-requisitos

Antes de executar o projeto, é necessário possuir um ambiente Python configurado.

Recomenda-se utilizar um ambiente Conda denominado:

```text
ceaut
```

Ative o ambiente com:

```bash
conda activate ceaut
```

Também é necessário instalar as bibliotecas utilizadas pelo dashboard.

---

# 📦 Instalação

## 1. Ativar o ambiente Conda

```bash
conda activate ceaut
```

## 2. Acessar o diretório do projeto

```bash
cd kafka-pyspark-dashboard
```

## 3. Instalar as dependências

Caso ainda não estejam instaladas:

```bash
pip install streamlit pandas pyarrow streamlit-autorefresh
```

O `pyarrow` é utilizado pelo Pandas para leitura dos arquivos Parquet.

---

# ▶️ Execução

Com o ambiente Conda ativado e estando no diretório do projeto:

```bash
conda activate ceaut

cd kafka-pyspark-dashboard

streamlit run dashboard.py
```

O comando:

```bash
streamlit run dashboard.py
```

inicia o servidor web do Streamlit e disponibiliza a interface do dashboard.

---

# ⚙️ Funcionamento

O dashboard realiza as seguintes operações:

1. Configura a aplicação Streamlit;
2. Ativa a atualização automática;
3. Localiza os arquivos Parquet;
4. Carrega os dados utilizando Pandas;
5. Remove registros duplicados;
6. Converte o campo `timestamp`;
7. Apresenta filtros;
8. Calcula indicadores;
9. Gera gráficos;
10. Identifica alertas;
11. Apresenta o status atual das máquinas;
12. Exibe os últimos registros;
13. Permite visualizar todos os dados processados.

---

# 🔄 Atualização automática

A aplicação utiliza `streamlit-autorefresh` para atualizar automaticamente o dashboard.

O intervalo configurado é de:

```text
2 segundos
```

No código:

```python
st_autorefresh(
    interval=2000,
    key="dashboard_refresh"
)
```

O valor:

```text
2000
```

representa **2000 milissegundos**, ou seja, **2 segundos**.

Dessa forma, o dashboard verifica periodicamente se novos dados foram disponibilizados nos arquivos Parquet.

---

# 📂 Carregamento dos dados

Os arquivos Parquet são procurados no diretório:

```python
PARQUET_PATH = Path("data/parquet")
```

A função responsável pelo carregamento é:

```python
carregar_dados()
```

A função:

- verifica se o diretório existe;
- localiza os arquivos `.parquet`;
- ignora arquivos vazios;
- tenta carregar individualmente cada arquivo;
- ignora arquivos que não possam ser lidos naquele momento;
- concatena os DataFrames;
- remove duplicidades;
- converte o timestamp.

---

## 🔍 Arquivos Parquet em processamento

Um cuidado importante foi implementado para situações em que o Spark esteja escrevendo um arquivo enquanto o Streamlit tenta lê-lo.

Nesse caso, a leitura pode gerar uma exceção.

O dashboard simplesmente ignora o arquivo naquele ciclo:

```python
except Exception:
    continue
```

Isso permite que o dashboard continue funcionando enquanto o Spark grava novos arquivos.

---

# 🧹 Remoção de duplicidades

Depois que os arquivos são concatenados, o dashboard remove possíveis registros duplicados:

```python
df = df.drop_duplicates()
```

Isso evita que registros repetidos provenientes de diferentes arquivos Parquet sejam apresentados no dashboard.

---

# 🕐 Conversão do timestamp

Quando a coluna `timestamp` está disponível, ela é convertida para o formato de data/hora utilizando:

```python
pd.to_datetime(
    df["timestamp"],
    errors="coerce"
)
```

O parâmetro:

```text
errors="coerce"
```

faz com que valores que não possam ser convertidos sejam transformados em valores nulos (`NaT`).

---

# 🎛️ Filtros

O dashboard possui filtros disponíveis na barra lateral.

## Máquina

Permite selecionar uma ou mais máquinas:

```text
⚙️ Filtros
└── Máquina
```

O filtro utiliza a coluna:

```text
machine_id
```

É possível selecionar múltiplas máquinas simultaneamente.

---

## Status

Também é possível filtrar os registros pelo status da máquina.

A coluna utilizada é:

```text
status
```

Os valores disponíveis são obtidos diretamente dos dados carregados.

---

# 📊 Indicadores

O dashboard apresenta quatro indicadores principais.

## 🏭 Máquinas

Representa a quantidade de máquinas distintas encontradas nos dados.

O cálculo é realizado utilizando:

```python
df["machine_id"].nunique()
```

---

## 📦 Produção

Representa a produção total.

A coluna utilizada é:

```text
production_count
```

Os valores são convertidos para numérico antes da soma.

---

## 🚨 Alertas

Representa a quantidade de registros identificados como alertas.

Quando disponível, é utilizada a coluna:

```text
machine_alert
```

Caso essa coluna ainda não exista, o dashboard considera como alertas os registros cujo status seja:

```text
WARNING
ERROR
```

---

## 🌡️ Temperatura média

Representa a temperatura média dos registros disponíveis.

A coluna utilizada é:

```text
temperature
```

Os valores são convertidos para numérico e, posteriormente, é calculada a média.

---

# 📈 Visualizações

O dashboard apresenta diferentes gráficos para acompanhamento das máquinas.

---

## 🌡️ Temperatura por Máquina

Apresenta a temperatura média agrupada por máquina.

O cálculo é realizado utilizando:

```python
df.groupby("machine_id")["temperature"].mean()
```

O resultado é exibido utilizando um gráfico de barras:

```python
st.bar_chart(temperatura)
```

---

## 📳 Vibração por Máquina

Apresenta a vibração média de cada máquina.

A coluna utilizada é:

```text
vibration
```

Os dados são agrupados por:

```text
machine_id
```

e a média é calculada para cada máquina.

---

## 📦 Produção por Máquina

Apresenta a produção acumulada por máquina.

A coluna utilizada é:

```text
production_count
```

O cálculo utiliza a soma dos valores:

```python
df.groupby("machine_id")["production_count"].sum()
```

---

## ⚡ Consumo de Energia

Apresenta o consumo médio de energia por máquina.

A coluna utilizada é:

```text
power_consumption
```

Os dados são agrupados por máquina e a média é calculada.

---

# 📈 Evolução da Temperatura

O dashboard também apresenta a evolução da temperatura ao longo do tempo.

São utilizadas as seguintes colunas:

```text
timestamp
machine_id
temperature
```

Os dados são:

1. selecionados;
2. removidos valores nulos;
3. ordenados pelo timestamp;
4. reorganizados utilizando `pivot`;
5. apresentados em um gráfico de linhas.

A estrutura utilizada é:

```python
temperatura_tempo = temperatura_tempo.pivot(
    index="timestamp",
    columns="machine_id",
    values="temperature"
)
```

O resultado é apresentado utilizando:

```python
st.line_chart(temperatura_tempo)
```

---

# 📈 Evolução da Vibração

O mesmo procedimento é utilizado para acompanhar a vibração ao longo do tempo.

São utilizadas:

```text
timestamp
machine_id
vibration
```

Cada máquina é representada como uma série temporal no gráfico.

---

# 🚨 Alertas Detectados

A seção:

```text
🚨 Alertas Detectados
```

apresenta os registros considerados anormais.

Quando existe a coluna:

```text
machine_alert
```

são selecionados os registros em que:

```text
machine_alert == True
```

Caso essa coluna não exista, são considerados alertas os registros com:

```text
WARNING
```

ou:

```text
ERROR
```

---

## 📋 Informações dos alertas

Quando existem alertas, são apresentadas, quando disponíveis, as seguintes informações:

| Campo | Descrição |
|---|---|
| `timestamp` | Data/hora do evento |
| `machine_id` | Identificação da máquina |
| `machine_type` | Tipo da máquina |
| `temperature` | Temperatura |
| `vibration` | Vibração |
| `power_consumption` | Consumo de energia |
| `tool_wear` | Desgaste da ferramenta |
| `status` | Status da máquina |
| `error_code` | Código do erro |
| `machine_alert` | Indicador de alerta |

Os registros são apresentados do evento mais recente para o mais antigo.

---

# 🏭 Status das Máquinas

A seção:

```text
🏭 Status das Máquinas
```

apresenta o estado mais recente conhecido de cada máquina.

Para isso, os registros são ordenados pelo:

```text
timestamp
```

e agrupados por:

```text
machine_id
```

Depois, é selecionado o último registro de cada máquina.

---

## 📋 Informações apresentadas

A tabela pode apresentar:

- `timestamp`
- `machine_id`
- `status`
- `temperature`
- `vibration`
- `tool_wear`

A disponibilidade de cada campo depende das colunas presentes nos dados processados.

---

# 📡 Últimos Dados Recebidos

A seção:

```text
📡 Últimos Dados Recebidos
```

apresenta os **20 registros mais recentes**.

Quando existe a coluna `timestamp`, os dados são ordenados de forma decrescente:

```text
mais recente
      ↓
mais antigo
```

Caso o timestamp não esteja disponível, são apresentados os primeiros 20 registros do DataFrame.

---

# 🔎 Visualização de Todos os Dados

O dashboard possui também um componente expansível:

```text
🔎 Visualizar todos os dados processados
```

Ao expandi-lo, todos os registros carregados são apresentados.

Quando existe `timestamp`, os dados são ordenados do mais recente para o mais antigo.

---

# 📊 Informações na barra lateral

A barra lateral apresenta também informações sobre o processamento.

Entre elas:

### Registros

Quantidade de registros carregados no DataFrame:

```text
Registros
```

### Último evento

Quando disponível, apresenta o timestamp mais recente encontrado nos dados.

O formato utilizado é:

```text
DD/MM/AAAA HH:MM:SS
```

### Atualização

A aplicação informa:

```text
🔄 Atualização automática: 2 segundos
```

---

# 🔄 Atualização manual

Além da atualização automática, existe o botão:

```text
🔄 Atualizar agora
```

Ao pressioná-lo, o cache dos dados é limpo:

```python
st.cache_data.clear()
```

e o dashboard é executado novamente:

```python
st.rerun()
```

Isso força uma nova leitura dos arquivos Parquet.

---

# ⚡ Cache dos dados

A função responsável pela leitura dos dados utiliza:

```python
@st.cache_data(ttl=2)
```

O tempo de vida do cache é de:

```text
2 segundos
```

Essa configuração reduz leituras desnecessárias dos arquivos Parquet e, ao mesmo tempo, permite que novos dados sejam disponibilizados rapidamente.

---

# 🗂️ Estrutura esperada dos dados

O dashboard foi desenvolvido considerando dados industriais contendo campos como:

```text
timestamp
machine_id
machine_type
part_id
operator
cycle_time
temperature
vibration
power_consumption
rpm
tool_number
tool_wear
status
error_code
production_count
```

Além desses campos, o dashboard também pode utilizar:

```text
machine_alert
```

A presença de determinadas colunas é verificada antes de realizar os cálculos e visualizações.

Isso permite que a aplicação continue funcionando mesmo quando determinados campos não estiverem disponíveis.

---

# 🧩 Tratamento de dados ausentes

O código utiliza verificações como:

```python
if "machine_id" in df.columns:
```

ou:

```python
if {
    "machine_id",
    "temperature"
}.issubset(df.columns):
```

Assim, determinados componentes somente são processados quando as respectivas colunas estão disponíveis.

Isso evita erros quando o esquema dos dados sofrer alterações ou quando determinado campo ainda não tiver sido produzido pelo pipeline de processamento.

---

# ⚠️ Nenhum dado disponível

Caso não existam arquivos Parquet válidos, o dashboard apresenta:

```text
Nenhum dado disponível.
```

e:

```text
Aguardando dados processados pelo PySpark...
```

Nesse cenário, a execução é interrompida até que novos dados estejam disponíveis.

---

# 🛡️ Tolerância durante a escrita dos arquivos

O dashboard considera que o PySpark pode estar escrevendo arquivos Parquet enquanto a aplicação tenta realizar a leitura.

Por esse motivo, cada arquivo é lido individualmente e erros de leitura são ignorados.

Isso evita que um arquivo temporariamente indisponível interrompa completamente a aplicação.

---

# 🖥️ Configuração da página

A página Streamlit é configurada com:

```text
Título: Monitoramento Industrial
Ícone: 🏭
Layout: wide
```

O layout amplo permite utilizar melhor o espaço horizontal para apresentar gráficos e indicadores.

---

# 📌 Rodapé

Ao final do dashboard são apresentadas informações como:

```text
Registros carregados
Atualização automática
Última atualização
```

A última atualização utiliza o horário atual do sistema.

---

# 🔗 Integração com Kafka e PySpark

O dashboard representa a etapa de visualização de uma arquitetura maior de processamento de dados.

O fluxo conceitual é:

```text
Produtor
   │
   ▼
Kafka
   │
   ▼
PySpark Structured Streaming
   │
   ▼
Processamento
   │
   ▼
Parquet
   │
   ▼
Streamlit
   │
   ▼
Dashboard
```

O Kafka é responsável pelo transporte dos eventos.

O PySpark realiza o processamento dos dados.

Os arquivos Parquet funcionam como fonte de dados para o dashboard.

O Streamlit é responsável pela visualização.

---

# 🚀 Execução completa

Considerando que os componentes do projeto estejam configurados, a sequência conceitual de execução é:

## 1. Ativar o ambiente

```bash
conda activate ceaut
```

## 2. Acessar o projeto

```bash
cd kafka-pyspark-dashboard
```

## 3. Iniciar o Kafka

A inicialização depende da configuração utilizada no ambiente.

## 4. Executar o produtor

Por exemplo:

```bash
python producer-kafka.py
```

## 5. Executar o processamento PySpark

Executar o programa responsável pelo consumo e processamento dos eventos.

## 6. Verificar os arquivos Parquet

Os dados processados devem estar disponíveis em:

```text
data/parquet/
```

## 7. Iniciar o dashboard

```bash
streamlit run dashboard.py
```

---

# 🧪 Verificação rápida

Após iniciar o dashboard, verifique:

- [ ] O ambiente Conda `ceaut` está ativo.
- [ ] O diretório `kafka-pyspark-dashboard` está correto.
- [ ] O diretório `data/parquet` existe.
- [ ] Existem arquivos `.parquet`.
- [ ] Os arquivos possuem registros.
- [ ] O PySpark está processando os eventos.
- [ ] O dashboard foi iniciado com `streamlit run dashboard.py`.
- [ ] Os indicadores estão sendo atualizados.
- [ ] Os gráficos estão sendo apresentados.
- [ ] A atualização automática está funcionando.

---

# ❗ Possíveis problemas

## Dashboard sem dados

Se aparecer:

```text
Nenhum dado disponível.
```

verifique:

```text
data/parquet/
```

e confirme se existem arquivos Parquet contendo registros.

---

## Arquivo Parquet sendo escrito

Se um arquivo estiver sendo criado ou atualizado pelo Spark, ele pode temporariamente não estar disponível para leitura.

O dashboard ignora esse arquivo e tentará carregá-lo novamente na próxima atualização.

---

## Coluna inexistente

Alguns gráficos dependem de colunas específicas.

Por exemplo, o gráfico de temperatura depende de:

```text
machine_id
temperature
```

O código verifica a existência dessas colunas antes de gerar o gráfico.

---

## Alertas não aparecem

A detecção de alertas depende de:

```text
machine_alert
```

ou, na ausência dessa coluna:

```text
status
```

com valores:

```text
WARNING
ERROR
```

---

# 📚 Resumo das principais funcionalidades

| Funcionalidade | Implementação |
|---|---|
| Leitura dos dados | Pandas |
| Fonte dos dados | Arquivos Parquet |
| Atualização automática | 2 segundos |
| Cache | 2 segundos |
| Filtro por máquina | Sim |
| Filtro por status | Sim |
| Total de máquinas | Sim |
| Produção total | Sim |
| Total de alertas | Sim |
| Temperatura média | Sim |
| Temperatura por máquina | Sim |
| Vibração por máquina | Sim |
| Produção por máquina | Sim |
| Consumo de energia | Sim |
| Evolução da temperatura | Sim |
| Evolução da vibração | Sim |
| Detecção de alertas | Sim |
| Status atual das máquinas | Sim |
| Últimos 20 registros | Sim |
| Visualização de todos os dados | Sim |
| Atualização manual | Sim |

---

# 👨‍🏫 Autor

**Professor Clayton Kossoski**

Universidade Tecnológica Federal do Paraná — UTFPR

**Especialização em Automação Industrial (CEAUT)**

---

# 📄 Licença

Este projeto foi desenvolvido para fins **educacionais e acadêmicos**, no contexto da Especialização em Automação Industrial (CEAUT).