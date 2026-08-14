# PySpark Structured Streaming + Apache Kafka

Este projeto implementa um pipeline de **processamento de dados em tempo real** utilizando **Apache Kafka** e **PySpark Structured Streaming**.

O programa consome mensagens JSON publicadas em um tópico Kafka, realiza a conversão dos dados, aplica transformações e regras para identificação de anomalias em máquinas industriais e, finalmente, grava os dados processados em arquivos no formato **Parquet**.

## 📋 Sumário

- [Sobre o projeto](#sobre-o-projeto)
- [Arquitetura](#arquitetura)
- [Tecnologias utilizadas](#tecnologias-utilizadas)
- [Estrutura dos dados](#estrutura-dos-dados)
- [Transformações realizadas](#transformações-realizadas)
- [Alertas de máquinas](#alertas-de-máquinas)
- [Consumo de energia por peça](#consumo-de-energia-por-peça)
- [Saída dos dados](#saída-dos-dados)
- [Pré-requisitos](#pré-requisitos)
- [Configuração do ambiente](#configuração-do-ambiente)
- [Execução](#execução)
- [Fluxo de processamento](#fluxo-de-processamento)
- [Configurações](#configurações)
- [Checkpoint](#checkpoint)
- [Encerramento](#encerramento)
- [Possíveis melhorias](#possíveis-melhorias)

---

## Sobre o projeto

O objetivo deste projeto é demonstrar a utilização do **Spark Structured Streaming** para processamento contínuo de dados provenientes do Apache Kafka.

O fluxo de dados pode ser representado da seguinte forma:

```text
+------------------+
|   Fonte de dados |
|  CSV / Producer  |
+--------+---------+
         |
         v
+------------------+
|   Apache Kafka   |
|     topic=input  |
+--------+---------+
         |
         v
+----------------------------+
| PySpark Structured         |
| Streaming                  |
|                            |
| - Leitura do Kafka         |
| - Conversão JSON           |
| - Validação/Parsing        |
| - Transformações           |
| - Identificação de alertas |
+-------------+--------------+
              |
              v
+----------------------------+
|       Arquivos Parquet     |
|        data/parquet        |
+----------------------------+
```

O processamento é realizado continuamente, com novos dados sendo processados a cada **5 segundos**.

---

## Arquitetura

A aplicação utiliza os seguintes componentes:

### Apache Kafka

Responsável pelo recebimento e disponibilização das mensagens de dados industriais.

O programa espera que o Kafka esteja disponível em:

```text
localhost:9092
```

O tópico utilizado é:

```text
input
```

### PySpark Structured Streaming

O PySpark realiza a leitura contínua do tópico Kafka e processa as mensagens recebidas.

As principais etapas são:

1. Conexão com o Kafka;
2. Leitura do tópico;
3. Conversão do campo `value` para `String`;
4. Interpretação da string como JSON;
5. Aplicação do schema;
6. Conversão do timestamp;
7. Criação dos indicadores de alerta;
8. Cálculo do consumo de energia por peça;
9. Gravação dos resultados em Parquet.

### Parquet

Os dados processados são armazenados em arquivos Parquet no diretório:

```text
data/parquet
```

O formato Parquet é adequado para processamento analítico por ser colunar e oferecer boa eficiência de armazenamento e leitura.

---

## Tecnologias utilizadas

- **Python**
- **Apache Kafka**
- **PySpark**
- **Spark Structured Streaming**
- **Kafka Spark SQL Connector**
- **Parquet**
- **Conda**

---

## Estrutura dos dados

As mensagens recebidas do Kafka devem estar no formato JSON.

Um exemplo de mensagem esperada é:

```json
{
    "timestamp": "2026-08-14 15:30:00",
    "machine_id": "M001",
    "machine_type": "CNC",
    "part_id": "P001",
    "operator": "OP001",
    "cycle_time": 12.5,
    "temperature": 42.3,
    "vibration": 0.32,
    "power_consumption": 125.7,
    "rpm": 1500,
    "tool_number": "T01",
    "tool_wear": 35.2,
    "status": "OK",
    "error_code": null,
    "production_count": 10
}
```

### Campos

| Campo | Tipo | Descrição |
|---|---|---|
| `timestamp` | String | Data e hora da medição |
| `machine_id` | String | Identificador da máquina |
| `machine_type` | String | Tipo da máquina |
| `part_id` | String | Identificador da peça |
| `operator` | String | Operador responsável |
| `cycle_time` | Double | Tempo do ciclo de produção |
| `temperature` | Double | Temperatura da máquina |
| `vibration` | Double | Nível de vibração |
| `power_consumption` | Double | Consumo de energia |
| `rpm` | Integer | Rotação por minuto |
| `tool_number` | String | Identificador da ferramenta |
| `tool_wear` | Double | Desgaste da ferramenta |
| `status` | String | Status da máquina |
| `error_code` | String | Código de erro |
| `production_count` | Integer | Quantidade produzida |

---

## Transformações realizadas

Após a leitura das mensagens Kafka, o programa executa diversas transformações.

### Conversão do timestamp

O campo `timestamp`, inicialmente recebido como texto, é convertido para o tipo `timestamp` do Spark:

```python
to_timestamp(
    col("timestamp"),
    "yyyy-MM-dd HH:mm:ss"
)
```

O formato esperado é:

```text
yyyy-MM-dd HH:mm:ss
```

Por exemplo:

```text
2026-08-14 15:30:00
```

---

## Alertas de máquinas

O programa cria indicadores para identificar possíveis condições anormais nas máquinas.

### Alerta de temperatura

Uma máquina é considerada em situação de alerta quando a temperatura é maior ou igual a **45 °C**.

```python
temperature >= 45
```

É criada a coluna:

```text
temperature_alert
```

Valores possíveis:

```text
true
false
```

---

### Alerta de vibração

O alerta de vibração é ativado quando a vibração é maior ou igual a:

```text
0.50
```

A coluna criada é:

```text
vibration_alert
```

Regra:

```python
vibration >= 0.50
```

---

### Alerta de desgaste da ferramenta

O alerta de desgaste é ativado quando o desgaste da ferramenta é maior ou igual a:

```text
80
```

A coluna criada é:

```text
tool_wear_alert
```

Regra:

```python
tool_wear >= 80
```

---

## Alerta geral da máquina

Além dos indicadores individuais, o programa cria um indicador geral denominado:

```text
machine_alert
```

A máquina é considerada em situação de alerta quando pelo menos uma das condições abaixo é verdadeira:

- Temperatura maior ou igual a `45`;
- Vibração maior ou igual a `0.50`;
- Desgaste da ferramenta maior ou igual a `80`;
- Status igual a `ERROR`;
- Existe um código de erro.

A regra utilizada é equivalente a:

```text
temperature >= 45
OR
vibration >= 0.50
OR
tool_wear >= 80
OR
status == "ERROR"
OR
error_code IS NOT NULL
```

Esse indicador permite identificar rapidamente registros que apresentam alguma condição potencialmente anormal.

---

## Consumo de energia por peça

O programa também calcula o consumo de energia por peça produzida.

A coluna criada é:

```text
energy_per_part
```

O cálculo é:

```text
power_consumption / production_count
```

O resultado é arredondado para três casas decimais.

O cálculo somente é realizado quando:

```text
production_count > 0
```

Caso contrário, o resultado será nulo.

Isso evita uma divisão por zero.

Exemplo:

```text
power_consumption = 125.7
production_count = 10
```

Resultado:

```text
energy_per_part = 12.570
```

---

## Saída dos dados

Após o processamento, são selecionados os seguintes campos:

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

temperature_alert
vibration_alert
tool_wear_alert
machine_alert

energy_per_part
```

Os dados são gravados no formato **Parquet**.

Diretório configurado no `writeStream`:

```text
data/parquet
```

---

## Pré-requisitos

Antes de executar a aplicação, é necessário possuir:

- Linux ou outro sistema compatível;
- Python;
- Conda;
- Apache Kafka;
- Apache Spark;
- PySpark;
- Java compatível com a versão do Spark;
- Tópico Kafka denominado `input`.

Também é necessário que o Kafka esteja em execução antes de iniciar o streaming.

---

## Configuração do ambiente

O projeto utiliza um ambiente Conda denominado:

```text
ceaut
```

Para ativá-lo:

```bash
conda activate ceaut
```

Em seguida, entre no diretório do projeto:

```bash
cd kafka-pyspark-dashboard
```

---

## Dependência do Kafka Connector

Para que o Spark consiga consumir dados diretamente do Kafka, é necessário disponibilizar o conector:

```text
org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
```

Essa dependência é fornecida durante a execução por meio da opção:

```bash
--packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0
```

O conector permite que o Spark Structured Streaming utilize o Kafka como fonte de dados.

---

## Execução

### 1. Ativar o ambiente Conda

```bash
conda activate ceaut
```

### 2. Entrar no diretório do projeto

```bash
cd kafka-pyspark-dashboard
```

### 3. Executar o streaming

```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 \
  streaming-pyspark.py
```

Também é possível executar em uma única linha:

```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 streaming-pyspark.py
```

---

## Mensagens exibidas no terminal

Ao iniciar corretamente, a aplicação deverá apresentar informações semelhantes a:

```text
==========================================
 PySpark Structured Streaming
==========================================
Kafka: localhost:9092
Tópico: input
Output: ./data/output
Streaming iniciado...
==========================================
```

O processo continuará em execução aguardando novas mensagens no tópico Kafka.

---

## Fluxo de processamento

O processamento realizado pelo programa pode ser resumido nas seguintes etapas:

```text
Kafka
  |
  v
raw_stream
  |
  | value
  v
CAST(value AS STRING)
  |
  v
JSON
  |
  v
from_json()
  |
  v
StructType
  |
  v
parsed_stream
  |
  v
Transformações
  |
  +--> timestamp
  |
  +--> temperature_alert
  |
  +--> vibration_alert
  |
  +--> tool_wear_alert
  |
  +--> machine_alert
  |
  +--> energy_per_part
  |
  v
final_stream
  |
  v
Parquet
  |
  v
data/parquet
```

---

## Configurações

As principais configurações do programa estão concentradas no início do código.

### Servidor Kafka

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

Indica o endereço do servidor Kafka.

---

### Tópico Kafka

```python
KAFKA_TOPIC = "input"
```

Indica o tópico que será consumido pelo Spark.

---

### Checkpoint

O código define:

```python
CHECKPOINT_PATH = "./checkpoints/input_stream"
```

Entretanto, a consulta de streaming utiliza diretamente:

```python
.option(
    "checkpointLocation",
    "checkpoints/parquet"
)
```

Portanto, **o valor de `CHECKPOINT_PATH` atualmente não é utilizado pelo `writeStream`**.

Caso a intenção seja centralizar essa configuração, uma alternativa seria utilizar:

```python
.option(
    "checkpointLocation",
    CHECKPOINT_PATH
)
```

---

### Diretório de saída

O código define:

```python
OUTPUT_PATH = "./data/output"
```

Porém, a consulta utiliza:

```python
.option(
    "path",
    "data/parquet"
)
```

Assim, atualmente existe uma diferença entre a variável `OUTPUT_PATH` e o caminho efetivamente utilizado pelo streaming.

Para evitar inconsistências, recomenda-se utilizar:

```python
.option(
    "path",
    OUTPUT_PATH
)
```

ou alterar a variável para:

```python
OUTPUT_PATH = "data/parquet"
```

---

## Starting Offsets

A leitura do Kafka utiliza:

```python
.option(
    "startingOffsets",
    "latest"
)
```

Isso significa que, ao iniciar o streaming, o Spark começará a consumir **novas mensagens produzidas a partir daquele momento**.

Mensagens anteriores existentes no tópico não serão processadas inicialmente.

Para iniciar a leitura desde o começo dos dados disponíveis no Kafka, poderia ser utilizado:

```python
.option(
    "startingOffsets",
    "earliest"
)
```

Essa opção é particularmente útil em testes ou quando se deseja processar mensagens históricas.

---

## Modo de saída

O streaming utiliza:

```python
.outputMode("append")
```

O modo `append` adiciona novos registros ao resultado à medida que eles são processados.

Esse modo é adequado para este cenário porque os registros recebidos do Kafka são transformados e posteriormente gravados como novos registros no armazenamento Parquet.

---

## Trigger

O processamento é executado em intervalos de:

```text
5 segundos
```

Configurado por:

```python
.trigger(
    processingTime="5 seconds"
)
```

Isso significa que o Spark verifica e processa os dados disponíveis aproximadamente a cada cinco segundos.

---

## Checkpoint e tolerância a falhas

O Structured Streaming utiliza checkpoint para armazenar informações relacionadas ao progresso do processamento.

Neste projeto, o checkpoint está configurado em:

```text
checkpoints/parquet
```

O checkpoint é importante porque permite ao Spark manter informações sobre o processamento realizado e auxiliar na recuperação da aplicação em caso de interrupção.

O diretório de checkpoint **não deve ser confundido com o diretório de saída Parquet**:

```text
checkpoints/parquet
```

é utilizado para controle do streaming, enquanto:

```text
data/parquet
```

armazena os dados processados.

---

## Estrutura esperada do projeto

Uma possível estrutura de diretórios é:

```text
kafka-pyspark-dashboard/
│
├── streaming-pyspark.py
│
├── data/
│   ├── parquet/
│   │   └── ...
│   │
│   └── output/
│
├── checkpoints/
│   ├── input_stream/
│   │   └── ...
│   │
│   └── parquet/
│       └── ...
│
└── README.md
```

Os diretórios `data/parquet` e `checkpoints/parquet` poderão ser criados automaticamente pelo Spark, caso não existam.

---

## Integração com um Kafka Producer

O streaming espera mensagens no tópico:

```text
input
```

Portanto, é necessário que algum produtor Kafka publique mensagens nesse tópico.

O fluxo completo pode ser:

```text
CSV / Fonte de dados
        |
        v
Kafka Producer
        |
        v
Apache Kafka
        |
     tópico
      input
        |
        v
PySpark Structured Streaming
        |
        v
Processamento
        |
        v
Arquivos Parquet
```

O produtor deve enviar mensagens JSON compatíveis com o schema definido no programa.

---

## Schema do JSON

O schema utilizado pelo Spark é definido por:

```python
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("machine_id", StringType(), True),
    StructField("machine_type", StringType(), True),
    StructField("part_id", StringType(), True),
    StructField("operator", StringType(), True),

    StructField("cycle_time", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("vibration", DoubleType(), True),
    StructField("power_consumption", DoubleType(), True),

    StructField("rpm", IntegerType(), True),

    StructField("tool_number", StringType(), True),
    StructField("tool_wear", DoubleType(), True),

    StructField("status", StringType(), True),
    StructField("error_code", StringType(), True),

    StructField("production_count", IntegerType(), True)
])
```

O terceiro argumento de `StructField` está configurado como:

```python
True
```

para todos os campos, permitindo valores nulos.

---

## Bibliotecas Python utilizadas

O código utiliza as seguintes bibliotecas do PySpark:

```python
from pyspark.sql import SparkSession
```

Responsável pela criação da sessão Spark.

```python
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    lit,
    round
)
```

Utilizadas para manipulação e transformação dos dados.

```python
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)
```

Utilizadas para definição do schema das mensagens JSON.

---

## Parâmetros de segurança e validação

O programa possui algumas regras básicas para evitar resultados inválidos.

### Divisão por zero

O cálculo do consumo de energia somente é realizado quando:

```python
production_count > 0
```

Caso contrário:

```python
None
```

é utilizado como resultado.

### Valores nulos

Os campos do schema permitem valores nulos:

```python
StructField(..., True)
```

Além disso, o teste:

```python
col("error_code").isNotNull()
```

permite identificar a existência de um código de erro.

---

## Monitoramento

O programa mantém o streaming em execução por meio de:

```python
query.awaitTermination()
```

Essa chamada faz com que o processo principal permaneça aguardando enquanto a consulta de streaming estiver ativa.

Para interromper manualmente a aplicação, normalmente pode-se utilizar:

```text
Ctrl + C
```

---

## Verificação dos arquivos Parquet

Após o processamento das mensagens, os arquivos podem ser encontrados em:

```text
data/parquet
```

O conteúdo pode posteriormente ser lido utilizando PySpark, por exemplo:

```python
df = spark.read.parquet("data/parquet")

df.show()
```

Também é possível consultar o schema:

```python
df.printSchema()
```

---

## Exemplo de resultado

Uma mensagem de entrada como:

```json
{
    "timestamp": "2026-08-14 15:30:00",
    "machine_id": "M001",
    "machine_type": "CNC",
    "part_id": "P001",
    "operator": "OP001",
    "cycle_time": 12.5,
    "temperature": 48.2,
    "vibration": 0.32,
    "power_consumption": 125.7,
    "rpm": 1500,
    "tool_number": "T01",
    "tool_wear": 35.2,
    "status": "OK",
    "error_code": null,
    "production_count": 10
}
```

produzirá, entre outros campos:

```text
temperature_alert = true
vibration_alert   = false
tool_wear_alert   = false
machine_alert     = true
energy_per_part   = 12.570
```

Nesse caso, o alerta de temperatura é ativado porque:

```text
48.2 >= 45
```

Consequentemente, o indicador geral `machine_alert` também será:

```text
true
```

---

## Possíveis melhorias

Algumas melhorias podem ser incorporadas futuramente ao projeto.

### 1. Corrigir as variáveis de configuração

Atualmente, existem variáveis definidas, mas que não são utilizadas diretamente na consulta:

```python
CHECKPOINT_PATH
OUTPUT_PATH
```

Recomenda-se utilizar essas variáveis na configuração do streaming.

---

### 2. Validação de JSON inválido

Mensagens Kafka contendo JSON inválido podem resultar em registros não interpretados corretamente.

Pode-se adicionar uma etapa de validação para identificar e separar mensagens inválidas.

---

### 3. Tratamento de dados nulos

Pode-se implementar regras específicas para campos críticos como:

- temperatura;
- vibração;
- consumo de energia;
- produção;
- status.

---

### 4. Dashboard

Os arquivos Parquet produzidos pelo Spark podem posteriormente ser utilizados por uma aplicação de dashboard, por exemplo:

```text
Kafka
  |
  v
PySpark
  |
  v
Parquet
  |
  v
Dashboard
```

Um dashboard pode apresentar:

- temperatura das máquinas;
- vibração;
- consumo de energia;
- quantidade produzida;
- desgaste das ferramentas;
- quantidade de alertas;
- máquinas em situação de erro;
- histórico das máquinas;
- consumo médio por peça.

---

### 5. Detecção de anomalias

As regras atuais são baseadas em thresholds fixos.

Uma evolução natural seria utilizar técnicas de:

- Machine Learning;
- detecção estatística de anomalias;
- previsão de falhas;
- manutenção preditiva.

---

## Resumo

Este projeto demonstra um pipeline de processamento de dados industriais em tempo real utilizando **Apache Kafka** e **PySpark Structured Streaming**.

O fluxo principal é:

```text
                 +----------------+
                 | Kafka Producer |
                 +-------+--------+
                         |
                         v
                 +----------------+
                 | Apache Kafka   |
                 | topic: input   |
                 +-------+--------+
                         |
                         v
              +-----------------------+
              | PySpark Structured    |
              | Streaming             |
              +-----------+-----------+
                          |
             +------------+-------------+
             |            |             |
             v            v             v
         Parsing      Alertas      Cálculos
         JSON         Máquina      Energia
             |            |             |
             +------------+-------------+
                          |
                          v
                  +---------------+
                  |    Parquet    |
                  |  data/parquet |
                  +---------------+
```

A aplicação permite, portanto, transformar dados de máquinas industriais recebidos em tempo real pelo Kafka em informações estruturadas e enriquecidas, incluindo indicadores de temperatura, vibração, desgaste de ferramentas, anomalias e consumo de energia por peça.

## Licença

Este projeto pode ser adaptado e utilizado para fins acadêmicos, didáticos e experimentais.