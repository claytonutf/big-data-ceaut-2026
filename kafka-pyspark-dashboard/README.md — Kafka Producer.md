# Kafka Producer — Publicação de Logs de Máquinas

Este projeto implementa um **Kafka Producer em Python** responsável por ler registros de máquinas industriais a partir de um arquivo CSV, converter os dados para os tipos apropriados e publicar os eventos no tópico `input` do Apache Kafka.

A aplicação simula uma máquina produzindo continuamente, publicando um novo evento a cada **2 segundos**.

---

## 1. Arquitetura

O fluxo básico da aplicação é:

```text
logs-maquina.csv
       |
       v
+-------------------+
| Kafka Producer    |
|     Python        |
+-------------------+
       |
       | JSON
       v
+-------------------+
| Apache Kafka      |
| Topic: input     |
+-------------------+
       |
       v
   Processamento
```

O Producer realiza as seguintes operações:

1. Abre o arquivo `logs-maquina.csv`;
2. Lê cada registro utilizando `csv.DictReader`;
3. Converte os campos numéricos para `float` ou `int`;
4. Converte campos vazios de `error_code` para `null`;
5. Converte o registro para JSON;
6. Publica o evento no tópico Kafka `input`;
7. Aguarda a confirmação do Kafka;
8. Exibe o offset e algumas informações do evento;
9. Aguarda 2 segundos antes de publicar o próximo evento.

---

## 2. Tecnologias utilizadas

- Python 3
- Apache Kafka 4.3.1
- `kafka-python`
- CSV
- JSON

---

## 3. Estrutura do projeto

Uma estrutura sugerida para o projeto é:

```text
kafka-pyspark-dashboard/
│
├── producer-kafka.py
├── logs-maquina.csv
└── README.md
```

O diretório do Kafka deve estar disponível separadamente:

```text
kafka_2.13-4.3.1/
```

---

## 4. Pré-requisitos

Antes de executar o Producer, é necessário ter instalado:

- Python 3;
- Conda;
- Apache Kafka;
- Java compatível com a versão do Kafka;
- Biblioteca `kafka-python`.

### 4.1. Ambiente Conda

O exemplo utiliza um ambiente Conda chamado `ceaut`.

Ative o ambiente:

```bash
conda activate ceaut
```

Caso o ambiente ainda não exista, ele pode ser criado com:

```bash
conda create -n ceaut python=3.12
```

Depois:

```bash
conda activate ceaut
```

### 4.2. Instalação do kafka-python

Instale a biblioteca utilizando:

```bash
pip install kafka-python
```

Para verificar a instalação:

```bash
pip show kafka-python
```

---

## 5. Inicialização do Apache Kafka

Antes de executar o Producer, o servidor Kafka deve estar em execução.

Considerando a instalação:

```text
kafka_2.13-4.3.1
```

acesse o diretório:

```bash
cd kafka_2.13-4.3.1
```

Inicialize o Kafka conforme a configuração utilizada no ambiente.

O Producer está configurado para utilizar:

```text
localhost:9092
```

Portanto, o Kafka deve estar disponível nessa porta.

---

## 6. Criação do tópico Kafka

O projeto utiliza três tópicos:

| Tópico | Finalidade |
|---|---|
| `input` | Receber os eventos publicados pelo Producer |


> **Observação:** o tópico `dashaboard` foi mantido com essa grafia para permanecer consistente com a configuração apresentada no projeto. Caso a intenção seja utilizar a grafia correta em inglês, o nome recomendado seria `dashboard`.

### Criar o tópico `input`

```bash
cd kafka_2.13-4.3.1

bin/kafka-topics.sh \
  --create \
  --topic input \
  --bootstrap-server localhost:9092
```

---

## 7. Arquivo CSV

O Producer utiliza o arquivo:

```text
logs-maquina.csv
```

O arquivo deve estar no mesmo diretório do script `producer-kafka.py`.

Os campos esperados são:

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

Um exemplo de registro é:

```csv
timestamp,machine_id,machine_type,part_id,operator,cycle_time,temperature,vibration,power_consumption,rpm,tool_number,tool_wear,status,error_code,production_count
2026-07-31 08:00:45,CNC-01,Centro de Usinagem,P0003,Carlos,22.1,41.0,0.25,5.1,3490,T01,13.0,RUNNING,,3
```

---

## 8. Código do Producer

O arquivo `producer-kafka.py` contém o seguinte código:

```python
import csv
import json
import time

from kafka import KafkaProducer

TOPIC = "input"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3,
    acks="all"
)

CSV_FILE = "logs-maquina.csv"

print("Iniciando Producer...")

with open(CSV_FILE, newline="", encoding="utf-8") as csvfile:

    reader = csv.DictReader(csvfile)

    for row in reader:

        # Conversão dos tipos
        event = {
            "timestamp": row["timestamp"],
            "machine_id": row["machine_id"],
            "machine_type": row["machine_type"],
            "part_id": row["part_id"],
            "operator": row["operator"],
            "cycle_time": float(row["cycle_time"]),
            "temperature": float(row["temperature"]),
            "vibration": float(row["vibration"]),
            "power_consumption": float(row["power_consumption"]),
            "rpm": int(row["rpm"]),
            "tool_number": row["tool_number"],
            "tool_wear": float(row["tool_wear"]),
            "status": row["status"],
            "error_code": row["error_code"] if row["error_code"] else None,
            "production_count": int(row["production_count"])
        }

        future = producer.send(TOPIC, event)

        metadata = future.get(timeout=10)

        print(
            f"[{metadata.offset}] "
            f"{event['machine_id']} -> "
            f"{event['part_id']} "
            f"({event['status']})"
        )

        producer.flush()

        # Simula uma máquina produzindo continuamente
        time.sleep(2)

producer.close()

print("\nFim da publicação.")
```

---

## 9. Configuração do Producer

O tópico utilizado pelo Producer é definido pela constante:

```python
TOPIC = "input"
```

O endereço do Kafka é:

```python
bootstrap_servers="localhost:9092"
```

### Confirmação das mensagens

O Producer utiliza:

```python
acks="all"
```

Isso faz com que a publicação aguarde a confirmação necessária dos brokers configurados.

Também são configuradas três tentativas de reenvio:

```python
retries=3
```

---

## 10. Serialização das mensagens

Os eventos são convertidos para JSON por meio de:

```python
value_serializer=lambda v: json.dumps(v).encode("utf-8")
```

Assim, um dicionário Python como:

```python
{
    "machine_id": "CNC-01",
    "temperature": 41.0
}
```

é convertido para uma mensagem JSON em bytes antes de ser enviada ao Kafka.

---

## 11. Conversão dos tipos

Os valores provenientes de um arquivo CSV são inicialmente tratados como strings.

Por isso, o Producer realiza conversões explícitas.

### Valores `float`

São convertidos utilizando:

```python
float()
```

Exemplos:

```python
"cycle_time"
"temperature"
"vibration"
"power_consumption"
"tool_wear"
```

### Valores `int`

São convertidos utilizando:

```python
int()
```

Exemplos:

```python
"rpm"
"production_count"
```

### Código de erro

O campo `error_code` possui um tratamento especial:

```python
"error_code": row["error_code"] if row["error_code"] else None
```

Quando o campo está vazio no CSV, o valor enviado para o Kafka será:

```json
null
```

---

## 12. Publicação da mensagem

A publicação é realizada com:

```python
future = producer.send(TOPIC, event)
```

Como o envio é assíncrono, o código utiliza:

```python
metadata = future.get(timeout=10)
```

para aguardar a confirmação da publicação.

O `metadata` permite obter informações sobre a mensagem, incluindo o seu `offset`.

---

## 13. Exibição da publicação

Após a confirmação da mensagem, o Producer exibe:

```text
[offset] machine_id -> part_id (status)
```

Por exemplo:

```text
[0] CNC-01 -> P0003 (RUNNING)
```

Outro evento poderá produzir:

```text
[1] CNC-02 -> P0005 (RUNNING)
```

O número entre colchetes representa o `offset` da mensagem dentro da partição do tópico.

---

## 14. Simulação de produção contínua

Após publicar cada evento, o programa aguarda:

```python
time.sleep(2)
```

Isso simula uma máquina industrial produzindo continuamente e enviando novos eventos ao Kafka.

Na prática:

```text
Evento 1
   |
   +--> Kafka
   |
   +--> espera 2 segundos
   |
Evento 2
   |
   +--> Kafka
   |
   +--> espera 2 segundos
   |
Evento 3
   |
   +--> Kafka
```

---

## 15. Como executar

Com o Kafka em execução, abra um terminal e acesse o diretório do projeto:

```bash
conda activate ceaut

cd kafka-pyspark-dashboard

python producer-kafka.py
```

Ao iniciar, deverá aparecer:

```text
Iniciando Producer...
```

Depois, a cada mensagem publicada, será exibida uma linha semelhante a:

```text
[0] CNC-01 -> P0003 (RUNNING)
```

Ao terminar a leitura do CSV:

```text
Fim da publicação.
```

---

## 16. Monitorando o tópico `input`

Para verificar as mensagens recebidas pelo Kafka, pode-se utilizar o console consumer.

A partir do diretório do Kafka:

```bash
cd kafka_2.13-4.3.1
```

execute:

```bash
bin/kafka-console-consumer.sh \
  --topic input \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

As mensagens publicadas pelo Producer serão exibidas no terminal.

---

## 17. Saída esperada

Uma mensagem publicada no tópico `input` terá o seguinte formato:

```json
{
  "timestamp": "2026-07-31 08:00:45",
  "machine_id": "CNC-01",
  "machine_type": "Centro de Usinagem",
  "part_id": "P0003",
  "operator": "Carlos",
  "cycle_time": 22.1,
  "temperature": 41.0,
  "vibration": 0.25,
  "power_consumption": 5.1,
  "rpm": 3490,
  "tool_number": "T01",
  "tool_wear": 13.0,
  "status": "RUNNING",
  "error_code": null,
  "production_count": 3
}
```

---

## 18. Exemplo completo de execução

### Terminal 1 — Kafka

O Kafka deve estar em execução e disponível em:

```text
localhost:9092
```

### Terminal 2 — Consumer

Execute:

```bash
cd kafka_2.13-4.3.1

bin/kafka-console-consumer.sh \
  --topic input \
  --bootstrap-server localhost:9092 \
  --from-beginning
```

### Terminal 3 — Producer

Execute:

```bash
conda activate ceaut

cd kafka-pyspark-dashboard

python producer-kafka.py
```

O Producer poderá apresentar:

```text
Iniciando Producer...
[0] CNC-01 -> P0003 (RUNNING)
[1] CNC-02 -> P0007 (RUNNING)
[2] CNC-01 -> P0008 (RUNNING)
...
Fim da publicação.
```

Enquanto isso, o Consumer exibirá as mensagens JSON recebidas do tópico `input`.

---

## 19. Fluxo completo da aplicação

O fluxo da solução pode ser representado da seguinte maneira:

```text
                    +----------------------+
                    |   logs-maquina.csv   |
                    +----------+-----------+
                               |
                               | leitura
                               v
                    +----------------------+
                    |   producer-kafka.py  |
                    |                      |
                    | csv.DictReader       |
                    | Conversão de tipos   |
                    | Serialização JSON    |
                    +----------+-----------+
                               |
                               | publish
                               v
                    +----------------------+
                    |     Apache Kafka      |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |       input          |
                    |       topic          |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |    Processamento     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |     processing       |
                    |       topic          |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |      Dashboard       |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |      dashaboard      |
                    |       topic          |
                    +----------------------+
```

---

## 20. Tópicos utilizados

A arquitetura utiliza os seguintes tópicos:

### `input`

Entrada dos dados provenientes das máquinas.

```text
Máquinas / CSV
      |
      v
    input
```

### `processing`

Tópico destinado aos dados após processamento.

```text
input
  |
  v
processing
```

### `dashaboard`

Tópico destinado aos dados utilizados pelo dashboard.

```text
processing
     |
     v
dashaboard
```

---

## 21. Verificando os tópicos existentes

Para listar os tópicos existentes no Kafka:

```bash
bin/kafka-topics.sh \
  --list \
  --bootstrap-server localhost:9092
```

A saída deverá conter os tópicos criados, por exemplo:

```text
dashaboard
input
processing
```

---

## 22. Descrever um tópico

Para obter informações sobre o tópico `input`:

```bash
bin/kafka-topics.sh \
  --describe \
  --topic input \
  --bootstrap-server localhost:9092
```

Esse comando permite verificar informações como:

- partições;
- líder;
- réplicas;
- ISR.

---

## 23. Encerramento

Ao terminar a leitura do arquivo CSV, o Producer executa:

```python
producer.close()
```

Isso encerra corretamente a conexão com o Kafka.

Em seguida, é apresentada a mensagem:

```text
Fim da publicação.
```

---

## 24. Solução de problemas

### 24.1. Kafka não está disponível

Caso apareça um erro relacionado à conexão com:

```text
localhost:9092
```

verifique se o Kafka está em execução.

Também confirme se o broker está configurado para utilizar a porta `9092`.

---

### 24.2. Tópico `input` não existe

Crie o tópico:

```bash
bin/kafka-topics.sh \
  --create \
  --topic input \
  --bootstrap-server localhost:9092
```

---

### 24.3. Arquivo CSV não encontrado

Caso seja apresentado um erro semelhante a:

```text
FileNotFoundError
```

verifique se o arquivo:

```text
logs-maquina.csv
```

está no mesmo diretório do:

```text
producer-kafka.py
```

---

### 24.4. Biblioteca Kafka não instalada

Caso apareça:

```text
ModuleNotFoundError: No module named 'kafka'
```

instale:

```bash
pip install kafka-python
```

---

### 24.5. Erro na conversão dos dados

Caso algum campo numérico contenha um valor inválido, chamadas como:

```python
float(row["temperature"])
```

ou:

```python
int(row["rpm"])
```

poderão gerar um erro.

Nesse caso, verifique o conteúdo do arquivo `logs-maquina.csv`.

---

## 25. Resumo

O Producer implementado neste projeto realiza a integração entre um arquivo CSV contendo dados de máquinas industriais e o Apache Kafka.

O fluxo principal é:

```text
CSV
 ↓
Python Producer
 ↓
Conversão dos dados
 ↓
JSON
 ↓
Kafka
 ↓
input
 ↓
Processamento
 ↓
processing
 ↓
Dashboard
```

A aplicação também simula um ambiente de produção contínua ao inserir um intervalo de 2 segundos entre as mensagens.

O tópico principal utilizado pelo Producer é:

```text
input
```

e o endereço do broker Kafka é:

```text
localhost:9092
```