# -------------------------------------------------------------
# Importação das bibliotecas
# -------------------------------------------------------------

# Classe responsável por consumir mensagens do Kafka.
from kafka import KafkaConsumer

# Classe utilizada para capturar exceções específicas do Kafka.
from kafka.errors import KafkaError

# Biblioteca utilizada para converter JSON em objetos Python.
import json


# -------------------------------------------------------------
# Configurações do Kafka
# -------------------------------------------------------------

# Endereço do broker Kafka.
BOOTSTRAP_SERVERS = "localhost:9092"

# Nome do tópico que será monitorado.
TOPIC = "basico"


# -------------------------------------------------------------
# Criação do Consumer
# -------------------------------------------------------------
try:

    consumer = KafkaConsumer(

        # Nome do tópico
        TOPIC,

        # Endereço do broker
        bootstrap_servers=BOOTSTRAP_SERVERS,

        # Caso seja a primeira execução,
        # lê todas as mensagens desde o início do tópico.
        auto_offset_reset="earliest",

        # Ativa confirmação automática dos offsets.
        enable_auto_commit=True,

        # Intervalo (ms) para gravação automática do offset.
        auto_commit_interval_ms=1000,

        # Converte automaticamente o JSON recebido
        # para um dicionário Python.
        value_deserializer=lambda m: json.loads(m.decode("utf-8"))
    )

    print(f"Conectado ao Kafka ({BOOTSTRAP_SERVERS})")
    print(f"Consumindo mensagens do tópico '{TOPIC}'...\n")

except Exception as e:

    print("Erro ao conectar ao Kafka:")
    print(e)

    exit(1)


# -------------------------------------------------------------
# Loop principal
# -------------------------------------------------------------
try:

    # O Consumer permanece aguardando novas mensagens.
    for message in consumer:

        # -----------------------------------------------------
        # Informações do Kafka
        # -----------------------------------------------------

        print("=" * 60)

        print("Mensagem recebida!")

        print(f"Tópico     : {message.topic}")
        print(f"Partição   : {message.partition}")
        print(f"Offset     : {message.offset}")

        # -----------------------------------------------------
        # Conteúdo da mensagem
        # -----------------------------------------------------

        dados = message.value

        print("\nConteúdo da mensagem:")

        print(f"ID.................: {dados['id']}")
        print(f"Máquina............: {dados['machine_id']}")
        print(f"Mensagem...........: {dados['mensagem']}")
        print(f"Timestamp..........: {dados['timestamp']}")

        print("=" * 60)
        print()


# -------------------------------------------------------------
# Tratamento de erros do Kafka
# -------------------------------------------------------------
except KafkaError as e:

    print("\nErro do Kafka:")
    print(e)


# -------------------------------------------------------------
# O usuário pressionou CTRL+C
# -------------------------------------------------------------
except KeyboardInterrupt:

    print("\nEncerrando Consumer...")


# -------------------------------------------------------------
# Outros erros
# -------------------------------------------------------------
except Exception as e:

    print("\nErro inesperado:")
    print(e)


# -------------------------------------------------------------
# Finalização
# -------------------------------------------------------------
finally:

    consumer.close()

    print("Consumer finalizado.")