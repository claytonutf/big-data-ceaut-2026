# -------------------------------------------------------------
# Importação das bibliotecas
# -------------------------------------------------------------

# Classe responsável por publicar mensagens em tópicos Kafka.
from kafka import KafkaProducer

# Classe utilizada para capturar exceções específicas do Kafka.
from kafka.errors import KafkaError

# Biblioteca para converter objetos Python em JSON.
import json

# Biblioteca para manipulação de datas e horários.
import time


# -------------------------------------------------------------
# Configurações do Kafka
# -------------------------------------------------------------

# Endereço do broker Kafka.
# Caso o broker esteja em outra máquina,
# basta alterar este endereço.
BOOTSTRAP_SERVERS = "localhost:9092"

# Nome do tópico onde as mensagens serão publicadas.
TOPIC = "basico"


# -------------------------------------------------------------
# Criação do Producer
# -------------------------------------------------------------
try:

    # Cria o Producer responsável por enviar mensagens ao Kafka.
    producer = KafkaProducer(

        # Endereço do broker.
        bootstrap_servers=BOOTSTRAP_SERVERS,

        # Converte automaticamente um objeto Python
        # em JSON antes do envio.
        #
        # Exemplo:
        #
        # {"id":1}
        #
        # torna-se
        #
        # b'{"id":1}'
        #
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    print(f"Conectado ao Kafka ({BOOTSTRAP_SERVERS})")
    print(f"Tópico: {TOPIC}\n")

# Caso ocorra algum erro de conexão
except Exception as e:

    print("Erro ao conectar ao Kafka:")
    print(e)

    # Encerra a aplicação
    exit(1)


# -------------------------------------------------------------
# Variáveis utilizadas durante a execução
# -------------------------------------------------------------

# Identificador sequencial das mensagens.
contador = 1

# Nome da máquina que está enviando os eventos.
# Em uma fábrica poderiam existir várias máquinas.
maquina = "usinagem-1"


# -------------------------------------------------------------
# Interface simples no terminal
# -------------------------------------------------------------
print("=" * 50)
print("Producer Kafka")
print("Digite uma mensagem e pressione ENTER.")
print("Digite 'sair' para encerrar.")
print("=" * 50)


# -------------------------------------------------------------
# Loop principal
# -------------------------------------------------------------
try:

    while True:

        # Aguarda o usuário digitar uma mensagem.
        texto = input("Mensagem: ")

        # Caso o usuário deseje encerrar,
        # finaliza o loop.
        if texto.lower() in ("sair", "exit", "quit"):
            break


        # -----------------------------------------------------
        # Cria a mensagem que será enviada ao Kafka.
        # -----------------------------------------------------
        mensagem = {

            # Identificador sequencial.
            "id": contador,

            # Máquina responsável pelo envio.
            "machine_id": maquina,

            # Texto digitado pelo usuário.
            "mensagem": texto,

            # Data e hora atuais.
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }


        try:

            # -------------------------------------------------
            # Publica a mensagem no tópico Kafka.
            #
            # O método send() retorna um objeto Future,
            # permitindo aguardar a confirmação do broker.
            # -------------------------------------------------
            future = producer.send(
                TOPIC,
                value=mensagem
            )


            # -------------------------------------------------
            # Aguarda a confirmação do broker.
            #
            # Se nenhuma exceção for lançada,
            # significa que a mensagem foi gravada
            # com sucesso no Kafka.
            # -------------------------------------------------
            metadata = future.get(timeout=10)


            # -------------------------------------------------
            # Garante que todas as mensagens pendentes
            # sejam transmitidas.
            #
            # Em aplicações de alto desempenho normalmente
            # o flush() é executado apenas periodicamente,
            # pois o Kafka realiza envio em lote (batch).
            # -------------------------------------------------
            producer.flush()


            # -------------------------------------------------
            # Exibe informações da mensagem gravada.
            # -------------------------------------------------
            print("\nMensagem enviada com sucesso!")

            print(f"Tópico    : {metadata.topic}")
            print(f"Partição  : {metadata.partition}")
            print(f"Offset    : {metadata.offset}")

            print(f"Conteúdo  : {mensagem}\n")


            # Atualiza o identificador da próxima mensagem.
            contador += 1


        # -----------------------------------------------------
        # Tratamento de erros específicos do Kafka
        # -----------------------------------------------------
        except KafkaError as e:

            print("\nErro do Kafka:")
            print(e)


        # -----------------------------------------------------
        # Tratamento de quaisquer outros erros
        # -----------------------------------------------------
        except Exception as e:

            print("\nErro inesperado:")
            print(e)


# -------------------------------------------------------------
# O usuário pressionou CTRL+C
# -------------------------------------------------------------
except KeyboardInterrupt:

    print("\nEncerrando Producer...")


# -------------------------------------------------------------
# Finalização da aplicação
# -------------------------------------------------------------
finally:

    # Fecha a conexão com o Kafka.
    producer.close()

    print("Producer finalizado.")