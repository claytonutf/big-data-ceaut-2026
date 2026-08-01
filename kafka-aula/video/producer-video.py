# Biblioteca OpenCV utilizada para leitura do vídeo
# e compressão dos frames em formato JPEG.
import cv2

# Biblioteca responsável por converter os bytes do JPEG
# em uma String Base64, facilitando o envio via JSON.
import base64

# Biblioteca para serialização do dicionário Python em JSON.
import json

# Biblioteca utilizada para obter o timestamp e controlar
# a velocidade de envio dos frames.
import time

# Cliente Producer do Apache Kafka
from kafka import KafkaProducer


# -------------------------------------------------------------
# Arquivo de vídeo que será transmitido
# -------------------------------------------------------------
VIDEO = "cars.mp4"

# Nome do tópico Kafka
TOPIC = "video"


# -------------------------------------------------------------
# Criação do Producer
# -------------------------------------------------------------
producer = KafkaProducer(

    # Endereço do broker Kafka
    bootstrap_servers="localhost:9092",

    # Serializa automaticamente os objetos Python para JSON
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)


# -------------------------------------------------------------
# Abre o arquivo de vídeo utilizando OpenCV
# -------------------------------------------------------------
cap = cv2.VideoCapture(VIDEO)


# -------------------------------------------------------------
# Obtém o FPS (Frames por Segundo) original do vídeo.
# Essa informação será utilizada para manter a mesma
# velocidade durante a transmissão.
# -------------------------------------------------------------
fps = cap.get(cv2.CAP_PROP_FPS)


# Caso o vídeo não informe o FPS corretamente,
# utiliza 30 FPS como valor padrão.
if fps <= 0:
    fps = 30


# Intervalo de tempo entre dois frames.
# Exemplo:
# FPS = 30
# delay = 1 / 30 = 0,033 segundos
delay = 1 / fps


# Contador de frames enviados
frame_number = 0

print("Transmitindo vídeo...")


# -------------------------------------------------------------
# Loop principal
# -------------------------------------------------------------
while True:

    # Lê o próximo frame do vídeo.
    #
    # ret   -> True se conseguiu ler.
    # frame -> imagem correspondente.
    ret, frame = cap.read()

    # Se não houver mais frames,
    # encerra a transmissão.
    if not ret:
        break


    # ---------------------------------------------------------
    # Compressão da imagem
    # ---------------------------------------------------------
    #
    # O frame é convertido para JPEG.
    #
    # A qualidade varia de 0 a 100.
    #
    # Quanto menor a qualidade,
    # menor será o tamanho da mensagem enviada ao Kafka.
    #
    _, buffer = cv2.imencode(
        ".jpg",
        frame,
        [cv2.IMWRITE_JPEG_QUALITY, 80]
    )


    # ---------------------------------------------------------
    # Conversão do JPEG para Base64
    # ---------------------------------------------------------
    #
    # O Kafka transportará um JSON.
    # Como JSON não suporta bytes diretamente,
    # o JPEG é convertido para uma String Base64.
    #
    frame64 = base64.b64encode(buffer).decode()


    # ---------------------------------------------------------
    # Monta a mensagem que será enviada ao Kafka
    # ---------------------------------------------------------
    mensagem = {

        # Número sequencial do frame
        "frame": frame_number,

        # Timestamp UNIX
        "timestamp": time.time(),

        # Imagem codificada em Base64
        "image": frame64
    }


    # ---------------------------------------------------------
    # Publica a mensagem no tópico Kafka
    # ---------------------------------------------------------
    producer.send(TOPIC, mensagem)


    # Aguarda a confirmação do envio.
    #
    # Em aplicações de alto desempenho normalmente o flush()
    # é executado apenas ao final do vídeo ou em intervalos
    # maiores para aproveitar o mecanismo de batch do Kafka.
    producer.flush()


    # Atualiza o contador de frames
    frame_number += 1


    # Aguarda o tempo correspondente ao FPS do vídeo.
    #
    # Isso faz com que a transmissão ocorra praticamente
    # na mesma velocidade do vídeo original.
    time.sleep(delay)


# -------------------------------------------------------------
# Libera o arquivo de vídeo
# -------------------------------------------------------------
cap.release()


# -------------------------------------------------------------
# Fecha o Producer Kafka
# -------------------------------------------------------------
producer.close()

print("Fim da transmissão.")