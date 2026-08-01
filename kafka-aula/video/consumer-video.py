# Biblioteca para converter Base64 em bytes
import base64

# Biblioteca para manipulação de arquivos JSON
import json

# OpenCV para reconstrução e exibição das imagens
import cv2

# Biblioteca NumPy utilizada para converter os bytes em um vetor de pixels
import numpy as np

# Cliente Consumer do Apache Kafka
from kafka import KafkaConsumer


# ------------------------------------------------------------------
# Cria um consumidor Kafka
# ------------------------------------------------------------------
consumer = KafkaConsumer(

    # Nome do tópico que será monitorado
    "video",

    # Endereço do broker Kafka
    bootstrap_servers="localhost:9092",

    # Caso seja a primeira execução, inicia a leitura desde o início
    # das mensagens existentes no tópico.
    auto_offset_reset="earliest",

    # Converte automaticamente o JSON recebido em um dicionário Python
    value_deserializer=lambda m: json.loads(m.decode())
)

print("Recebendo vídeo...")


# ------------------------------------------------------------------
# Loop infinito que aguarda novas mensagens do Kafka
# ------------------------------------------------------------------
for message in consumer:

    # message.value contém o objeto JSON enviado pelo Producer
    data = message.value

    # Exemplo do conteúdo recebido:
    #
    # {
    #     "frame": 15,
    #     "timestamp": 1753990212,
    #     "image": "<base64>"
    # }

    # Recupera a imagem codificada em Base64 e converte novamente
    # para uma sequência de bytes (JPEG).
    jpg = base64.b64decode(data["image"])

    # Converte os bytes em um vetor NumPy.
    # O OpenCV utiliza esse vetor para reconstruir a imagem.
    np_array = np.frombuffer(jpg, dtype=np.uint8)

    # Reconstrói o frame colorido a partir do JPEG.
    frame = cv2.imdecode(np_array, cv2.IMREAD_COLOR)

    # Exibe o frame em uma janela.
    # Como os frames chegam continuamente, a sequência gera um vídeo.
    cv2.imshow("Video Kafka", frame)

    # Aguarda 1 milissegundo por uma tecla.
    #
    # Caso o usuário pressione a tecla 'q',
    # encerra a reprodução do vídeo.
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break


# ------------------------------------------------------------------
# Fecha a conexão com o Kafka
# ------------------------------------------------------------------
consumer.close()


# ------------------------------------------------------------------
# Fecha todas as janelas abertas pelo OpenCV
# ------------------------------------------------------------------
cv2.destroyAllWindows()