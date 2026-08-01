from kafka import KafkaConsumer

topico="basico"

consumer = KafkaConsumer(
    topico,
    bootstrap_servers="localhost:9092"
)

for mensagem in consumer:
    print(mensagem.value.decode("utf-8"))