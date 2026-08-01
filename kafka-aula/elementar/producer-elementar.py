from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers="localhost:9092")

topico='basico'

while True:
    mensagem = input()

    if mensagem.lower() == "sair":
        break

    producer.send(topico, mensagem.encode("utf-8"))
    producer.flush()

producer.close()