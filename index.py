from confluent_kafka import Producer , Consumer , KafkaError , KafkaException
import socket
from bd import BD  # Importe a classe BD do arquivo bd.py


config = {'bootstrap.servers': 'pkc-ldjyd.southamerica-east1.gcp.confluent.cloud:9092',
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'WBLERTUYSKBLR46Z',
        'sasl.password': 't35H7Qk/5So/vPxoo9qDCxUknjJ8QxJlNS8krcE26WHw5/z90JZFebPOaqz4n+lr',
        'client.id': socket.gethostname(),
        # Best practice for higher availability in librdkafka clients prior to 1.7
        #'session.timeout.ms':45000, 
        # Required connection configs for Confluent Cloud Schema Registry
        #'schema.registry.url':'https://psrc-12dqx5.southamerica-east1.gcp.confluent.cloud',
        #'basic.auth.credentials.source':'USER_INFO',
        #'basic.auth.user.info':'DS64SPS35AMXM7HT:lpwe0HFupfXx8eQqOIy1gCisxcJOi1Si2y0f9DAUg2QMr+mcUMWQ6tG5viYfiaPr'
}


def produce(topic, config):
    producer = Producer(config)
    producer.produce(topic, key="teste 23", value="Teste produção de mensagem em topico 14 ")
    producer.flush()


def consumer(topic, config):
    # Adicione 'group.id' ao config se ainda não estiver definido
    if 'group.id' not in config:
        config['group.id'] = 'my_consumer_group'

    consumer = Consumer(config)
    consumer.subscribe([topic]) 

    bd = BD()  # Instancie a classe BD

    teste = 0 

    print('teste 01')
    try:
        while True:
            teste = teste+1
            print(f"Mensagem numero: {teste}")
          
            msg = consumer.poll(1.0)
            
            if msg is None:
                print("Nenhuma mensagem recebida neste intervalo de tempo.")
                continue
            
            # Verifique se a mensagem não é nula antes de tentar acessar o valor
            if msg.value() is not None:
                mensagem = msg.value().decode('utf-8')
                print(f"Mensagem recebida: {mensagem}")
                # Insira a mensagem no banco de dados
                bd.inserir_mensagem(mensagem)
            else:
                print("Mensagem vazia recebida.")
                
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


def consumerAll(topic, config):
    # Adicione 'group.id' ao config se ainda não estiver definido
    if 'group.id' not in config:
        config['group.id'] = 'my_consumer_group'

    # Defina 'auto.offset.reset' como 'earliest' para ler todas as mensagens do início
    config['auto.offset.reset'] = 'earliest'

    consumer = Consumer(config)
    consumer.subscribe([topic]) 
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                print("Nenhuma mensagem recebida neste intervalo de tempo.")
                break
            
            # Verifique se a mensagem não é nula antes de tentar acessar o valor
            if msg.value() is not None:
                print(f"Mensagem recebida: {msg.value().decode('utf-8')}")
            else:
                print("Mensagem vazia recebida.")
                
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


def main():
    topic = "topic_0"
    #produce(topic, config)
    #consumerAll(topic, config)
    consumer(topic, config)

if __name__ == "__main__":
    main()