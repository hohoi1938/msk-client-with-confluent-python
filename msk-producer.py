import configparser
import socket
from random import choice

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Producer

# ConfigParser オブジェクトを生成
config = configparser.ConfigParser()

#設定ファイル読み込み
config.read('./config.ini')

# MSK 認証トークンを取得
def get_oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(config['DEFAULT']['REGION'])
    return auth_token, expiry_ms/1000

if __name__ == '__main__':
    print("Producer Started")

    producer_config = {
        'bootstrap.servers': config['DEFAULT']['BOOTSTRAP_SERVERS'],
        'security.protocol': config['DEFAULT']['SECURITY_PROTOCOL'],
        'sasl.mechanisms':   config['DEFAULT']['SASL_MECHANISM'],
        'acks': config['PRODUCER']['ACKS'],
        'oauth_cb': get_oauth_cb,
        'client.id': socket.gethostname()
    }

    # プロデューサーインスタンスを生成
    producer = Producer(producer_config)

    # コールバック関数定義  
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # 書き込み先のトピックを指定
    topic = config['DEFAULT']['TOPIC']

    # サンプルメッセージデータ定義
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']

    count = 0
    # トピックにメッセージを書き込み
    for _ in range(10):
        user_id = choice(user_ids)
        product = choice(products)
        producer.produce(topic, product, user_id, callback=delivery_callback)
        count += 1

    producer.poll(10000)
    producer.flush()
    print("Produced Messages")