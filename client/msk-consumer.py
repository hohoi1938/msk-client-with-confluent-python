import configparser
import socket

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Consumer

# ConfigParser オブジェクトを生成
config = configparser.ConfigParser()

#設定ファイル読み込み
config.read('../config/config.ini')

# MSK 認証トークンを取得
def get_oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(config['DEFAULT']['REGION'])
    return auth_token, expiry_ms/1000

if __name__ == '__main__':

    print("Consumer Started")
    consumer_config = {
        'bootstrap.servers': config['DEFAULT']['BOOTSTRAP_SERVERS'],
        'security.protocol': config['DEFAULT']['SECURITY_PROTOCOL'],
        'sasl.mechanisms':   config['DEFAULT']['SASL_MECHANISM'],
        'group.id':          config['CONSUMER']['GROUP_ID'],
        'auto.offset.reset': config['CONSUMER']['AUTO_OFFSET_RESET'],
        'oauth_cb': get_oauth_cb,
        'client.id': socket.gethostname()
    }

    # コンシューマーインスタンスを生成
    consumer = Consumer(consumer_config)

    # トピックをサブスクライブ
    topic = config['DEFAULT']['TOPIC']
    consumer.subscribe([topic])

    # メッセージをポーリング
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                    topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # コンシューマーをグレースフルクローズ
        consumer.close()