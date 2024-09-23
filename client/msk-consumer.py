import socket

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from confluent_kafka import Consumer

import config

region = config.REGION
bootstrap_server = config.BOOTSTRAP_SERVERS
security_protocol = config.SECURITY_PROTOCOL
sasl_mechanism = config.SASL_MECHANISM
group_id = config.GROUP_ID
auto_offset_reset = config.AUTO_OFFSET_RESET
topic = config.TOPIC

# MSK 認証トークンを取得
def get_oauth_cb(oauth_config):
    auth_token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
    return auth_token, expiry_ms/1000

if __name__ == '__main__':

    print("Consumer Started")
    consumer_config = {
        'bootstrap.servers': bootstrap_server,
        'security.protocol': security_protocol,
        'sasl.mechanisms': sasl_mechanism,
        'group.id': group_id,
        'auto.offset.reset': auto_offset_reset,
        'oauth_cb': get_oauth_cb,
        'client.id': socket.gethostname()
    }

    # コンシューマーインスタンスを生成
    consumer = Consumer(consumer_config)

    # トピックをサブスクライブ
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