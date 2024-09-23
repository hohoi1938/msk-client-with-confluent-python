from dotenv import load_dotenv
import os

load_dotenv()

REGION=os.getenv('REGION')
BOOTSTRAP_SERVERS=os.getenv('BOOTSTRAP_SERVERS')
SECURITY_PROTOCOL=os.getenv('SECURITY_PROTOCOL')
SASL_MECHANISM=os.getenv('SASL_MECHANISM')
TOPIC=os.getenv('TOPIC')
ACKS=os.getenv('ACKS')
GROUP_ID=os.getenv('GROUP_ID')
AUTO_OFFSET_RESET=os.getenv('AUTO_OFFSET_RESET')