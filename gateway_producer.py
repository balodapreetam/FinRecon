from kafka import KafkaProducer
import json 
import random 
from datetime import datetime 
import time 

producer = KafkaProducer(
    bootstrap_servers = 'localhost:9092',
    value_serializer = lambda v: json.dumps (v).encode('utf-8')   
)

def generate_transaction(source):
    txn_id = "TXN" + str(random.randint(1000, 9999))
    user_id = random.randint(100, 110)
    amount = round(random.uniform(100, 5000), 2)
    timestamp = datetime.now().isoformat()
    status = random.choice (["SUCCESS" , "FAILED" , "PENDING"])
    return{
        "txn_id" : txn_id,
        "user_id" : user_id,
        "amount" : amount,
        "timestamp" : timestamp,
        "status" : status,
        "source" : source
    }

while True : 
    txn = generate_transaction("GATEWAY")
    producer.send("gateway-transactions", value=txn)
    print("🖨️Sent : ", txn)
    time.sleep(2)