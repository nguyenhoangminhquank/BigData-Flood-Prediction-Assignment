import json
from confluent_kafka import Producer, Consumer

BOOTSTRAP = "localhost:9094"
TOPIC = "sandbox"

def on_delivery(err, msg):
    if err:
        print("Lỗi gửi:", err)
    else:
        print(f"Đã gửi key={msg.key().decode()} -> partition {msg.partition()}, offset {msg.offset()}")


producer =Producer({"bootstrap.servers": BOOTSTRAP})
for station, day in [("ASIA_0001", "2022-08-15"), ("ASIA_0009", "2022-08-15"), ("ASIA_0001", "2022-08-16")]:
    value={"station_id": station, "event_time": f"{day}T00:00:00Z"}
    producer.produce(TOPIC, key=station, value=json.dumps(value), on_delivery=on_delivery)
producer.flush()

consumer=Consumer({
    "bootstrap.servers": BOOTSTRAP,
    "group.id": "smoke_test",
    "auto.offset.reset":"earliest",
})
consumer.subscribe([TOPIC])
received=0
while(received<3):
    msg=consumer.poll(5.0)
    if msg is None:
        print("Hết 5 giây không nhân được bản tin")
        break
    if msg.error():
        print("Lỗi đọc:", msg.erorr())
        continue
    print(F"Nhận được partition {msg.partition()} key={msg.key().decode()} value={json.loads(msg.value())}")
    received+=1
consumer.close()