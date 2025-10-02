import json
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC_ACK = "factory/bin_flow/ack"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)
client.loop_start()

print("Simulator ready. Type task names to send acknowledgement:")
print("Tasks: bin_registration, job_allocation, verification, job_closeout, dispatch")
print("Type 'exit' to quit.")

while True:
    task = input("Enter completed task: ").strip()
    if task.lower() == "exit":
        break
    if task not in ["bin_registration", "job_allocation", "verification", "job_closeout", "dispatch"]:
        print("Invalid task name. Try again.")
        continue
    
    payload = {"task": task}
    client.publish(TOPIC_ACK, json.dumps(payload))
    print(f"Acknowledgement sent for task: {task}")

client.loop_stop()
client.disconnect()
