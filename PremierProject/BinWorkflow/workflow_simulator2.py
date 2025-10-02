import json
import random
import paho.mqtt.client as mqtt

BROKER = "localhost"
PORT = 1883
TOPIC_ACK = "factory/bin_flow/ack"

client = mqtt.Client()
client.connect(BROKER, PORT, 60)
client.loop_start()

print("Simulator ready. Type task names to send acknowledgement with simulated data:")
print("Tasks: bin_registration, job_allocation, verification, job_closeout, dispatch")
print("Type 'exit' to quit.")

# Keep track of how many jobs were simulated
job_count = 0
task_order = ["bin_registration", "job_allocation", "verification", "job_closeout", "dispatch"]

while True:
    task = input("Enter completed task: ").strip()
    if task.lower() == "exit":
        break
    if task not in task_order:
        print("Invalid task name. Try again.")
        continue
    
    # Simulate task data
    data = {
        "uid": f"BIN{1000 + job_count}",
        "tare_weight": round(1 + task_order.index(task), 2),
        "gross_weight": round(10 + task_order.index(task), 2),
        "target_count": 10 + task_order.index(task),
        "count_ok": True
    }

    payload = {"task": task, "data": data}
    client.publish(TOPIC_ACK, json.dumps(payload))
    print(f"Acknowledgement sent for task: {task} with data: {data}")

    # Increment job count when dispatch is completed
    if task == "dispatch":
        job_count += 1

client.loop_stop()
client.disconnect()
