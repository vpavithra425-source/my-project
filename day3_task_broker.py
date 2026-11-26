
# Day 3 - Understanding Task Broker

print("Welcome to MeshWeaver Task Broker")


# Function to execute tasks
def execute_task(task):

    operation = task["operation"]
    num1 = task["num1"]
    num2 = task["num2"]

    if operation == "addition":
        return num1 + num2

    elif operation == "subtraction":
        return num1 - num2

    elif operation == "multiplication":
        return num1 * num2

    else:
        return "Unsupported operation"


# Task received from sender
task = {
    "task_id": 3,
    "operation": "multiplication",
    "num1": 10,
    "num2": 5
}

# Execute task
print("Task Received:", task)

result = execute_task(task)

# Display result
print("Task Executed Successfully")
print("Result:", result)
