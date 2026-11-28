
# MeshWeaver – Internship Daily Work Log

**Project:** MeshWeaver – Zero-Dependency P2P Async Task Broker

**Internship Duration:** 20 Days

## My Responsibilities

1. Receiving tasks, executing tasks, and returning results.
2. Implementing the Gossip Protocol.
3. Sharing CPU and RAM information between peers every 5 seconds.

---

## Day 1 – Project Setup and Git Configuration

**Status:** Completed

**Work Performed:**
- Set up the Python virtual environment.
- Activated the virtual environment.
- Explored the Git repository and branch configuration.
- Created a `.gitignore` file.
- Added Python environment and cache files to `.gitignore`.
- Committed and pushed the changes to my GitHub branch.

**Learning Outcome:**

Learned how to create and activate a Python virtual environment, manage Git branches, ignore unnecessary files, and push code changes to a remote GitHub repository.

**Git Commit:** `03e407b`

---

## Daily Progress Template

### Day [Number] – [Topic]

**Status:**

**Objective:**

**Files Created or Modified:**

**Work Performed:**

**Code Implemented:**

**Testing and Output:**

**Learning Outcome:**

**Next Day's Plan:**

**Git Commit ID:**

---

## Day 2 - Python Functions and Modular Programming

**Status:** Completed

**Objective:**
Learn Python functions, parameters, return values, and reusable code.

**Files Created or Modified:**
- `day2_practice.py`
- `WORK_LOG.md`

**Work Performed:**
- Created a new Python practice file.
- Practiced defining and calling functions.
- Learned how to pass parameters to functions.
- Used return statements to return values.
- Created a welcome message function.
- Implemented an addition function using two parameters.
- Executed the Python program successfully.

**Testing and Output:**
Successfully executed `day2_practice.py`. Verified the welcome message and addition result of 30.

**Learning Outcome:**
Learned how Python functions help organize code into reusable components. Understood parameters, function calls, and return values. These concepts will help me develop modular components for the MeshWeaver task broker.

**Next Day's Plan:**
Understand task broker architecture and asynchronous programming.

**Git Commit ID:** db3875f

---

## Day 3 - Task Broker Basics and Task Execution

**Status:** Completed

**Objective:**
Understand the basic Task Broker workflow and practice receiving, processing, and executing tasks using Python.

**Files Created or Modified:**
- `day3_task_broker.py`
- `WORK_LOG.md`

**Work Performed:**
- Created a Python practice program to understand the Task Broker workflow.
- Represented tasks using Python dictionaries.
- Created an `execute_task()` function to process tasks.
- Implemented addition, subtraction, and multiplication operations.
- Used conditional statements to identify the requested operation.
- Used return statements to send the calculated result back.
- Executed and tested all three operations successfully.

**Testing and Output:**
Successfully tested addition (10 + 20 = 30), subtraction (20 - 10 = 10), and multiplication (10 * 5 = 50).

**Learning Outcome:**
Learned how a basic Task Broker receives task information, identifies an operation, executes it using a function, and returns the result. Improved understanding of Python dictionaries, functions, parameters, conditional statements, and return values.

**Next Day's Plan:**
Learn asynchronous programming concepts and understand how tasks can be managed in an asynchronous system.

**Git Commit ID:** c966da2  
---

## Day 4 - Asynchronous Task Processing and Multiple Workers

**Status:** Completed

**Objective:**
Understand asynchronous programming, task queues, multiple workers, and error handling using Python asyncio.

**Files Created or Modified:**
- `day4_async_practice.py`
- `day4_async_task_broker.py`
- `WORK_LOG.md`

**Work Performed:**
- Practiced synchronous and asynchronous programming in Python.
- Learned how to use `async`, `await`, and `asyncio.run()`.
- Implemented multiple coroutines using `asyncio.gather()`.
- Measured asynchronous execution time using `time.perf_counter()`.
- Created a Mini Async Task Broker to process multiple tasks.
- Implemented input validation and exception handling.
- Used `asyncio.Queue()` to manage incoming tasks.
- Created two asynchronous workers using `asyncio.create_task()`.
- Implemented task completion tracking using `task_done()` and `queue.join()`.

**Testing and Output:**
Successfully executed the asynchronous task broker with two workers. Tested addition, subtraction, multiplication, division by zero, unsupported operations, invalid input, and missing task fields. Both workers processed tasks successfully, and all seven task responses were collected without crashing the program.

**Learning Outcome:**
Learned how asynchronous programming allows multiple coroutines to make progress concurrently. Understood how asyncio queues distribute tasks among available workers and how exception handling prevents invalid tasks from crashing the program. Improved understanding of asynchronous task execution, worker management, and result collection.

**Next Day's Plan:**
Improve task result handling and study how task processing can be organized for the MeshWeaver project.

**Git Commit ID:** 0aaf8a2                                    


---

## Day 5 - MeshWeaver Client-Server Communication and Task Execution

**Status:** Completed

**Objective:**
Understand the actual MeshWeaver UDP client-server architecture and implement sequential task submission with result handling and error reporting.

**Files Created or Modified:**
- `project/MeshWeaver/client.py`
- `WORK_LOG.md`

**Work Performed:**
- Explored the actual MeshWeaver client and server implementation.
- Understood how UDP communication transfers serialized task data between the client and server.
- Updated the client to submit multiple arithmetic tasks sequentially.
- Implemented addition, subtraction, multiplication, and division functions.
- Processed server responses and tracked successful and failed tasks.
- Tested exception handling by submitting a division-by-zero task.
- Verified that the server returned an error response without stopping its execution.

**Testing and Output:**
Successfully tested four tasks through the local MeshWeaver UDP client-server setup. Addition returned 30, subtraction returned 30, and multiplication returned 50. Division by zero returned an error message. The client correctly reported three successful tasks and one failed task.

**Learning Outcome:**
Learned how the actual MeshWeaver client sends serialized tasks to a UDP server and receives execution results. Understood sequential task submission, response processing, and error reporting. This test verified basic local client-server task execution. Multi-peer communication, Gossip Protocol, and CPU/RAM sharing are still pending.

**Next Day's Plan:**
Study peer discovery and implement the basic Gossip Protocol for sharing information between MeshWeaver peers.

**Git Commit ID:** 659de0e

---

## Day 6 - Gossip Protocol and Peer Resource Sharing

**Status:** Completed

**Objective:**
Implement a basic asynchronous Gossip Protocol to exchange peer information and share CPU and RAM usage between nodes every five seconds.

**Files Created or Modified:**
- `project/MeshWeaver/gossip.py`
- `WORK_LOG.md`

**Work Performed:**
- Created a separate Gossip Protocol implementation using Python asyncio and UDP communication.
- Implemented CPU usage monitoring using the Windows GetSystemTimes API through ctypes.
- Implemented RAM monitoring using the Windows GlobalMemoryStatusEx API.
- Used JSON messages to exchange peer information and resource statistics.
- Implemented peer discovery using configured seed peers.
- Configured the Gossip loop to exchange information every five seconds.
- Started two local Gossip nodes using different UDP ports.
- Verified that both nodes could send and receive peer CPU and RAM information.

**Testing and Output:**
Successfully started two Gossip nodes on localhost using ports 10000 and 10001. Verified bidirectional communication between both nodes. Both nodes received peer information, including CPU and RAM usage, through UDP messages at five-second intervals. CPU and RAM monitoring also worked successfully without installing external libraries.

**Learning Outcome:**
Learned the basics of the Gossip Protocol, UDP-based peer communication, asynchronous networking, JSON message exchange, and system resource monitoring using Windows APIs. Understood how peers can share their resource information periodically. The current implementation is a locally tested prototype and still needs further validation and integration with the main MeshWeaver architecture.

**Next Day's Plan:**
Review the Gossip implementation, improve peer information validation, and study how to integrate resource sharing with the existing MeshWeaver architecture.

**Git Commit ID:** Pending

