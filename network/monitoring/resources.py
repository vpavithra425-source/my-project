import psutil


def get_resource_usage():

    cpu = psutil.cpu_percent(interval=1)
    ram = psutil.virtual_memory().percent

    return {
        "cpu": cpu,
        "ram": ram
    }


if __name__ == "__main__":

    usage = get_resource_usage()

    print("CPU Usage:", usage["cpu"], "%")
    print("RAM Usage:", usage["ram"], "%")