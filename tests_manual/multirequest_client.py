import os
from multiprocessing import Process

import requests
import time
from time import perf_counter

NUMBER_OF_CLIENTS = 4
DATASET_VERSION_TUPLES = [
    ("BEFOLKNING_HUSHNR", "10.0.0.0"),
    ("BEFOLKNING_STATUSKODE", "5.0.0.0"),
    ("INNTEKT_PGIVINNT", "6.0.0.0"),
    ("INNTEKT_BANKINNSK", "6.0.0.0"),
]
URL = "http://localhost:10000/data/event/stream"


def send_req(dataset_name: str, version: str):
    process_id = os.getpid()
    print("process id:", process_id)
    print(f"Dataset {dataset_name} ver {version}")

    payload = {
        "version": version,
        "credentials": {"username": "", "password": ""},
        "dataStructureName": dataset_name,
        "startDate": 1,
        "stopDate": 32767,
    }

    headers = {
        "Content-Type": "application/json",
        "X-Request-ID": "my-xrequest-1",
    }
    params = {}
    start = perf_counter()
    r = requests.post(URL, json=payload, params=params, headers=headers)
    stop = perf_counter() - start

    print(
        f"process id {process_id} took {stop} with status code {r.status_code}"
    )


if __name__ == "__main__":
    processes = [
        Process(
            target=send_req,
            args=[DATASET_VERSION_TUPLES[i][0], DATASET_VERSION_TUPLES[i][1]],
        )
        for i in range(NUMBER_OF_CLIENTS)
    ]
    for i in range(NUMBER_OF_CLIENTS):
        processes[i].start()
        print(f"started process {i}")
        time.sleep(0.1)
