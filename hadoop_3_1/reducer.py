import sys
import json

current_key = None

total_flights = 0
min_delay = float("inf")
max_delay = float("-inf")
sum_delay = 0
total_cancelled = 0
months = set()


def emit(key):
    if key is None:
        return

    avg_delay = sum_delay / total_flights if total_flights else 0
    cancellation_rate = total_cancelled / total_flights if total_flights else 0

    output = {
    "num_flights": total_flights,
    "min_arr_delay": min_delay,
    "max_arr_delay": max_delay,
    "avg_arr_delay": avg_delay,
    "cancellation_rate": cancellation_rate,
    "months_active": list(months)
    }

    print(f"{key}\t{json.dumps(output)}")


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    key_str, value_str = line.split("\t")

    value_parts = value_str.split(",")

    count = int(value_parts[0])
    arr_delay = float(value_parts[1])
    cancelled = int(value_parts[3])
    month = value_parts[4]

    # new key
    if current_key and current_key != key_str:
        emit(current_key)

        total_flights = 0
        min_delay = float("inf")
        max_delay = float("-inf")
        sum_delay = 0
        total_cancelled = 0
        months = set()

    current_key = key_str

    total_flights += count
    min_delay = min(min_delay, arr_delay)
    max_delay = max(max_delay, arr_delay)
    sum_delay += arr_delay
    total_cancelled += cancelled
    months.add(month)


emit(current_key)