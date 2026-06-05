#!/usr/bin/env python3
import sys
import json

current_origin = None

carrier_stats = {}
airport_total_dep = 0
airport_total_flights = 0


def reset():
    global carrier_stats, airport_total_dep, airport_total_flights
    carrier_stats = {}
    airport_total_dep = 0
    airport_total_flights = 0


def emit(origin):
    if not origin:
        return

    airport_avg = (
        airport_total_dep / airport_total_flights
        if airport_total_flights else 0
    )

    results = []

    for carrier, s in carrier_stats.items():
        if s["flights"] == 0:
            continue

        avg_dep = s["dep"] / s["flights"]
        avg_arr = s["arr"] / s["flights"]
        cancel_rate = s["cancelled"] / s["flights"]

        diff = avg_dep - airport_avg

        results.append({
            "carrier": carrier,
            "flights": s["flights"],
            "avg_dep": avg_dep,
            "avg_arr": avg_arr,
            "cancel": cancel_rate,
            "diff": diff
        })

    # ranking per delay medio di partenza
    results.sort(key=lambda x: x["avg_dep"], reverse=True)

    rank = 1
    for r in results:
        print(json.dumps({
            "origin": origin,
            "carrier": r["carrier"],
            "num_flights": r["flights"],
            "avg_dep_delay": round(r["avg_dep"], 2),
            "avg_arr_delay": round(r["avg_arr"], 2),
            "cancellation_rate": round(r["cancel"], 4),

            # 🔥 airport baseline esplicito
            "airport_avg_dep_delay": round(airport_avg, 2),

            "dep_delay_diff": round(r["diff"], 2),
            "rank": rank
        }))
        rank += 1


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    try:
        key, value = line.split("\t")

        origin, carrier = key.split("|")
        count, dep, arr, cancelled = value.split(",")

        count = int(count)
        dep = float(dep)
        arr = float(arr)
        cancelled = float(cancelled)

    except Exception:
        continue

    #cambio aeroporto
    if current_origin and current_origin != origin:
        emit(current_origin)
        reset()

    current_origin = origin

    # init carrier bucket
    if carrier not in carrier_stats:
        carrier_stats[carrier] = {
            "flights": 0,
            "dep": 0,
            "arr": 0,
            "cancelled": 0
        }

    # accumulate carrier stats
    carrier_stats[carrier]["flights"] += count
    carrier_stats[carrier]["dep"] += dep
    carrier_stats[carrier]["arr"] += arr
    carrier_stats[carrier]["cancelled"] += cancelled

    # accumulate airport totals (ALL carriers)
    airport_total_dep += dep
    airport_total_flights += count


# flush finale
emit(current_origin)