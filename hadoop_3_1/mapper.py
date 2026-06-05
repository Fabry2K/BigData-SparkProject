import sys
import csv

#!/usr/bin/env python3
for line in sys.stdin:

    line = line.strip()

    if not line or line.startswith("fl_date"):
        continue

    try:
        row = next(csv.reader([line]))

        if len(row) < 9:
            continue

        carrier = row[2]
        origin = row[4]
        # destination = row[5]
        month = row[1]

        dep_delay = float(row[6]) if row[6] else 0
        arr_delay = float(row[7]) if row[7] else 0
        cancelled = int(row[8]) if row[8] else 0

        dep_delay = max(dep_delay, 0)
        arr_delay = max(arr_delay, 0)

        key = f"{carrier}|{origin}"
        value = f"1,{dep_delay},{arr_delay},{cancelled},{month}"

        print(f"{key}\t{value}")

    except Exception as e:
        sys.stderr.write(str(e) + "\n")
        continue