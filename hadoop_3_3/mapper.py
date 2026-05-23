import sys
import csv

#!/usr/bin/env python3
for line in sys.stdin:

    line = line.strip()
    if not line:
        continue

    try:
        row = next(csv.reader([line]))

        if row[0] == "op_unique_carrier":
            continue

        carrier = row[0]
        origin = row[1]

        dep_delay = max(float(row[2]) if row[2] else 0, 0)
        arr_delay = max(float(row[3]) if row[3] else 0, 0)
        cancelled = float(row[4] if row[4] else 0)

        # KEY: origin + carrier
        key = f"{origin}|{carrier}"

        value = f"1,{dep_delay},{arr_delay},{cancelled}"

        print(f"{key}\t{value}")

    except Exception as e:
        sys.stderr.write(str(e) + "\n")
        continue