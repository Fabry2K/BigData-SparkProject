# mapreduce_3_3.py

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv


class FlightAnalysis3_3(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_stats,
                reducer=self.reducer_stats
            ),
            MRStep(
                mapper=self.mapper_ranking,
                reducer=self.reducer_ranking
            )
        ]

    # ==========================================================
    # STEP 1
    # ==========================================================

    def mapper_stats(self, _, line):

        reader = csv.reader([line])

        try:
            row = next(reader)

            # skip header
            if row[0] == "op_unique_carrier":
                return

            carrier = row[0]
            origin = row[1]

            dep_delay = max(float(row[2]), 0)
            arr_delay = max(float(row[3]), 0)

            cancelled = float(row[4])

            yield origin, (
                carrier,
                dep_delay,
                arr_delay,
                cancelled,
                1
            )

        except:
            pass

    def reducer_stats(self, origin, values):

        company_data = {}

        total_airport_dep_delay = 0
        total_airport_flights = 0

        for carrier, dep_delay, arr_delay, cancelled, count in values:

            if carrier not in company_data:
                company_data[carrier] = {
                    "flights": 0,
                    "dep_delay": 0,
                    "arr_delay": 0,
                    "cancelled": 0
                }

            company_data[carrier]["flights"] += count
            company_data[carrier]["dep_delay"] += dep_delay
            company_data[carrier]["arr_delay"] += arr_delay
            company_data[carrier]["cancelled"] += cancelled

            total_airport_dep_delay += dep_delay
            total_airport_flights += count

        airport_avg_dep_delay = (
            total_airport_dep_delay / total_airport_flights
        )

        for carrier, stats in company_data.items():

            avg_dep_delay = (
                stats["dep_delay"] / stats["flights"]
            )

            avg_arr_delay = (
                stats["arr_delay"] / stats["flights"]
            )

            cancellation_rate = (
                stats["cancelled"] / stats["flights"]
            )

            dep_delay_diff = (
                avg_dep_delay - airport_avg_dep_delay
            )

            yield origin, (
                carrier,
                stats["flights"],
                avg_dep_delay,
                avg_arr_delay,
                cancellation_rate,
                dep_delay_diff
            )

    # ==========================================================
    # STEP 2
    # ==========================================================

    def mapper_ranking(self, origin, values):

        carrier = values[0]
        avg_dep_delay = values[2]

        yield origin, (
            carrier,
            values,
            avg_dep_delay
        )

    def reducer_ranking(self, origin, values):

        sorted_companies = sorted(
            values,
            key=lambda x: x[2]
        )

        rank = 1

        for carrier, values, _ in sorted_companies:

            yield None, {
                "origin": origin,
                "carrier": carrier,
                "num_flights": values[1],
                "avg_dep_delay": round(values[2], 2),
                "avg_arr_delay": round(values[3], 2),
                "cancellation_rate": round(values[4], 4),
                "dep_delay_diff": round(values[5], 2),
                "rank": rank
            }

            rank += 1


if __name__ == "__main__":
    FlightAnalysis3_3.run()