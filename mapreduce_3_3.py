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

            # keys (codice compagnia, aeroporto di partenza)
            carrier = row[0]
            origin = row[1]

            # ritardo in partenza, ritardo in arrivo
            dep_delay = max(float(row[2]), 0)
            arr_delay = max(float(row[3]), 0)
            # voli cancellati
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

        # ritardo totale per aeroporto e numero di voli per aeroporto
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

            # numero voli per origin e carrier
            company_data[carrier]["flights"] += count
            # ritardo per origin e carrier
            company_data[carrier]["dep_delay"] += dep_delay
            company_data[carrier]["arr_delay"] += arr_delay
            company_data[carrier]["cancelled"] += cancelled

            # ritardo per origin
            total_airport_dep_delay += dep_delay
            # numero voli per origin
            total_airport_flights += count


        # ritardo medio in partenza per origin
        airport_avg_dep_delay = (total_airport_dep_delay / total_airport_flights)

        for carrier, stats in company_data.items():

            # ritardo medio in partenza per carrier
            avg_dep_delay = (stats["dep_delay"] / stats["flights"])
            # ritardo medio in arrivo per carrier
            avg_arr_delay = (stats["arr_delay"] / stats["flights"])
            # tasso di cancellazione
            cancellation_rate = (stats["cancelled"] / stats["flights"])

            # differenza tra ritardo medio in partenza della compagnia
            dep_delay_diff = (avg_dep_delay - airport_avg_dep_delay)

            yield origin, (
                carrier,
                stats["flights"],
                avg_dep_delay,
                avg_arr_delay,
                cancellation_rate,
                dep_delay_diff
            )

    # ===================================================================
    # STEP 2
    # ===================================================================
    # A questo punto, abbiamo, per compagnia:
    #   - numero di voli 
    #   - ritardo medio in partenza
    #   - ritardo medio in arrivo
    #   - tasso di cancellazione
    #   - differenza tra ritardi in partenza medi di compagnia e aeroporto
    #
    # Procediamo col raggruppare le compagnie per aeroporto, ordinandole
    # in base al ritardo medio in partenza (dalla migliore alla peggiore)
    # ====================================================================

    def mapper_ranking(self, origin, values):

        carrier = values[0]
        avg_dep_delay = values[2]

        yield origin, (
            carrier,
            values,
            avg_dep_delay
        )

    def reducer_ranking(self, origin, values):

        # ordinamento compagnie in base all'aeroporto
        sorted_companies = sorted(values, key=lambda x: x[2])


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