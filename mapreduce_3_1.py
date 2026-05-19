from mrjob.job import MRJob
from mrjob.step import MRStep

import csv
from io import StringIO


class FlightAnalysis(MRJob):

    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_flights,
                reducer=self.reducer_statistics
            )
        ]


    def mapper_flights(self, _, line):

        # skip header
        if line.startswith("fl_date"):
            return

        try:
            row = next(csv.reader(StringIO(line)))

            # keys (codice compagnia, aeroporto di partenza, mese)
            carrier = row[2]
            origin = row[4]
            # destination = row[]  --> non so quale sia la colonna :(
            month = row[1]

            # tratta
            # route = (origin, destination)

            # ritardo in partenza, ritardo in arrivo
            dep_delay = float(row[6]) if row[6] else 0
            arr_delay = float(row[7]) if row[7] else 0
            # voli cancellati
            cancelled = int(row[8]) if row[8] else 0

            # ritardi negativi (arrivi e partenze in anticipo) portati a 0
            dep_delay = max(dep_delay, 0)
            arr_delay = max(arr_delay, 0)


            # creazione mappa key x value
            key = (carrier, origin)
            # key = (carrier, route)

            value = (
                1,
                arr_delay,
                arr_delay,
                arr_delay,
                cancelled,
                month
            )

            yield key, value

        except:
            pass


    def reducer_statistics(self, key, values):

        # variabili attese 
        total_flights = 0

        min_delay = float("inf")
        max_delay = float("-inf")

        sum_delay = 0
        total_cancelled = 0

        months = set()

        for value in values:

            (
                count,
                arr_delay,
                min_arr,
                max_arr,
                cancelled,
                month
            ) = value


            total_flights += count

            min_delay = min(min_delay, min_arr)
            max_delay = max(max_delay, max_arr)

            sum_delay += arr_delay

            total_cancelled += cancelled

            months.add(month)

        # ritardo medio per key ottenuto dai parametri appena calcolati
        avg_delay = sum_delay / total_flights
        # tasso di cancellazione ottenuto dai paramteri appena calcolati
        cancellation_rate = total_cancelled / total_flights

        yield key, {
            "num_flights": total_flights,
            "min_arr_delay": min_delay,
            "max_arr_delay": max_delay,
            "avg_arr_delay": avg_delay,
            "cancellation_rate": cancellation_rate,
            "months_active": list(months)
        }


if __name__ == '__main__':
    FlightAnalysis.run()