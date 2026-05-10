import matplotlib.pyplot as plt


def plot_analisi_3_1(timer_3_1_quarter, timer_3_1_half, timer_3_1_normal, timer_3_1_double, timer_3_1_quadruple):
    # dimensioni dataset
    dataset_sizes = [
        "1/4x",
        "1/2x",
        "1x",
        "2x",
        "4x"
    ]

    # tempi misurati
    execution_times = [
        timer_3_1_quarter,
        timer_3_1_half,
        timer_3_1_normal,
        timer_3_1_double,
        timer_3_1_quadruple
    ]

    # creazione grafico
    plt.figure(figsize=(10, 6))

    plt.plot(
        dataset_sizes,
        execution_times,
        marker="o"
    )

    # etichette
    plt.xlabel("Dataset size")
    plt.ylabel("Execution time (seconds)")

    # titolo
    plt.title("Spark Core Local - Analysis 3.1")

    # griglia
    plt.grid(True)

    # salvataggio
    plt.savefig("output/spark_core_local_analysis_3_1.png")

    # visualizzazione
    plt.show()