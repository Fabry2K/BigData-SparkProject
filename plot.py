import matplotlib.pyplot as plt


def plot_analisi(timer_quarter, timer_half, timer_normal, timer_double, timer_quadruple, name, path):
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
        timer_quarter,
        timer_half,
        timer_normal,
        timer_double,
        timer_quadruple
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
    plt.title(name)

    # griglia
    plt.grid(True)

    # salvataggio
    plt.savefig(path)



def plot_confronto(series, title, path):
    """
    Parameters
    ----------
    series : list of tuples
        [
            ("Algoritmo A", [t1, t2, t3, t4, t5]),
            ("Algoritmo B", [t1, t2, t3, t4, t5]),
            ...
        ]
    title : str
        Titolo del grafico.
    path : str
        Percorso di salvataggio.
    """

    dataset_sizes = ["1/4x", "1/2x", "1x", "2x", "4x"]

    plt.figure(figsize=(10, 6))

    for label, execution_times in series:
        plt.plot(
            dataset_sizes,
            execution_times,
            marker="o",
            label=label
        )

    plt.xlabel("Dataset size")
    plt.ylabel("Execution time (seconds)")
    plt.title(title)

    plt.grid(True)
    plt.legend()

    plt.savefig(path)
    plt.close()
