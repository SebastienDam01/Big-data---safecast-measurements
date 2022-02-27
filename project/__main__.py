import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from utils import CassandraWrapper, dummy_generator, kmeans


class GetWrapper(CassandraWrapper):  # type: ignore
    def getDistribution(self, query):
        """
        Returns a DataFrame given the query

        Parameters
        ----------
        query
            String

        Returns
        -------
        DataFrame containing the rows of Cassandra Table
        """
        return pd.DataFrame(list(self._session.execute(query)))


if __name__ == "__main__":
    wrapper = GetWrapper()

    # Distribution of the measures
    print("Saving the distribution of the measures...")
    query = "SELECT value, unit FROM measurements"
    measurements = wrapper.getDistribution(query)

    measurements["value"] = measurements["value"].astype("int64")

    cpm = measurements.loc[measurements["unit"] == "cpm"]

    plot_ = sns.countplot(x=cpm["value"], data=cpm)

    plt.xlim(0, 200)
    plt.xticks(np.arange(0, 200, 20))
    plt.xlabel("Mesures (cpm)")
    plt.ylabel("Fréquence")
    plt.title("Distribution des mesures relevées")
    plt.savefig("distribution.png", format="png")

    # Areas with the most radiations
    print("Saving the areas the most radiated...")
    query = "SELECT latitude, longitude, value, unit FROM area_most_radio"
    area = wrapper.getDistribution(query)

    map_radia = area.loc[area["unit"] == "cpm"]

    # Centroids with most radioactive areas
    # We take 4 centroids but we extract three of them,
    # which are far apart enough.
    print("Computing the centroids, this might take around 30 minutes...")
    X = map_radia.iloc[:, :-2]
    X = X.to_numpy()
    centroids = kmeans(4, 10, dummy_generator, X)

    print("Centroids for most radioactive areas: \n", centroids)

    print(
        "Europe radiations sum :",
        map_radia.loc[
            (map_radia["longitude"] >= -10) & (map_radia["longitude"] <= 50), "value"
        ].sum(),
    )
    print(
        "Oceania radiations sum :",
        map_radia.loc[map_radia["latitude"] <= 10, "value"].sum(),
    )

    # Change the range to fit with the coordinates of the map
    # dirty hack : we use Japan as reference for the points
    BBox = (
        map_radia.longitude.min() * 1.3,
        map_radia.longitude.max() * 1.15,
        map_radia.latitude.min() * 1.2,
        map_radia.latitude.max() * 1.4,
    )

    map = plt.imread("map.jpg")

    fig, ax = plt.subplots(figsize=(8, 7))
    plot = sns.scatterplot(
        x="longitude",
        y="latitude",
        data=area,
        hue="value",
        palette=sns.color_palette("coolwarm", as_cmap=True),
    )
    ax.set_ylabel("latitude")
    ax.set_xlabel("longitude")
    plot.legend(title="value (cpm)")
    ax.set_title("Radiations dans le monde (2011-2020)")
    ax.imshow(map, zorder=0, extent=BBox, aspect="equal", cmap="Greys_r")
    plt.savefig("areas.png", format="png")
