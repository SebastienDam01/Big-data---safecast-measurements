import csv
import os
import uuid
from dataclasses import dataclass
from io import TextIOWrapper
from typing import List  # , Optional
from zipfile import ZipFile

import numpy as np
from cassandra.cluster import Cluster
from scipy.spatial.distance import cdist


class CassandraWrapper:
    def __init__(self, user: str = None) -> None:
        self._cluster = Cluster(["91.121.221.190", "91.121.141.223", "91.121.76.14"])
        keyspace = f"{user if user is not None else os.getenv('USER')}_safecast"
        self._session = self._cluster.connect(keyspace=keyspace)


@dataclass(frozen=True)
class Measurements:
    captured_time: uuid.UUID
    latitude: float
    longitude: float
    value: float
    unit: str
    location_name: str
    device_id: int
    md5sum: str
    height: int
    surface: int
    radiation: int
    uploaded_time: uuid.UUID
    loader_id: int


def stream_csv(csv_zip: str) -> (int, List[str]):
    """
    Return lines of zipped CSV files one by one as a list of strings.

    :param csv_zip: the path to the zipped csv file
    :return: an index and its row of a zipped csv file as a list of strings
    """
    with ZipFile(csv_zip) as zf:
        # We take each of the csv files
        csv_files = sorted(filter(lambda file: file.endswith(".csv"), zf.namelist()))
        for csv_file in csv_files:
            with zf.open(csv_file, "r") as csv_f:
                reader = csv.reader(TextIOWrapper(csv_f, "utf-8"))
                for i, row in enumerate(reader):
                    yield i, row


def dummy_generator(X):
    """
    Most simple generator for streaming.

    """
    for x in X:
        yield x


def kmeans(K: int, max_iter: int, generator, dataset):
    """
    Perform the KMeans algorithm in streaming.

    :param K: number of centroids
    :param max_iter: maximum number of iterations
    :param generator: generator taking a reference to a dataset as input
    :param dataset: reference to the dataset for the generator
    :return: K centroids
    """
    (d,) = next(generator(dataset)).shape
    centroids = np.zeros((K, d))
    for k, c in enumerate(generator(dataset)):
        if k >= K:
            break
        centroids[k] = c

    for n_iter in range(max_iter):
        centroids_acc = np.zeros((K, d))
        centroids_counter = np.zeros((K,))

        for x in generator(dataset):
            if np.isfinite(x).all():
                dist = cdist(x[np.newaxis, :], centroids)
                closest = np.argmin(dist)
                centroids_counter[closest] += 1
                centroids_acc[closest] += x

        centroids = centroids_acc / centroids_counter[:, np.newaxis]

    return centroids
