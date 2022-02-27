import argparse
import datetime
import glob
import os
import uuid
from typing import List

from cassandra.util import uuid_from_time
from joblib import Parallel, delayed
from utils import CassandraWrapper, Measurements, stream_csv


class CreateWrapper(CassandraWrapper):  # type: ignore
    def create_measurement(self, measurement: Measurements):
        """
        Insert a measurement in the Cassandra Cluster.

        Parameters
        ----------
        measurement : Measurements
            Measurements object

        Returns
        -------
        None
        """
        self._session.execute(
            """
                INSERT INTO measurements (
                    captured_time,
                    latitude,
                    longitude,
                    value,
                    unit,
                    location_name,
                    device_id,
                    md5sum,
                    height,
                    surface,
                    radiation,
                    uploaded_time,
                    loader_id
                )
                VALUES (
                    %(captured_time)s,
                    %(latitude)s,
                    %(longitude)s,
                    %(value)s,
                    %(unit)s,
                    %(location_name)s,
                    %(device_id)s,
                    %(md5sum)s,
                    %(height)s,
                    %(surface)s,
                    %(radiation)s,
                    %(uploaded_time)s,
                    %(loader_id)s
                );
            """,
            measurement.__dict__,
        )

    def create_area_most_radio(self, measurement: Measurements):
        """
        Insert a measurement in area_most_radio.
        This table classes the values depending on the area.

        Parameters
        ----------
        measurement : Measurements
            Measurements object

        Returns
        -------
        None
        """
        self._session.execute(
            """
                INSERT INTO area_most_radio (
                    latitude,
                    longitude,
                    value,
                    unit,
                    md5sum
                )
                VALUES (
                    %(latitude)s,
                    %(longitude)s,
                    %(value)s,
                    %(unit)s,
                    %(md5sum)s
                );
            """,
            measurement.__dict__,
        )


def str_to_time_uuid(date_as_str: str, format: str = "%Y-%m-%d %H:%M:%S") -> uuid.UUID:
    """
    Convert a string to a time_uuid given a format and returns it.

    Parameters
    ----------
    date_as_str : str
        The date in string format
    format : str, default "%Y-%m-%d %H:%M:%S"
        Format of the date

    Returns
    -------
    The date as an uuid
    """
    dt = datetime.datetime.strptime(date_as_str, format)
    return uuid_from_time(dt)


def soft_int(s: str) -> int:
    """
    Convert a string to an integer.

    Parameters
    ----------
    s : str
        String

    Returns
    -------
    The integer.
    If the string is empty, returns -1.
    """
    return int(s) if s.isdigit() else -1


def row_to_measurement(row: List[str]) -> Measurements:
    """
    Convert a CSV file line to a Measurements object.

    Used priorly to insertion.

    Parameters
    ----------
    row : List[str]
        Row from a Measurement object.

    Returns
    -------
    A Measurement object
    """
    (
        captured_time,
        latitude,
        longitude,
        value,
        unit,
        location_name,
        device_id,
        md5sum,
        height,
        surface,
        radiation,
        uploaded_time,
        loader_id,
    ) = row

    measurement = Measurements(
        captured_time=str_to_time_uuid(captured_time),
        latitude=float(latitude),
        longitude=float(longitude),
        value=float(value),
        unit=unit,
        location_name=location_name,
        device_id=soft_int(device_id),
        md5sum=md5sum,
        height=soft_int(height),
        surface=soft_int(surface),
        radiation=soft_int(radiation),
        uploaded_time=str_to_time_uuid(uploaded_time),
        loader_id=soft_int(loader_id),
    )
    return measurement


def insert_data_from_zip(zip_csv: str):
    """
    Insert data from a zip file in the Cassandra Cluster.

    This function uses the methods defined in CreateWrapper.

    Parameters
    ----------
    zip_csv : str
        Zip file.

    Returns
    -------
    None
    """
    failed = 0
    passed = 0
    wrapper = CreateWrapper()

    # We directly iterate over the csv file lines here
    for index, row in stream_csv(zip_csv):
        if index == 0:
            continue
        try:
            measurement = row_to_measurement(row)
            wrapper.create_measurement(measurement)
            wrapper.create_area_most_radio(measurement)
            passed += 1

        except ValueError:
            failed += 1

    return zip_csv, passed, failed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="insert", description="Insert data in the Cassandra cluster"
    )

    parser.add_argument(
        "--pattern",
        type=str,
        default="measurements.csv.zip",
        help="Pattern for the CSV file to import",
    )

    args = parser.parse_args()

    data_folder = os.path.abspath(os.path.join(__file__, os.pardir, os.pardir, "data"))

    zipped_csv_files = sorted(glob.glob(f"{data_folder}/{args.pattern}"))
    if len(zipped_csv_files) == 0:
        raise RuntimeError(
            f"No file to import. "
            f"Is the pattern correctly specified? "
            f"Current pattern: {args.pattern}"
        )

    print("The following files will be imported:")
    for zip_csv in zipped_csv_files:
        print(zip_csv)

    if input("Confirm with 'y': ").lower() != "y":
        print("Aborted")
        exit(0)

    res = Parallel(n_jobs=3)(
        delayed(insert_data_from_zip)(zip_csv) for zip_csv in zipped_csv_files
    )

    for zip_csv, passed, failed in res:
        print(f"{zip_csv} Passed: {passed}, Failed: {failed}")
