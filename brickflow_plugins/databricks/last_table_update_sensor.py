import logging
import time
from datetime import timedelta, datetime
from typing import Union

from delta import DeltaTable

from dateutil.parser import parse
from pyspark.sql import SparkSession


class LastTableUpdateSensor:
    """
    Sensor to check if the delta table has been updated within a certain time frame based on the table history.
    The sensor will use workflow execution timestamp and compare it with the last timestamp obtained from the history.
    If this difference is less than provided time delta, the sensor will succeed, otherwise it will keep poking the
    table.
    """

    def __init__(
        self,
        catalog: str,
        database: str,
        table: str,
        execution_timestamp: Union[str, datetime, int],
        time_delta: Union[dict, timedelta],
        interval: int = 600,  # seconds
        skip_poking: bool = True,
    ):
        """
        Parameters
        ----------
        catalog: str
            Catalog name
        database: str
            Database name
        table: str
            Table name pointing to the existing object in the Unity Catalog
        execution_timestamp: Union[str, datetime, int]
            The timestamp of the execution as provided by the workflow
        time_delta: Union[dict, timedelta]
            Time delta between the workflow execution and the last update of the table based on the delta history.
        interval: int
            Interval in seconds between the pokes
        skip_poking: bool
            Skip poking the table completely
            (useful for test purposes or when upstream is not triggered on a regualr basis, e.g. dev or qa environments)
        """
        self.catalog = catalog
        self.database = database
        self.table = table

        table_name = self.catalog + "." + self.database + "." + self.table
        logging.info(f"Checking the table '{table_name}' for the last update...")

        if isinstance(execution_timestamp, str):
            self.execution_timestamp = parse(execution_timestamp)
        elif isinstance(execution_timestamp, int):
            # Brickflow provides the timestamp in milliseconds
            self.execution_timestamp = datetime.utcfromtimestamp(execution_timestamp / 1000)
        else:
            self.execution_timestamp = execution_timestamp

        self.time_delta = time_delta if isinstance(time_delta, timedelta) else timedelta(**time_delta)
        self.interval = interval
        self.skip_poking = skip_poking

        self._spark = SparkSession.getActiveSession()
        self._delta_table = DeltaTable.forName(self._spark, table_name)

    @property
    def last_change_timestamp(self) -> datetime:
        """
        Get the last update timestamp of the delta table.
        """
        rows = (
            self._delta_table.history()
            .where("operation in ('CREATE OR REPLACE TABLE AS SELECT','MERGE','APPEND','OVERWRITE','WRITE')")
            .select("timestamp")
            .limit(1)
            .collect()
        )

        if len(rows) > 0:
            return rows[0][0]
        return parse("1900-01-01 00:00:00")

    def poke(self) -> None:
        """
        Continuously check if the table has been updated within the time frame.
        """
        rt = self.execution_timestamp - self.time_delta
        t = self.last_change_timestamp

        while t <= rt:
            logging.info(f"Last update is '{t}', while the expected run timestamp is '{rt}'... Continue poking...")

            if self.skip_poking is True:
                break

            time.sleep(self.interval)
            t = self.last_change_timestamp

        logging.info(f"Success! Last update is '{t}', while the expected run timestamp is '{rt}', exiting!")
