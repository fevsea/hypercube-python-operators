from enum import StrEnum


class Datum:
    """Represents a unit of data on the cluster.

    Only this class can be actually persisted and retrieved from the disk. Other classes are just wrappers.
    """


    def __init__(self):
        """Only uncommitted data can be modified."""
        self.commited = False
