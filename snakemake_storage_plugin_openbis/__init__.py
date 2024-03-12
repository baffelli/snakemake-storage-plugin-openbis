from dataclasses import dataclass, field
from typing import Any, Iterable, Optional, List
from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface

from pybis import Openbis

from snakemake_storage_plugin_openbis.openbis.auth import login_anom

from urllib.parse import urlparse


# Optional:
# Define settings for your storage plugin (e.g. host url, credentials).
# They will occur in the Snakemake CLI as --storage-<storage-plugin-name>-<param-name>
# Make sure that all defined fields are 'Optional' and specify a default value
# of None or anything else that makes sense in your case.
# Note that we allow storage plugin settings to be tagged by the user. That means,
# that each of them can be specified multiple times (an implicit nargs=+), and
# the user can add a tag in front of each value (e.g. tagname1:value1 tagname2:value2).
# This way, a storage plugin can be used multiple times within a workflow with different
# settings.
@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    """Settings for the openBIS storage provider."""

    host: Optional[str] = field(
        default=None,
        metadata={
            "help": "The host url of the openBIS instance",
            "tags": ["openbis"],
        },
    )
    username: Optional[str] = field(
        default=None,
        metadata={
            "help": "The username to use for authentication",
            "tags": ["openbis"],
        },
    )
    password: Optional[str] = field(
        default=None,
        metadata={
            "help": "The password to use for authentication",
            "tags": ["openbis"],
        },
    )
    token: Optional[str] = field(
        default=None,
        metadata={
            "help": "The token to use for authentication",
            "tags": ["openbis"],
        },
    )
    anonymous: Optional[bool] = field(
        default=None,
        metadata={
            "help": "Use anonymous access to openBIS",
            "tags": ["openbis"],
        },
    )


# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    openbis: Openbis
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        match self.settings:
            case None:
                raise ValueError("No settings provided")
            case StorageProviderSettings(
                host=hst, username=un, password=pwd, token=None, anonymous=False
            ):
                self.openbis = Openbis(hst)
                self.openbis.login(un, pwd)
            case StorageProviderSettings(
                host=hst, username=None, password=None, token=token, anonymous=False
            ):
                self.openbis = Openbis(hst)
                self.openbis.set_token(token)
            case StorageProviderSettings(
                host=hst, username=None, password=None, token=None, anonymous=True
            ):
                openbis = Openbis(hst)
                self.openbis = login_anom(openbis)
            case StorageProviderSettings(
                host=hst, username=None, password=None, token=None, anonymous=False
            ):
                raise ValueError("No authentication provided")
            case StorageProviderSettings(
                host=hst, username=un, password=pwd, token=token, anonymous=True
            ):
                raise ValueError("Both token and username/password provided")

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example queries with description for this storage provider (at
        least one)."""
        return [
            ExampleQuery(
                description="Get all the contents of the dataset attached to a sample (object)",
                query="openbis://openbis-server/space/project/object/RAW_DATA",
                query_type=QueryType.ANY,
            ),
            ExampleQuery(
                description="Get a file from a dataset identfied by its permid",
                query="openbis://openbis-server/20210705210000226-747/a.txt",
                query_type=QueryType.INPUT,
            ),
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        None

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        10

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme != "openbis":
            return StorageQueryValidationResult(valid=False, reason="Invalid scheme, only openbis:// is allowed")


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        pass

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        ...

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        ...

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        # return True if the object exists
        ...

    @retry_decorator
    def mtime(self) -> float:
        # return the modification time
        ...

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        ...

    @retry_decorator
    def retrieve_object(self):
        # Ensure that the object is accessible locally under self.local_path()
        ...

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        ...

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        ...

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        ...
