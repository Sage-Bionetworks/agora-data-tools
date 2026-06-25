"""
This module provides classes for querying external data sources, including Ensembl, BioMart, Pharos and UniProt, using
the bioservices library or HTTP GET/POST requests. Each class returns query results as a pandas DataFrame.

Abstract classes:
    * QueryObject: Abstract base class for every other query object.

    * BatchedQuery(QueryObject, ABC): Abstract base class for query objects that require breaking queries into batches.

Concrete classes:
    * BioMartQuery(QueryObject): Class for querying the BioMart API via bioservices for gene information. This class
        sends the full request in one query.

    * EnsemblVersionQuery(BatchedQuery): Class for querying the Ensembl API via bioservices for version information
        about Ensembl IDs. This class sends requests in batches to comply with server limits.

    * PharosQuery(QueryObject): Class for querying the Pharos API via POST for targets information. This class sends
        the full request in one query.

    * UniProtQuery(BatchedQuery): Class for querying the UniProt API via the unipressed library for protein accession
        information. This class sends requests in batches to reduce response time from the server.
"""

import json
import time
import requests
import pandas as pd
from io import StringIO
from abc import ABC, abstractmethod
from unipressed import IdMappingClient
from bioservices import Ensembl, BioMart, UniProt


class QueryObject(ABC):
    """
    Abstract base class for querying external data sources via a library, GET or POST. This class automatically retries
    failed queries up to `n_retries` times, with each attempt triggered by calling `_send_query`.

    Subclasses must implement the `_send_query` method:
        * `_send_query` should handle building and sending the actual request.
        * If the overall request is a single query, `_send_query` will be called with the same arguments that were
              passed to query().
        * If the request is a batched query, this function will be called with argument `batch_items`, which
              contains items from a single batch.
        * `_send_query` should return a pandas DataFrame containing the query results, or None if the query failed.

    Subclass usage:
        * Queries are sent by instantiating a subclass and calling <object>.query() with any arguments necessary for the
          subclass-specific query.
        * Subclass initialization should include parameters like n_retries, batch_size, or other information that isn't
          specific to a single query.
        * query() should take arguments that are specific to a single query. All arguments passed to query() are passed
          as-is through to `_send_query`.
    """

    DEFAULT_N_RETRIES = 5

    def __init__(self, n_retries: int = DEFAULT_N_RETRIES) -> None:
        if n_retries < 0:
            raise ValueError("n_retries must be a non-negative integer")

        self.n_retries = n_retries

    @abstractmethod
    def _send_query(self, *args, **kwargs) -> pd.DataFrame:
        pass

    def query(self, *args, **kwargs) -> pd.DataFrame:
        """
        Queries the external data source, automatically handling retries. Classes that need to implement custom query
        logic should override this method. All arguments passed to this function are passed all the way through to
        `_send_query`.
        """
        return self._query_with_retries(*args, **kwargs)

    def _query_with_retries(self, *args, **kwargs) -> pd.DataFrame:
        """
        Sends a single query, retrying up to self.n_retries times. Each attempt is made by calling _send_query, which
        should be implemented by the subclass. All arguments are passed through as-is to _send_query.
        """
        tries = 0
        while tries < self.n_retries:
            try:
                data = self._send_query(*args, **kwargs)
                # Data will be a pandas DataFrame if the query was successful, or None if the query failed
                if data is not None:
                    return data
            except Exception as e:
                print(e)  # Print but don't raise immediately

            # If data was None or there was an exception, increment the retry counter and sleep
            # before trying again. Raise an error if the maximum number of retries is reached.
            tries = tries + 1
            if tries >= self.n_retries:
                raise RuntimeError(f"Query failed after {self.n_retries} retries")

            print(f"Retrying... (attempt {tries+1}/{self.n_retries})")
            time.sleep(2**tries)  # Exponential backoff

        return None


class BatchedQuery(QueryObject, ABC):
    """
    Abstract class for query objects that require breaking queries into batches.

    This class was written assuming that query() will be called with a list of strings (e.g. a list of Ensembl IDs)
    that can be split into smaller lists for querying.
    """

    DEFAULT_BATCH_SIZE = 1000

    def __init__(self, batch_size: int = DEFAULT_BATCH_SIZE, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.batch_size = batch_size

    def query(self, items: list[str], *args, **kwargs) -> pd.DataFrame:
        """
        Overrides superclass method to send the query in smaller batches. All additional arguments passed to this
        function are passed all the way through to `_send_query`.
        """
        return self._batched_query(items, *args, **kwargs)

    def _batched_query(self, items: list[str], *args, **kwargs) -> pd.DataFrame:
        """
        Helper method to split a large list of items into smaller batches for querying. Each batch is sent via
        _query_with_retries to allow automatic retries in case of failure. All additional args passed to this function
        are passed all the way through to `_send_query`.

        Args:
            items (list[str]): A list of items (e.g. Ensembl IDs) to query.

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated results of all batches.

        Raises:
            RuntimeError: If data retrieval fails after the specified number of retries.
        """
        results = []

        for batch_start in range(0, len(items), self.batch_size):
            end = min(len(items), batch_start + self.batch_size)
            print(f"Querying items {batch_start + 1} - {end}")

            # If this function returns without raising an error, the query was successful
            data = self._query_with_retries(
                batch_items=items[batch_start:end], *args, **kwargs
            )
            results.append(data)

        return pd.concat(results, ignore_index=True)


class BioMartQuery(QueryObject):
    """
    Class that queries the BioMart API via the bioservices library to get gene information for either human, mouse, or
    marmosets. This class allows specifying which fields to request from BioMart via `attributes`, and allows filtering
    to specific genes via `filters`.

    See also the BioMart REST documentation: https://www.ensembl.org/info/data/biomart/biomart_restful.html

    Usage example:
        result = BioMartQuery().query(
            dataset=BioMartQuery.SPECIES_HUMAN,
            attributes=['ensembl_gene_id', 'external_gene_name', 'chromosome_name'],
            filters={'external_gene_name': ['BRCA1', 'TP53']}
        )
    """

    # The Ensembl-formatted dataset values to use for each species
    SPECIES_HUMAN = "hsapiens_gene_ensembl"  # Human
    SPECIES_MOUSE = "mmusculus_gene_ensembl"  # Mouse
    SPECIES_MARMOSET = "cjacchus_gene_ensembl"  # Marmoset

    VALID_DATASETS = [SPECIES_HUMAN, SPECIES_MOUSE, SPECIES_MARMOSET]

    def _send_query(
        self, dataset: str, attributes: list[str], filters: dict[str, list[str]]
    ) -> pd.DataFrame:
        """
        Sends the bioservices BioMart query and returns the response as a pandas DataFrame.

        Args:
            dataset (str): the dataset to query. Must be one of the species in VALID_DATASETS.
            attributes (list[str]): a list of attributes that Biomart should return as columns. Example:
                ['ensembl_gene_id', 'external_gene_name', 'chromosome_name']
            filters (dict[str, list[str]]): a dict where the keys are the attribute to filter on, and the values are a
                list of valid items. Example: {'external_gene_name': list_of_symbols}

        Returns:
            pd.DataFrame: Biomart's response in DataFrame format, where columns should match the attributes
                list and rows contain results that match the filter

        Raises:
            ValueError: if the dataset/species is not in the list of valid datasets.
        """
        if dataset not in self.VALID_DATASETS:
            raise ValueError(
                f"Invalid dataset: {dataset}. Valid options are: {self.VALID_DATASETS}"
            )

        bm = BioMart()
        bm.add_dataset_to_xml(dataset)

        for name, value in filters.items():
            bm.add_filter_to_xml(name, list(value))

        for attribute in attributes:
            bm.add_attribute_to_xml(attribute)

        res = bm.query(bm.get_xml())

        # The response comes as a tab-separated string with no column headers
        res_df = pd.read_csv(StringIO(res), sep="\t", header=None)
        res_df.columns = attributes

        return res_df


class EnsemblVersionQuery(BatchedQuery):
    """
    Class that queries the Ensembl API to get version information for each Ensembl ID. The API can only process 1000 IDs
    at a time, so the query is broken into batches. Queries are made via the bioservices library.

    See also the API documentation: https://rest.ensembl.org/documentation/info/archive_id_post

    Usage example:
        result = EnsemblVersionQuery().query(['ENSG00000139618', 'ENSG00000141510'])
    """

    # API can process a maximum of 1000 IDs at a time
    BATCH_SIZE = 1000

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize with the default batch size. Batch size is not changeable for this object due to API limitations.
        """
        super().__init__(batch_size=self.BATCH_SIZE, *args, **kwargs)

    def _send_query(self, batch_items: list[str]) -> pd.DataFrame:
        """
        Queries a single batch of Ensembl IDs using the bioservices library and returns the result as a pandas
        DataFrame.

        Args:
            batch_items (list[str]): A list of Ensembl IDs to query in a single batch.

        Returns:
            pd.DataFrame: A pandas DataFrame with the query results. If the request was unsuccessful, the function
            returns None.
        """
        # res will be a JSON string if the request is successful
        res = Ensembl().post_archive(identifiers=batch_items)
        return pd.DataFrame(res) if res else None


class PharosQuery(QueryObject):
    """
    Class that queries the Pharos API via POST (using the requests library) to download data for a specified model and
    fields. This class currently defines constants for downloading all data in the "Targets" database (model) for three
    specific fields, but is written to be extendable to other models and fields if necessary.

    The API uses GraphQL, and the query is formatted accordingly before sending the request.

    See also the Pharos API experimentation page: https://pharos.nih.gov/api
    * Note: To find what other fields can be added to a query, go to the Pharos Targets page
    (https://pharos.nih.gov/targets), click "Download", and use the field names as they appear in the "Fields" column.

    Usage example:
        result = PharosQuery().query(
            model=PharosQuery.MODEL_TARGETS,
            fields=PharosQuery.TARGET_FIELDS
        )
    """

    # Query definitions
    PHAROS_URL = "https://pharos-api.ncats.io/graphql"

    QUERY_FORMAT = """
        {{
            download(model: "{model}", fields: {fields}, sqlOnly: false) {{
                data
            }}
        }}
    """

    # Model and field definitions
    MODEL_TARGETS = "Targets"
    TARGET_FIELDS = ["UniProt", "Symbol", "Target Development Level"]

    def _send_query(self, model: str, fields: list[str]) -> pd.DataFrame:
        """
        Sends a download() query to the Pharos API and returns the result as a pandas DataFrame.
        The API uses GraphQL, so the query string is formatted accordingly before sending the request.

        The response from the API is a JSON string that can be converted to a dict with the following structure:
            data {
                download {
                    data {
                        [list of dicts, each with fields "id", "UniProt", "Symbol", and "Target Development Level"]]
                    }
                }
            }
        The inner "data" dict is converted to a pandas DataFrame and returned.

        Args:
            model (str): The name of the Pharos model to query (e.g. "Targets").
            fields (list[str]): A list of fields to include in the query.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the query results.
        """

        query_body = self.QUERY_FORMAT.format(model=model, fields=str(fields)).replace(
            "'", '"'  # Replace single quotes from str(fields) with double quotes
        )

        response = requests.post(url=self.PHAROS_URL, json={"query": query_body})

        if response.ok:
            res_dict = json.loads(response.content)
            pharos_df = pd.json_normalize(res_dict["data"]["download"]["data"])
            return pharos_df

        return None


class UniProtQuery(BatchedQuery):
    """
    Class that queries the UniProt API via the unipressed library to get mapping information from Ensembl IDs to UniProt
    IDs. UniProt API limits are quite high but requests can take a long time for larger batches, so the query is broken
    into batches. Queries are made via IdMappingClient.submit, and responses are converted from JSON to pandas
    DataFrames.

    This class has constants for mapping Ensembl IDs to UniProt IDs. Other IDs can be mapped by changing the `source`
    and `dest` arguments to `query()`.

    Usage example:
        result = UniProtQuery().query(
            ['ENSG00000139618', 'ENSG00000141510'],
            source = UniProtQuery.ID_ENSEMBL,
            dest = UniProtQuery.ID_UNIPROT_SWISS
        )
    """

    # Default batch size.
    BATCH_SIZE = 1000

    # Sleep for 2 seconds in between checking for request status, and time out after 1 minute of waiting
    SLEEP_TIME = 2
    DEFAULT_REQUEST_TIMEOUT = 60

    # Constants for the input/output IDs
    ID_ENSEMBL = "Ensembl"
    ID_UNIPROT_SWISS = "UniProtKB-Swiss-Prot"
    ID_UNIPROT_AC = "UniProtKB_AC-ID"

    def __init__(
        self,
        batch_size: int = BATCH_SIZE,
        request_timeout: int = DEFAULT_REQUEST_TIMEOUT,
        *args,
        **kwargs,
    ) -> None:
        """
        Initializes the UniProtQuery object with the default batch size if no batch size is provided, and the default
        request timeout (in seconds) if no request timeout is provided.
        """
        super().__init__(batch_size=batch_size, *args, **kwargs)
        self.request_timeout = request_timeout

    def _send_query(
        self, batch_items: list[str], source: str, dest: str
    ) -> pd.DataFrame:
        """
        Queries a single batch of input IDs using the UniProt API via the unipressed library and returns the result
        as a pandas DataFrame. The status of the request is checked every 2 seconds until a response is received. If
        no response is received after 1 minute, the function returns None.

        Args:
            batch_items (list[str]): A list of input IDs to query in a single batch.
            source (str): The source ID type, e.g. "Ensembl".
            dest (str): The destination ID type, e.g. "UniProtKB-Swiss-Prot".

        Returns:
            pd.DataFrame: A pandas DataFrame with the query results. If the request timed out, the function returns
            None.
        """
        # Temporarily commented out: the UniProt() functions in bioservices are far simpler to use but they use too much
        # memory due to a bug
        # res = UniProt().mapping(
        #    fr="Ensembl",
        #    to="UniProtKB-Swiss-Prot",
        #    query=batch_items
        # )

        # return pd.DataFrame(res["results"]) if res and "results" in res else None

        request = IdMappingClient.submit(source=source, dest=dest, ids=batch_items)

        timeout = self.request_timeout

        while timeout > 0:
            time.sleep(self.SLEEP_TIME)
            timeout = timeout - self.SLEEP_TIME

            status = request.get_status()
            if status == "FINISHED":
                return pd.DataFrame(request.each_result())
            else:
                print("Waiting for response from UniProt...")

        print(f"Request to UniProt timed out after {self.request_timeout} seconds")
        return None
