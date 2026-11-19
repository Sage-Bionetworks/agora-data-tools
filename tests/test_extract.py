import asyncio
import inspect
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from agoradatatools.etl import extract, utils


def test_read_csv_into_df_if_not_csv():
    with pytest.raises(ValueError, match="Please make sure *"):
        extract.read_csv_into_df(csv_path="./tests/test_assets/test_file.tsv")


def test_read_csv_into_df():
    with patch.object(pd, "read_csv", return_value=pd.DataFrame()) as patch_pd_read_csv:
        df = extract.read_csv_into_df(csv_path="./tests/test_assets/test_file.csv")
        patch_pd_read_csv.assert_called_once_with(
            "./tests/test_assets/test_file.csv", float_precision="round_trip"
        )
        assert isinstance(df, pd.DataFrame)


def test_read_tsv_into_df_if_not_tsv():
    with pytest.raises(ValueError, match="Please make sure *"):
        extract.read_tsv_into_df(tsv_path="./tests/test_assets/test_file.csv")


def test_read_tsv_into_df():
    with patch.object(pd, "read_csv", return_value=pd.DataFrame()) as patch_pd_read_csv:
        df = extract.read_tsv_into_df(tsv_path="./tests/test_assets/test_file.tsv")
        patch_pd_read_csv.assert_called_once_with(
            "./tests/test_assets/test_file.tsv", sep="\t"
        )
        assert isinstance(df, pd.DataFrame)


def test_read_table_into_df(syn):
    mock_df = pd.DataFrame({"Database": ["centerMapping"], "Id": ["syn123"]})
    with patch.object(extract, "query", return_value=mock_df) as patch_query:
        df = extract.read_table_into_df(table_id="syn11111111", syn=syn)
        patch_query.assert_called_once_with(
            query="select * from syn11111111", synapse_client=syn
        )
        assert isinstance(df, pd.DataFrame)
        assert df.equals(mock_df)


def test_read_feather_into_df_if_not_feather():
    with pytest.raises(ValueError, match="Please make sure *"):
        extract.read_feather_into_df(feather_path="./tests/test_assets/test_file.csv")


def test_read_feather_into_df():
    with patch.object(
        pd, "read_feather", return_value=pd.DataFrame()
    ) as patch_pd_read_feather:
        df = extract.read_feather_into_df(
            feather_path="./tests/test_assets/test_file.feather"
        )
        patch_pd_read_feather.assert_called_once_with(
            "./tests/test_assets/test_file.feather"
        )
        assert isinstance(df, pd.DataFrame)


def test_read_json_into_df_if_not_json():
    with pytest.raises(ValueError, match="Please make sure *"):
        extract.read_json_into_df(json_path="./tests/test_assets/test_file.csv")


def test_read_json_into_df():
    with patch.object(
        pd, "read_json", return_value=pd.DataFrame()
    ) as patch_pd_read_json:
        df = extract.read_json_into_df(json_path="./tests/test_assets/test_file.json")
        patch_pd_read_json.assert_called_once_with(
            "./tests/test_assets/test_file.json", orient="records"
        )
        assert isinstance(df, pd.DataFrame)


def test_read_yaml_into_df_if_not_yaml():
    with pytest.raises(ValueError, match="Please make sure *"):
        extract.read_yaml_into_df(yaml_path="./tests/test_assets/test_file.csv")


def test_read_yaml_into_df():
    """Test reading YAML file and converting to DataFrame."""
    df = extract.read_yaml_into_df(
        yaml_path="./tests/test_assets/immunohisto_transform/input/immunohisto_measure_order.yaml"
    )

    # Verify it returns a DataFrame
    assert isinstance(df, pd.DataFrame)

    # Verify it has the expected generic columns (key/items)
    assert "key" in df.columns
    assert "items" in df.columns

    # Verify it has the expected number of rows (5 biomarkers + 8 pathology = 13)
    assert len(df) == 13

    # Verify the biomarkers are present
    biomarker_rows = df[df["key"] == "biomarkers"]
    assert len(biomarker_rows) == 5
    assert "NfL" in biomarker_rows["items"].values

    # Verify the pathology measures are present
    pathology_rows = df[df["key"] == "pathology"]
    assert len(pathology_rows) == 8
    assert "Tau (HT7)" in pathology_rows["items"].values


@pytest.mark.parametrize("syn_id, version", [("syn1111111", None), ("syn1111111.1", 1)])
# test if synapse entity is retrieved without and with version number
def test_get_entity_as_df_with_version(syn, syn_id, version):
    """
    Tests handling of synapse id with and without version number
    """
    # dummy synapse entity needed to supply entity.path to read_csv_into_df for next test
    mock_entity = MagicMock(path="fake/path.csv")
    with patch.object(
        extract, "get", return_value=mock_entity
    ) as patch_get, patch.object(
        extract, "read_csv_into_df", return_value=pd.DataFrame()
    ) as patch_read_csv_into_df:
        extract.get_entity_as_df(syn_id=syn_id, source="csv", syn=syn)
        patch_get.assert_called_once_with(
            synapse_id=syn_id.split(".")[0],
            version_number=version,
            synapse_client=syn,
        )
        patch_read_csv_into_df.assert_called_once_with(csv_path="fake/path.csv")


# test raise if  is not supported
def test_get_entity_as_df_not_supported(syn):
    with patch.object(extract, "get", return_value=MagicMock()):
        with pytest.raises(ValueError, match="File type not *"):
            extract.get_entity_as_df(syn_id="syn1111111", source="abc", syn=syn)


@pytest.mark.parametrize(
    "source, callable",
    [
        ("table", "read_table_into_df"),
        ("csv", "read_csv_into_df"),
        ("tsv", "read_tsv_into_df"),
        ("feather", "read_feather_into_df"),
        ("json", "read_json_into_df"),
        ("yaml", "read_yaml_into_df"),
    ],
)
# test handling of different formats to df
def test_get_entity_as_df_supported_formats(syn, source, callable):
    with patch.object(
        extract, "get", return_value=MagicMock(path="fake/path")
    ), patch.object(
        extract, callable, return_value=pd.DataFrame()
    ) as patch_source_to_df:
        df = extract.get_entity_as_df(syn_id="syn1111111", source=source, syn=syn)
        patch_source_to_df.assert_called_once()
        assert isinstance(df, pd.DataFrame)


def test_get_entity_as_df_async_returns_same_result_as_sync(syn):
    # GIVEN get_entity_as_df is patched to return a known value
    with patch.object(
        extract, "get_entity_as_df", return_value=pd.DataFrame({"a": [1]})
    ) as patch_sync:
        # WHEN I await the async wrapper built from it
        wrapped = utils.to_async(extract.get_entity_as_df)
        df = asyncio.run(wrapped(syn_id="syn1111111", source="csv", syn=syn))
    # THEN the sync function was called with the same arguments and its result returned
    patch_sync.assert_called_once_with(syn_id="syn1111111", source="csv", syn=syn)
    pd.testing.assert_frame_equal(df, pd.DataFrame({"a": [1]}))


def test_get_entity_as_df_async_is_a_coroutine_function():
    assert inspect.iscoroutinefunction(extract.get_entity_as_df_async)


READERS = {
    "csv": extract.read_csv_into_df,
    "tsv": extract.read_tsv_into_df,
    "json": extract.read_json_into_df,
}

MISMATCHED_FORMAT = {"csv": "tsv", "tsv": "csv", "json": "csv"}

# Every reader is tested with every suffix in COMPRESSION_SUFFIXES, so a suffix added
# there is covered here without editing these tests.
compressed_reader_cases = pytest.mark.parametrize(
    "file_format, suffix",
    [(fmt, suffix) for fmt in READERS for suffix in extract.COMPRESSION_SUFFIXES],
)


def _write_uncompressed_and_compressed(tmp_path, file_format, suffix):
    # pandas picks the compression from the file name when writing, as it does when reading
    df = pd.DataFrame({"gene": ["A", "B"], "value": [1, 2]})
    uncompressed = tmp_path / f"sample.{file_format}"
    compressed = tmp_path / f"sample.{file_format}{suffix}"
    for path in (uncompressed, compressed):
        if file_format == "json":
            df.to_json(path, orient="records")
        else:
            df.to_csv(path, sep="\t" if file_format == "tsv" else ",", index=False)
    return str(uncompressed), str(compressed)


def test_gzip_is_a_supported_compression():
    assert ".gz" in extract.COMPRESSION_SUFFIXES


@compressed_reader_cases
def test_read_compressed_file_matches_uncompressed(tmp_path, file_format, suffix):
    uncompressed_path, compressed_path = _write_uncompressed_and_compressed(
        tmp_path, file_format, suffix
    )
    # the compressed file must differ on disk, or this test would prove nothing
    with open(uncompressed_path, "rb") as plain, open(compressed_path, "rb") as packed:
        assert plain.read() != packed.read()
    reader = READERS[file_format]
    pd.testing.assert_frame_equal(reader(uncompressed_path), reader(compressed_path))


@compressed_reader_cases
def test_read_rejects_compressed_file_without_file_type(file_format, suffix):
    reader = READERS[file_format]
    with pytest.raises(ValueError, match="Please make sure *"):
        reader(f"sample{suffix}")


@compressed_reader_cases
def test_read_rejects_compressed_file_with_mismatched_file_type(file_format, suffix):
    reader = READERS[file_format]
    with pytest.raises(ValueError, match="Please make sure *"):
        reader(f"sample.{MISMATCHED_FORMAT[file_format]}{suffix}")
