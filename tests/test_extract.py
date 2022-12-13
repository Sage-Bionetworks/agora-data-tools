from unittest.mock import patch

import pandas as pd
import pytest
import synapseclient

from agoradatatools.etl import extract, utils


class MockAsDF:
    def asDataFrame(self):
        database_dict = {"Database": ["centerMapping"], "Id": ["syn123"]}
        databasetosynid_mappingdf = pd.DataFrame(database_dict)
        return databasetosynid_mappingdf


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
    mock_df = MockAsDF()
    with patch.object(syn, "tableQuery", return_value=mock_df) as patch_syn_tablequery:
        df = extract.read_table_into_df(table_id="syn11111111", syn=syn)
        patch_syn_tablequery.assert_called_once_with("select * from syn11111111")
        assert isinstance(df, pd.DataFrame)
        assert df.equals(mock_df.asDataFrame())


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


# test if utils._login_to_synapse is called when syn=None
def test_get_entity_as_df_syn_is_none(syn):
    with patch.object(
        utils, "_login_to_synapse", return_value=syn
    ) as patch_login_to_synapse:
        extract.get_entity_as_df(syn_id="syn11111111", format="table", syn=None)
        patch_login_to_synapse.assert_called_once()


# dummy synapse entity needed to supply entity.path to read_csv_into_df for next test
ENTITY = synapseclient.File("fake/path.csv", parent="syn1111111")


@pytest.mark.parametrize("syn_id", [("syn1111111"), ("syn1111111.1")])
# test if synapse entity is retrieved without and with version number
def test_get_entity_as_df_with_version(syn, syn_id):
    """
    Tests handling of synapse id with and without version number
    """
    with patch.object(syn, "get", return_value=ENTITY), patch.object(
        extract, "read_csv_into_df", return_entity=pd.DataFrame()
    ) as patch_read_csv_into_df:
        extract.get_entity_as_df(syn_id=syn_id, format="csv", syn=syn)
        patch_read_csv_into_df.assert_called_once_with(csv_path="fake/path.csv")


# test raise if format is not supported
def test_get_entity_as_df_format_not_supported(syn):
    with pytest.raises(ValueError, match="File type not *"):
        extract.get_entity_as_df(syn_id="syn1111111", format="abc", syn=syn)


@pytest.mark.parametrize(
    "format, callable",
    [
        ("table", "read_table_into_df"),
        ("csv", "read_csv_into_df"),
        ("tsv", "read_tsv_into_df"),
        ("feather", "read_feather_into_df"),
        ("json", "read_json_into_df"),
    ],
)
# test handling of different formats to df
def test_get_entity_as_df_supported_formats(syn, format, callable):
    with patch.object(
        extract, callable, return_value=pd.DataFrame()
    ) as patch_source_to_df:
        df = extract.get_entity_as_df(syn_id="syn1111111", format=format, syn=syn)
        patch_source_to_df.assert_called_once()
        assert isinstance(df, pd.DataFrame)


if __name__ == "__main__":
    pytest.main()
