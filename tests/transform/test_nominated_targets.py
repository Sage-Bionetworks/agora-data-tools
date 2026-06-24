import os

import pandas as pd
import pytest

from agoradatatools.etl.transform import nominated_targets


class TestTransformNominatedTargets:
    """Tests for transform_nominated_targets pass/fail paths using nominated_targets fixtures."""

    data_files_path = "tests/test_assets/nominated_targets"

    pass_test_data = [
        (
            "target_list_good_input.csv",
            "gene_metadata_good_input.json",
            "pharos_classes_good_input.csv",
            "nominated_targets_good_test_output.json",
        ),
    ]
    pass_test_ids = [
        "Pass with good data (retains gene without pharos_class, resolves multi-row pharos)",
    ]

    # Each tuple is (target_list_file, gene_metadata_file, pharos_classes_file,
    # error_type, error_match). A None file intentionally omits that dataset.
    fail_test_data = [
        # pharos_classes dataset is missing entirely.
        (
            "target_list_good_input.csv",
            "gene_metadata_good_input.json",
            None,
            ValueError,
            "Missing required datasets",
        ),
        # target_list is missing the required "source" column.
        (
            "target_list_missing_source_column_input.csv",
            "gene_metadata_good_input.json",
            "pharos_classes_good_input.csv",
            ValueError,
            "Missing required columns",
        ),
        # pharos_classes has a pharos_class value outside the allowed set.
        (
            "target_list_good_input.csv",
            "gene_metadata_good_input.json",
            "pharos_classes_invalid_class_input.csv",
            ValueError,
            "column 'pharos_class': .*one_of",
        ),
        # target_list has an empty ensembl_gene_id (fails the not_empty rule).
        (
            "target_list_empty_ensembl_gene_id_input.csv",
            "gene_metadata_good_input.json",
            "pharos_classes_good_input.csv",
            ValueError,
            "column 'ensembl_gene_id': .*not_empty",
        ),
    ]
    fail_test_ids = [
        "Fail with missing pharos_classes dataset",
        "Fail with missing required column in target_list",
        "Fail with invalid pharos_class value",
        "Fail with empty ensembl_gene_id in target_list",
    ]

    @staticmethod
    def _load_datasets(
        target_list_file: str,
        gene_metadata_file: str | None = None,
        pharos_classes_file: str | None = None,
    ) -> dict[str, pd.DataFrame]:
        input_path = os.path.join(
            TestTransformNominatedTargets.data_files_path, "input"
        )
        datasets = {
            "target_list": pd.read_csv(os.path.join(input_path, target_list_file))
        }
        if gene_metadata_file is not None:
            datasets["gene_metadata"] = pd.read_json(
                os.path.join(input_path, gene_metadata_file)
            )
        if pharos_classes_file is not None:
            datasets["pharos_classes"] = pd.read_csv(
                os.path.join(input_path, pharos_classes_file)
            )
        return datasets

    @pytest.mark.parametrize(
        "target_list_file, gene_metadata_file, pharos_classes_file, expected_output_file",
        pass_test_data,
        ids=pass_test_ids,
    )
    def test_transform_nominated_targets_should_pass(
        self,
        target_list_file: str,
        gene_metadata_file: str,
        pharos_classes_file: str,
        expected_output_file: str,
    ) -> None:
        datasets = self._load_datasets(
            target_list_file, gene_metadata_file, pharos_classes_file
        )
        output_df = nominated_targets.transform_nominated_targets(datasets=datasets)
        output_df = output_df.reset_index(drop=True)
        expected_df = pd.read_json(
            os.path.join(self.data_files_path, "output", expected_output_file),
        )
        expected_df["initial_nomination"] = expected_df["initial_nomination"].astype(
            "Int64"
        )
        expected_df = expected_df.reset_index(drop=True)
        pd.testing.assert_frame_equal(output_df, expected_df)

    @pytest.mark.parametrize(
        "target_list_file, gene_metadata_file, pharos_classes_file, error_type, error_match",
        fail_test_data,
        ids=fail_test_ids,
    )
    def test_transform_nominated_targets_should_fail(
        self,
        target_list_file: str,
        gene_metadata_file: str | None,
        pharos_classes_file: str | None,
        error_type: type[BaseException],
        error_match: str,
    ) -> None:
        datasets = self._load_datasets(
            target_list_file, gene_metadata_file, pharos_classes_file
        )
        with pytest.raises(error_type, match=error_match):
            nominated_targets.transform_nominated_targets(datasets=datasets)

    def test_transform_nominated_targets_should_fail_on_duplicate_gene_metadata(
        self,
    ) -> None:
        """Duplicate ensembl_gene_id in gene_metadata violates the m:1 merge."""
        datasets = self._load_datasets(
            "target_list_good_input.csv",
            "gene_metadata_duplicate_ensembl_gene_id_input.json",
            "pharos_classes_good_input.csv",
        )
        with pytest.raises(pd.errors.MergeError):
            nominated_targets.transform_nominated_targets(datasets=datasets)


class TestSortedUniqueSplit:
    """Tests for the _sorted_unique_split helper."""

    test_data = [
        (["Rush"], ["Rush"]),
        ([None], []),
        (["   "], []),
        (["Rush, MSBB"], ["MSBB", "Rush"]),
        (["Rush,"], ["Rush"]),
        (["Rush", "MSBB, Mayo"], ["MSBB", "Mayo", "Rush"]),
        (["Rush", None], ["Rush"]),
    ]
    test_ids = [
        "Single row, single token",
        "Single row, NA value",
        "Single row, all-whitespace value (dropped)",
        "Single row, comma-separated string",
        "Single row, trailing comma with missing second value",
        "Multiple rows, mix of single and comma-separated",
        "Multiple rows, one missing value",
    ]

    @pytest.mark.parametrize("values, expected", test_data, ids=test_ids)
    def test_sorted_unique_split(self, values: list, expected: list[str]) -> None:
        result = nominated_targets._sorted_unique_split(pd.Series(values, dtype=object))
        assert result == expected


class TestResolvePharosClass:
    """Tests for the _resolve_pharos_class helper."""

    test_data = [
        (["Tbio"], "Tbio"),
        ([None], None),
        (["Tdark", "Tclin", "Tbio"], "Tclin"),
        (["Tdark", None], "Tdark"),
    ]
    test_ids = [
        "Single row, valid value",
        "Single row, missing value",
        "Multiple rows, highest priority not first",
        "Multiple rows, one missing value",
    ]

    @pytest.mark.parametrize("values, expected", test_data, ids=test_ids)
    def test_resolve_pharos_class(self, values: list, expected: str | None) -> None:
        result = nominated_targets._resolve_pharos_class(
            pd.Series(values, dtype=object)
        )
        assert result == expected
