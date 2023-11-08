import great_expectations as gx
from great_expectations.data_context import FileDataContext

context = FileDataContext.create(project_root_dir="great_expectations")

from expectations.expect_column_values_to_have_list_length import (
    ExpectColumnValuesToHaveListLength,
)

test_dataset = "./metabolomics.json"
context = gx.get_context()
validator = context.sources.pandas_default.read_json(test_dataset)

# ad_diagnosis_p_value
validator.expect_column_values_to_be_of_type("ad_diagnosis_p_value", "list")
validator.expect_column_values_to_not_be_null("ad_diagnosis_p_value")
# for custom and experimental expectations you have to pass args as kwargs
validator.expect_column_values_to_have_list_length(
    column="ad_diagnosis_p_value", list_length=1
)

# associated gene name
validator.expect_column_values_to_be_of_type("associated_gene_name", "str")
validator.expect_column_values_to_not_be_null("associated_gene_name")
validator.expect_column_value_lengths_to_be_between(
    "associated_gene_name", min_value=1, max_value=25
)
# allows all alphanumeric characters, underscores, periods, and dashes
validator.expect_column_values_to_match_regex(
    "associated_gene_name", "^[A-Za-z0-9_.-]+$"
)

# association p
validator.expect_column_values_to_be_of_type("association_p", "float")
validator.expect_column_values_to_not_be_null("association_p")
validator.expect_column_values_to_be_between("association_p", min_value=0, max_value=1)

# ensembl gene id
validator.expect_column_values_to_be_of_type("ensembl_gene_id", "str")
validator.expect_column_values_to_not_be_null("ensembl_gene_id")
validator.expect_column_value_lengths_to_equal("ensembl_gene_id", 15)
# checks format and allowed chatacters
validator.expect_column_values_to_match_regex("ensembl_gene_id", "^ENSG\d{11}$")
validator.expect_column_values_to_be_unique("ensembl_gene_id")

# gene_wide_p_threshold_1kgp
validator.expect_column_values_to_be_of_type("gene_wide_p_threshold_1kgp", "float")
validator.expect_column_values_to_not_be_null("gene_wide_p_threshold_1kgp")
validator.expect_column_values_to_be_between(
    "gene_wide_p_threshold_1kgp", min_value=0, max_value=0.05
)

# metabolite full name
validator.expect_column_values_to_be_of_type("metabolite_full_name", "str")
validator.expect_column_values_to_not_be_null("metabolite_full_name")
validator.expect_column_value_lengths_to_be_between(
    "metabolite_full_name", min_value=1, max_value=25
)
# allows all alphanumeric characters, dashes, parentheses, hyphens and spaces
validator.expect_column_values_to_match_regex(
    "metabolite_full_name", "^[A-Za-z0-9\s\-:.()+]+$"
)

# metabolite ID
validator.expect_column_values_to_be_of_type("metabolite_id", "str")
validator.expect_column_values_to_not_be_null("metabolite_id")
validator.expect_column_value_lengths_to_be_between(
    "metabolite_id", min_value=1, max_value=15
)
# allows all alphanumeric characters and periods
validator.expect_column_values_to_match_regex("metabolite_id", "^[A-Za-z0-9.]+$")

# n_per_group
validator.expect_column_values_to_be_of_type("n_per_group", "list")
validator.expect_column_values_to_not_be_null("n_per_group")
validator.expect_column_values_to_have_list_length(column="n_per_group", list_length=2)

# transposed_boxplot_stats
validator.expect_column_values_to_be_of_type("transposed_boxplot_stats", "list")
validator.expect_column_values_to_not_be_null("transposed_boxplot_stats")
validator.expect_column_values_to_have_list_length(
    column="transposed_boxplot_stats", list_length=2
)

# save expectation suite and run checkpoint
validator.save_expectation_suite()
checkpoint = context.add_or_update_checkpoint(
    name="agora-test-checkpoint",
    validator=validator,
)
checkpoint_result = checkpoint.run()

# generate and open report
context.view_validation_result(checkpoint_result)
