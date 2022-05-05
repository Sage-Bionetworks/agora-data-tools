import pandas as pd
import numpy as np
import warnings


def standardize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Takes in a dataframe and performs standard operations on column names
    :param df: a dataframe
    :return: a dataframe
    """

    df.columns = df.columns.str.replace("[#,@,&,*,^,?,(,),%,$,#,!,/]", "", regex=True)
    df.columns = df.columns.str.replace("[' ', '-', '.']", "_", regex=True)
    df.columns = map(str.lower, df.columns)

    return df


def standardize_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Finds non-compliant values and corrects them
    *if more data cleaning options need to be added to this,
    this needs to be refactored to another function
    :param df: a dataframe
    :return: a dataframe
    """

    # for column in df:
    #     dt = df[column].dtype
    #     if dt == int or dt == float:
    #         df[column] = df[column].fillna(0)
    #     else:
    #         df[column] = df[column].fillna("")

    try:
        df = df.replace(["n/a", "N/A", "n/A", "N/a"], np.nan, regex=True)
    except TypeError:
        print("Error comparing types.")

    return df


def rename_columns(df: pd.DataFrame, column_map: dict) -> pd.DataFrame:
    """Takes in a dataframe and renames columns according to the mapping provided
    :param df: a dataframe
    :param column_map: a dict with the mapping for the columns to be renamed
    :return: a dataframe
    """
    try:
        df = df.rename(columns=column_map)
    except TypeError:
        print("Column mapping must be a dictionary")
        return df

    return df


def nest_fields(df: pd.DataFrame, grouping: str, new_column: str) -> pd.DataFrame:
    """
    This will create a dictionary object with the result of the grouping provided
    :param df: a dataframe
    :param grouping: a string containing the column to group by
    :param new_column: a string with the name of the new column that will contain
    the nested field
    :return: a dataframe
    """
    return (df.groupby(grouping)
            .apply(lambda row: row.replace({np.nan: None}).to_dict('records'))
            .reset_index()
            .rename(columns={0: new_column}))


def calculate_distribution(df: pd.DataFrame, col: str, is_scored):
    if is_scored is not None:
        df = df[df[is_scored] == 'Y'] # df does not have the isscored
    else:
        df = df[df.isin(['Y']).any(axis=1)]

    if df[col].dtype == object:
        df[col] = df[col].astype(float)

    obj = {}

    '''
    In order to smooth out the bins and make sure the entire range from 0
    to the highest theoretical value has been found, we create a copy of the 
    column with that value added to it.  We use to calculate distributions 
    and bins, and subtract the value at the end
    '''
    upper_bound = np.ceil(df[col].max())
    distribution = df[col].append(pd.Series([upper_bound]), ignore_index=True)

    obj["distribution"] = list(pd.cut(distribution, bins=10, precision=3, include_lowest=True, right=True).value_counts())
    obj["distribution"][-1] -= 1 # since this was calculated with the artificial upper_bound, we subtract it

    discard, obj["bins"] = list(pd.cut(distribution, bins=10, precision=3, retbins=True))
    obj["bins"] = np.around(obj["bins"].tolist()[1:], 2)
    base = [0, *obj["bins"][:-1]]
    obj["bins"] = zip(base, obj["bins"])
    obj["bins"] = list(obj["bins"])

    obj["min"] = np.around(df[col].min(), 4)
    obj["max"] = np.around(df[col].max(), 4)
    obj["mean"] = np.around(df[col].mean(), 4)
    obj["first_quartile"] = np.around(df[col].quantile(q=0.25, interpolation='midpoint'))
    obj["third_quartile"] = np.around(df[col].quantile(q=0.75, interpolation='midpoint'))

    return obj


def transform_overall_scores(df: pd.DataFrame) -> pd.DataFrame:
    interesting_columns = ['ensg', 'hgnc_gene_id', 'overall', 'geneticsscore', 'omicsscore', 'literaturescore']

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ['isscored_genetics', 'isscored_omics', 'isscored_lit']
    mapping = dict(zip(interesting_columns[3:], scored))

    for field, is_scored in mapping.items():
        df.loc[lambda row: row[is_scored] == 'N', field] = np.nan

    df['overall'] = df['overall'] - df['flyneuropathscore'].astype(dtype='float64', errors='raise')
    df.drop(columns=['flyneuropathscore'], inplace=True)
    df['literaturescore'] = pd.to_numeric(df['literaturescore'])

    return df[interesting_columns]


def join_datasets(left: pd.DataFrame, right: pd.DataFrame, how: str, on: str):
    return pd.merge(left=left, right=right, how=how, on=on)


def transform_team_info(datasets: dict):
    team_info = datasets['team_info']
    team_member_info = datasets['team_member_info']

    team_member_info = team_member_info.groupby('team')\
        .apply(lambda x: x[x.columns.difference(['team'])]
               .fillna('')
               .to_dict(orient='records'))\
        .reset_index(name="members")

    return join_datasets(left=team_info, right=team_member_info, how='left', on='team')


def transform_rna_seq_data(datasets: dict, adjusted_p_value_threshold: int):
    diff_exp_data = datasets['diff_exp_data']
    gene_info = datasets['gene_info']
    target_list = datasets['target_list']
    eqtl = datasets['eqtl']

    eqtl = eqtl[['ensembl_gene_id', 'haseqtl']]
    gene_info = pd.merge(left=gene_info,
                         right=eqtl,
                         on='ensembl_gene_id',
                         how='left')

    diff_exp_data['study'].replace(to_replace={'MAYO': 'MayoRNAseq', 'MSSM': 'MSBB'}, regex=True, inplace=True)
    diff_exp_data['sex'].replace(
        to_replace={'ALL': 'males and females', 'FEMALE': 'females only', 'MALE': 'males only'},
        regex=True, inplace=True)
    diff_exp_data['model'].replace(to_replace='\\.', value=' x ', regex=True, inplace=True)
    diff_exp_data['model'].replace(to_replace={'Diagnosis': 'AD Diagnosis'}, regex=True, inplace=True)
    diff_exp_data['logfc'] = diff_exp_data['logfc']
    diff_exp_data['fc'] = 2 ** diff_exp_data['logfc']
    diff_exp_data['model'] = diff_exp_data['model'] + " (" + diff_exp_data['sex'] + ")"

    adjusted_diff_exp_data = diff_exp_data.loc[
        ((diff_exp_data['adj_p_val'] <= adjusted_p_value_threshold) | (diff_exp_data['ensembl_gene_id']
                                                                       .isin(target_list['ensembl_gene_id'])))
        & (diff_exp_data['ensembl_gene_id'].isin(gene_info['ensembl_gene_id']))
                                                   ]

    adjusted_diff_exp_data = adjusted_diff_exp_data.drop_duplicates(['ensembl_gene_id'])
    adjusted_diff_exp_data = adjusted_diff_exp_data[['ensembl_gene_id']]

    diff_exp_data = diff_exp_data[diff_exp_data['ensembl_gene_id'].isin(adjusted_diff_exp_data['ensembl_gene_id'])]
    diff_exp_data = diff_exp_data[['ensembl_gene_id', 'logfc', 'fc', 'ci_l', 'ci_r',
                                   'adj_p_val', 'tissue', 'study', 'model', 'hgnc_symbol']]


    diff_exp_data = pd.merge(left=diff_exp_data,
                             right=gene_info,
                             on='ensembl_gene_id',
                             how='left')

    diff_exp_data = diff_exp_data[diff_exp_data['hgnc_symbol'].notna()]
    diff_exp_data = diff_exp_data[
        ['ensembl_gene_id', 'hgnc_symbol', 'logfc', 'fc', 'ci_l', 'ci_r',
         'adj_p_val', 'tissue', 'study', 'model']]

    return diff_exp_data


def transform_network(datasets: dict):
    gene_info = datasets['gene_info']
    networks = datasets['networks']

    gene_info.rename(columns={"symbol": "hgnc_symbol"}, inplace=True)
    gene_info = gene_info[['ensembl_gene_id', 'hgnc_symbol']]
    gene_info.drop_duplicates(inplace=True)

    networks = networks[
        networks['genea_ensembl_gene_id'].isin(gene_info['ensembl_gene_id']) &
        networks['geneb_ensembl_gene_id'].isin(gene_info['ensembl_gene_id'])]

    merged = pd.merge(left=networks,
                      right=gene_info,
                      left_on='genea_ensembl_gene_id',
                      right_on='ensembl_gene_id',
                      how='left')

    merged = pd.merge(left=networks,
                      right=gene_info,
                      left_on='geneb_ensembl_gene_id',
                      right_on='ensembl_gene_id',
                      how='left')
    merged = merged[['genea_ensembl_gene_id', 'geneb_ensembl_gene_id',
                     'genea_external_gene_name', 'geneb_external_gene_name', 'brainregion']]

    return merged


def transform_gene_metadata(datasets: dict, adjusted_p_value_threshold, protein_level_threshold):
    '''
    This function will perform transformations and incrementally create a dataset called gene_metadata.
    Each dataset will be left_joined onto gene_info.
    '''
    gene_info = datasets['gene_info']
    igap = datasets['igap']
    eqtl = datasets['eqtl']
    proteomics = datasets['proteomics']
    rna_change = datasets['rna_expression_change']

    # remove duplicate ensembl_gene_ids and select columns
    gene_info = gene_info.groupby('ensembl_gene_id').apply(lambda x: x.nlargest(1, "_version")).reset_index(drop=True)
    gene_info = gene_info[['ensembl_gene_id', 'symbol', 'name', 'summary', 'alias']]

    gene_metadata = pd.merge(left=gene_info,
                             right=igap,
                             how='left',
                             on='ensembl_gene_id')
    gene_metadata['igap'] = gene_metadata.apply(lambda row: False if row['hgnc_symbol'] is np.NaN else True, axis=1)
    gene_metadata['igap'].fillna(False, inplace=True)
    gene_metadata = gene_metadata[['ensembl_gene_id', 'symbol', 'name', 'summary', 'alias', 'igap']]

    gene_metadata = pd.merge(left=gene_metadata,
                             right=eqtl,
                             how='left',
                             on='ensembl_gene_id')
    gene_metadata = gene_metadata[['ensembl_gene_id', 'symbol', 'name', 'summary', 'alias', 'igap', 'haseqtl']]
    gene_metadata.rename(columns={'haseqtl': 'eqtl'},
                         inplace=True)
    gene_metadata['eqtl'] = gene_metadata['eqtl'].replace({'TRUE': True}).fillna(False)

    rna_change = rna_change[['ensembl_gene_id', 'adj_p_val']]
    rna_change = rna_change.groupby('ensembl_gene_id')['adj_p_val'].agg('min').reset_index()

    gene_metadata = pd.merge(left=gene_metadata,
                             right=rna_change,
                             how='left',
                             on='ensembl_gene_id')
    gene_metadata['adj_p_val'] = gene_metadata['adj_p_val'].fillna(-1)
    gene_metadata['rna_brain_change_studied'] = gene_metadata.apply(
        lambda row: False if row['adj_p_val'] == -1 else True, axis=1)
    gene_metadata['rna_in_ad_brain_change'] = gene_metadata.apply(
        lambda row: True if row['adj_p_val'] <= adjusted_p_value_threshold else False, axis=1)

    gene_metadata = gene_metadata[
        ['ensembl_gene_id', 'name', 'summary', 'alias', 'igap', 'symbol', 'eqtl', 'rna_in_ad_brain_change',
         'rna_brain_change_studied']]

    proteomics = proteomics.dropna(subset=['log2_fc', 'cor_pval', 'ci_lwr', 'ci_upr'])
    proteomics = proteomics.groupby('ensg')['cor_pval'].agg('min').reset_index()

    gene_metadata = pd.merge(left=gene_metadata,
                             right=proteomics,
                             how='left',
                             left_on='ensembl_gene_id',
                             right_on='ensg')
    gene_metadata['cor_pval'] = gene_metadata['cor_pval'].fillna(-1)
    gene_metadata['protein_brain_change_studied'] = gene_metadata.apply(
        lambda row: False if row['cor_pval'] == -1 else True, axis=1)
    gene_metadata['protein_in_ad_brain_change'] = gene_metadata.apply(
        lambda row: True if row['cor_pval'] <= protein_level_threshold else False, axis=1)

    gene_metadata = gene_metadata[
        ['ensembl_gene_id', 'name', 'summary', 'symbol', 'alias', 'igap', 'eqtl', 'rna_in_ad_brain_change',
         'rna_brain_change_studied', 'protein_in_ad_brain_change', 'protein_brain_change_studied']]
    gene_metadata.rename(columns={'symbol': 'hgnc_symbol'}, inplace=True)

    return gene_metadata


def transform_gene_info(datasets: dict):

    gene_metadata = datasets['gene_metadata']
    target_list = datasets['target_list']
    median_expression = datasets['median_expression']
    druggability = datasets['druggability']

    # these are the interesting columns of the druggability dataset
    useful_columns = ['geneid', 'sm_druggability_bucket', 'safety_bucket', 'abability_bucket', 'pharos_class',
                      'classification', 'safety_bucket_definition', 'abability_bucket_definition']
    druggability = druggability[useful_columns]

    target_list = nest_fields(df=target_list,
                              grouping='ensembl_gene_id',
                              new_column='nominated_target')



    median_expression = nest_fields(df=median_expression,
                                    grouping='ensembl_gene_id',
                                    new_column='median_expression')


    druggability = nest_fields(df=druggability,
                               grouping='geneid',
                               new_column='druggability')
    druggability.rename(columns={'geneid': 'ensembl_gene_id'}, inplace=True)

    for dataset in [target_list, median_expression, druggability]:
        gene_metadata = pd.merge(left=gene_metadata,
                                 right=dataset,
                                 on='ensembl_gene_id',
                                 how='left')



    # create 'nominations' field
    gene_metadata['nominations'] = gene_metadata.apply(
        lambda row: len(row['nominated_target']) if isinstance(row['nominated_target'], list) else np.NaN, axis=1)

    # here we return gene_metadata because we preserved its fields and added to the dataframe
    return gene_metadata


def transform_distribution_data(datasets: dict):

    overall_scores = datasets['overall_scores']

    # subtract flyneuropath score from over all scores
    overall_scores['overall'] = overall_scores['overall'] - overall_scores['flyneuropathscore']

    interesting_columns = ['ensg', 'overall', 'geneticsscore', 'omicsscore', 'literaturescore']

    # create mapping to deal with missing values as they take different shape across the fields
    scored = ['isscored_genetics', 'isscored_omics', 'isscored_lit']
    mapping = dict(zip(interesting_columns[2:], scored))
    mapping['overall'] = None

    overall_scores = overall_scores[interesting_columns + scored]

    neo_matrix = {}
    for col in interesting_columns[1:]:  # excludes the ENSG
        neo_matrix[col] = calculate_distribution(overall_scores, col, mapping[col])

    neo_matrix['Logsdon'] = neo_matrix.pop('overall')
    neo_matrix['GeneticsScore'] = neo_matrix.pop('geneticsscore')
    neo_matrix['OmicsScore'] = neo_matrix.pop('omicsscore')
    neo_matrix['LiteratureScore'] = neo_matrix.pop('literaturescore')

    additional_data = [{'name': 'Overall Score', 'syn_id': 'syn25913473', 'wiki_id': '613107'},
                       {'name': 'Genetics Score', 'syn_id': 'syn25913473', 'wiki_id': '613104'},
                       {'name': 'Genomics Score', 'syn_id': 'syn25913473', 'wiki_id': '613106'},
                       {'name': 'Literature Score', 'syn_id': 'syn25913473', 'wiki_id': '613105'}
                       ]
    for col, additional in zip(neo_matrix.keys(), additional_data):
        neo_matrix[col]['name'] = additional['name']
        neo_matrix[col]['syn_id'] = additional['syn_id']
        neo_matrix[col]['wiki_id'] = additional['wiki_id']

    return neo_matrix


def transform_rna_distribution_data(datasets: dict):
    rna_df = datasets['rna']
    rna_df = rna_df[['tissue', 'model', 'logfc']]

    rna_df = rna_df.groupby(['tissue', 'model']).agg('describe')['logfc'].reset_index()[['model', 'tissue', 'min', 'max', '25%', '50%', '75%']]
    rna_df.rename(columns={'25%': 'first_quartile', '50%': 'median', '75%': 'third_quartile'}, inplace=True)

    rna_df['IQR'] = rna_df['third_quartile'] - rna_df['first_quartile']
    rna_df['min'] = rna_df['first_quartile'] - (1.5 * rna_df['IQR'])
    rna_df['max'] = rna_df['third_quartile'] + (1.5 * rna_df['IQR'])

    for col in ['min', 'max', 'median', 'first_quartile', 'third_quartile']:
        rna_df[col] = np.around(rna_df[col], 4)

    rna_df.drop('IQR', axis=1, inplace=True)

    return rna_df


def apply_custom_transformations(datasets: dict, dataset_name: str, dataset_obj: dict):

    if type(datasets) is not dict or type(dataset_name) is not str:
        return None

    if dataset_name == "overall_scores":
        df = datasets['overall_scores']
        return transform_overall_scores(df=df)
    elif dataset_name == "distribution_data":
        return transform_distribution_data(datasets=datasets)
    elif dataset_name == "team_info":
        return transform_team_info(datasets=datasets)
    elif dataset_name == "rnaseq_differential_expression":
        return transform_rna_seq_data(datasets=datasets,
                                      adjusted_p_value_threshold=dataset_obj['custom_transformations']['adjusted_p_value_threshold'])
    elif dataset_name == "network":
        return transform_network(datasets=datasets)
    elif dataset_name == 'gene_metadata':
        return transform_gene_metadata(datasets=datasets,
                                       adjusted_p_value_threshold=dataset_obj['custom_transformations']['adjusted_p_value_threshold'],
                                       protein_level_threshold=dataset_obj['custom_transformations']['protein_level_threshold'])
    elif dataset_name == 'gene_info':
        return transform_gene_info(datasets=datasets)
    elif dataset_name == 'rna_distribution_data':
        return transform_rna_distribution_data(datasets=datasets)
    else:
        return None

