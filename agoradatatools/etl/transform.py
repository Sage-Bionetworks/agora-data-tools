import pandas as pd
from numpy import NaN

def standardize_column_names(df: pd.core.frame.DataFrame) -> pd.DataFrame:
    """Takes in a dataframe and performs standard operations on column names
    :param df: a dataframe
    :return: a dataframe
    """

    df.columns = df.columns.str.replace("[#,@,&,*,^,?,(,),%,$,#,!,/]", "")
    df.columns = df.columns.str.replace("[' ', '-', '.']", "_")
    df.columns = map(str.lower, df.columns)

    return df


def standardize_values(df: pd.core.frame.DataFrame) -> pd.DataFrame:
    """
    Finds non-compliant values and corrects them
    *if more data cleaning options need to be added to this,
    this needs to be refactored to another function
    :param df: a dataframe
    :return: a dataframe
    """

    for column in df:
        dt = df[column].dtype
        if dt == int or dt == float:
            df[column] = df[column].fillna(0)
        else:
            df[column] = df[column].fillna("")

    try:
        df = df.replace(["NA", "n/a", "N/A", "na", "n/A", "N/a", "Na", "nA"], "")
    except TypeError:
        print("Error comparing types.")

    return df

def rename_columns(df: pd.core.frame.DataFrame, column_map: dict) -> pd.DataFrame:
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

def nest_fields(df: pd.core.frame.DataFrame, grouping: str, new_column: str) -> pd.DataFrame:
    """
    This will create a dictionary object with the result of the grouping provided
    :param df: a dataframe
    :param grouping: a string containing the column to group by
    :param new_column: a string with the name of the new column that will contain the nested field
    :return: a dataframe
    """
    return (df.groupby(grouping)
            .apply(lambda row: row.to_dict('records'))
            .reset_index()
            .rename(columns={0: new_column}))


def transform_overall_scores(df: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    interesting_columns = ['ensg', 'genename', 'overall', 'geneticsscore', 'omicsscore', 'literaturescore',
                           'flyneuropathscore']

    df['overall'] = df['overall'] - df['flyneuropathscore']
    df.drop(columns=['flyneuropathscore'], inplace=True)

    return df

def join_datasets(left: pd.core.frame.DataFrame, right: pd.core.frame.DataFrame, how: str, on: str):
    return pd.merge(left=left, right=right, how=how, on=on)

def transform_team_info(datasets: dict):
    left = datasets['syn12615624'] # team_info
    right = datasets['syn12615633'] # team_member_info

    right = right.groupby('team')\
        .apply(lambda x: x[x.columns.difference(['team'])] .fillna('').to_dict(orient='records'))\
        .reset_index(name="members")

    return join_datasets(left=left, right=right, how='left', on='team')

def transform_rna_seq_data(datasets: dict, models_to_keep: list, adjusted_p_value_threshold: int):
    diff_exp_data = datasets['syn14237651']
    gene_info = datasets['syn25953363']
    target_list = datasets['syn12540368']
    eqtl = datasets['syn12514912']

    eqtl = eqtl[['ensembl_gene_id', 'haseqtl']]
    gene_info = pd.merge(left=gene_info, right=eqtl, on='ensembl_gene_id', how='left')

    diff_exp_data['tmp'] = diff_exp_data[['model', 'comparison', 'sex']].agg(' '.join, axis=1)
    diff_exp_data = diff_exp_data[diff_exp_data['tmp'].isin(models_to_keep)]

    diff_exp_data['study'].replace(to_replace={'MAYO': 'MayoRNAseq', 'MSSM': 'MSBB'}, inplace=True)
    diff_exp_data['sex'].replace(
        to_replace={'ALL': 'males and females', 'FEMALE': 'females only', 'MALE': 'males only'}, inplace=True)
    diff_exp_data['model'].replace(to_replace='\\.', value=' x ', regex=True)
    diff_exp_data['model'].replace(to_replace={'Diagnosis': 'AD Diagnosis'}, inplace=True)
    diff_exp_data['logfc'] = diff_exp_data['logfc'].round(decimals=3)
    diff_exp_data['fc'] = 2 ** diff_exp_data['logfc']
    diff_exp_data['model'] = diff_exp_data['model'] + " (" + diff_exp_data['sex'] + ")"

    adjusted_diff_exp_data = diff_exp_data.loc[((diff_exp_data['adj_p_val'] <= adjusted_p_value_threshold)
                                                | (diff_exp_data['ensembl_gene_id'].isin(
                target_list['ensembl_gene_id'])))
                                               & (diff_exp_data['ensembl_gene_id'].isin(gene_info['ensembl_gene_id']))
                                               ]

    adjusted_diff_exp_data = adjusted_diff_exp_data.drop_duplicates(['ensembl_gene_id'])
    adjusted_diff_exp_data = adjusted_diff_exp_data[['ensembl_gene_id']]

    diff_exp_data = diff_exp_data[diff_exp_data['ensembl_gene_id'].isin(adjusted_diff_exp_data['ensembl_gene_id'])]
    diff_exp_data = diff_exp_data[['ensembl_gene_id', 'logfc', 'fc', 'ci_l', 'ci_r',
                                   'adj_p_val', 'tissue', 'study', 'model', 'hgnc_symbol']]

    diff_exp_data = pd.merge(left=diff_exp_data, right=gene_info, on='ensembl_gene_id', how='left')

    diff_exp_data = diff_exp_data[diff_exp_data['hgnc_symbol'].notna()]
    diff_exp_data = diff_exp_data[
        ['ensembl_gene_id', 'hgnc_symbol', 'logfc', 'fc', 'ci_l', 'ci_r', 'adj_p_val', 'tissue',
         'study', 'model']]

    return diff_exp_data

def transform_network(datasets: dict):
    gene_info = datasets['syn25953363']
    networks = datasets['syn11685347']

    gene_info.rename(columns={"symbol": "hgnc_symbol"}, inplace=True)
    gene_info = gene_info[['ensembl_gene_id', 'hgnc_symbol']]
    gene_info.drop_duplicates(inplace=True)

    networks = networks[
        networks['genea_ensembl_gene_id'].isin(gene_info['ensembl_gene_id']) &
        networks['geneb_ensembl_gene_id'].isin(gene_info['ensembl_gene_id'])]

    merged = pd.merge(left=networks, right=gene_info, left_on='genea_ensembl_gene_id', right_on='ensembl_gene_id',
                      how='left')
    merged = pd.merge(left=networks, right=gene_info, left_on='geneb_ensembl_gene_id', right_on='ensembl_gene_id',
                      how='left')
    merged = merged[['genea_ensembl_gene_id', 'geneb_ensembl_gene_id',
           'genea_external_gene_name', 'geneb_external_gene_name', 'brainregion']]

    return merged


def transform_gene_metadata(datasets: dict, adjusted_p_value_threshold, protein_level_threshold):
    '''
    This function will perform transformations and incrementally create a dataset called gene_metadata.
    Each dataset will be left_joined onto gene_info.
    '''
    gene_info = datasets['syn25953363']
    igap = datasets['syn12514826']
    eqtl = datasets['syn12514912']
    proteomics = datasets['syn18689335']
    brain_expression_change = datasets['syn11914808']
    rna_change = datasets['syn14237651']

    # remove duplicate ensembl_gene_ids and select columns
    gene_info = gene_info.groupby('ensembl_gene_id').apply(lambda x: x.nlargest(1, "_version")).reset_index(drop=True)
    gene_info = gene_info[['ensembl_gene_id', 'symbol', 'name', 'summary', 'alias']]


    gene_metadata = pd.merge(left=gene_info, right=igap, how='left', on='ensembl_gene_id')
    gene_metadata['igap'] = gene_metadata.apply(lambda row: False if row['hgnc_symbol'] is NaN else True, axis=1)
    gene_metadata['igap'].fillna(False, inplace=True)
    gene_metadata = gene_metadata[['ensembl_gene_id', 'symbol', 'name', 'summary', 'alias', 'igap']]

    gene_metadata = pd.merge(left=gene_metadata, right=eqtl, how='left', on='ensembl_gene_id')
    gene_metadata = gene_metadata[['ensembl_gene_id', 'symbol', 'name', 'summary', 'alias', 'igap', 'haseqtl']]
    gene_metadata.rename(columns={'haseqtl': 'eqtl'}, inplace=True)
    gene_metadata['eqtl'] = gene_metadata['eqtl'].replace({'TRUE': True}).fillna(False)

    rna_change = rna_change[['ensembl_gene_id', 'adj_p_val']]
    rna_change = rna_change.groupby('ensembl_gene_id')['adj_p_val'].agg('min').reset_index()

    gene_metadata = pd.merge(left=gene_metadata, right=rna_change, how='left', on='ensembl_gene_id')
    gene_metadata['adj_p_val'] = gene_metadata['adj_p_val'].fillna(-1)
    gene_metadata['rna_brain_change_studied'] = gene_metadata.apply(
        lambda row: False if row['adj_p_val'] == -1 else True, axis=1)
    gene_metadata['rna_in_ad_brain_change'] = gene_metadata.apply(
        lambda row: True if row['adj_p_val'] <= adjusted_p_value_threshold else False, axis=1)

    gene_metadata = gene_metadata[
        ['ensembl_gene_id', 'name', 'summary', 'alias', 'igap', 'symbol', 'eqtl', 'rna_in_ad_brain_change',
         'rna_brain_change_studied']]


    proteomics = proteomics.groupby('ensg')['cor_pval'].agg('min').reset_index()

    gene_metadata = pd.merge(left=gene_metadata, right=proteomics, how='left', left_on='ensembl_gene_id',
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

    gene_metadata = datasets['syn26868788']
    target_list = datasets['syn12540368']
    median_expression = datasets['syn12514804']
    druggability = datasets['syn13363443']

    # these are the interesting columns of the druggability dataset
    useful_columns = ['geneid', 'sm_druggability_bucket', 'safety_bucket', 'abability_bucket', 'pharos_class',
                      'classification', 'safety_bucket_definition', 'abability_bucket_definition']
    druggability = druggability[useful_columns]

    target_list = nest_fields(df=target_list,
                              grouping='ensembl_gene_id',
                              new_column='nominated_target')

    median_expression = nest_fields(df=target_list,
                              grouping='ensembl_gene_id',
                              new_column='median_expression')

    druggability = nest_fields(df=target_list,
                              grouping='ensembl_gene_id',
                              new_column='druggability')

    for dataset in [target_list, median_expression, druggability]:
        gene_metadata = pd.merge(left=gene_metadata, right=dataset, on='ensembl_gene_id', how='left')


    # create 'nominations' field
    gene_metadata['nominations'] = gene_metadata.apply(
        lambda row: len(row['nominated_target']) if isinstance(row['nominated_target'], list) else NaN, axis=1)

    # here we return gene_metadata because we preserved its fields and added to the dataframe
    return gene_metadata

    pass


def apply_custom_transformations(datasets: dict, dataset_name: str, dataset_obj: dict):

    if type(datasets) is not dict or type(dataset_name) is not str:
        return None

    print(dataset_obj)

    if dataset_name == "overall_scores":
        df = datasets['syn25575156']
        return transform_overall_scores(df=df)
    elif dataset_name == "team_info":
        return transform_team_info(datasets=datasets)
    elif dataset_name == "rnaseq_differential_expression":
        return transform_rna_seq_data(datasets=datasets,
                                      models_to_keep=dataset_obj['custom_transformations']['models_to_keep'],
                                      adjusted_p_value_threshold=dataset_obj['custom_transformations']['adjusted_p_value_threshold'])
    elif dataset_name == "network":
        return transform_network(datasets=datasets)
    elif dataset_name == 'gene_metadata':
        return transform_gene_metadata(datasets=datasets,
                                       adjusted_p_value_threshold=dataset_obj['custom_transformations']['adjusted_p_value_threshold'],
                                       protein_level_threshold=dataset_obj['custom_transformations']['protein_level_threshold'])
    elif dataset_name == 'gene_info':
        return transform_gene_info(datasets=datasets)
    else:
        return None