import pandas as pd

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
    :param column_map: a dict with the mappoing for the columns to be renamed
    :return: a dataframe
    """
    try:
        df = df.rename(columns=column_map)
    except TypeError:
        print("Column mapping must be a dictionary")
        return df

    return df


def transform_overall_scores(df: pd.core.frame.DataFrame) -> pd.core.frame.DataFrame:
    interesting_columns = ['ensg', 'genename', 'logsdon', 'geneticsscore', 'omicsscore', 'literaturescore',
                           'flyneuropathscore']

    df['logsdon'] = df['logsdon'] - df['flyneuropathscore']
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
    else:
        return None