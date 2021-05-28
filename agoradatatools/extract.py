import agoradatatools.utils as utils
import pandas as pd

'''Reads in a csv file into a dataframe'''
def read_csv_to_df(syn_id: str, syn=None ):

    if syn is None:
        syn = utils._login_to_synapse()

    entity = syn.get(syn_id)
    csv_as_df = pd.read_csv(entity.path)

    return csv_as_df