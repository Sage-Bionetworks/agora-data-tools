import synapseclient

def _login_to_synapse():
    syn = synapseclient.Synapse()
    syn.login()
    return syn

