import synapseclient
import json
import os

syn = synapseclient.Synapse()
syn_token = os.environ.get("SYNAPSE_TOKEN")

if syn_token is None:
    raise ValueError("SYNAPSE_TOKEN environment variable not set.")

syn.login(authToken=syn_token)

# TODO: update to get parent from the test_config
files = syn.getChildren("syn17015333")

# Iterate through children of the parent
def get_md5s():
    """Get md5s of all files in the parent folder."""
    md5_dict = {}
    for child in files:
        if child["type"] == "org.sagebionetworks.repo.model.FileEntity":
            entity = syn.get(child["id"], downloadFile=False)  # Only fetch metadata
            md5_dict[entity.name] = entity.md5
    return md5_dict


if __name__ == "__main__":
    md5s = get_md5s()
    print(json.dumps(md5s, sort_keys=True))
