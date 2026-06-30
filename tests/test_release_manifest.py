import pytest
from unittest import mock
from unittest.mock import patch
import synapseclient
import pandas as pd
from agoradatatools.etl import load, utils
from agoradatatools import process

STAGING_PATH = "./staging"


class TestReleaseManifest:
    """Tests for the release_manifest function."""

    config_path = "./path/to/config"
    destination = "destination"
    team_images_id = "syn987"
    manifest_path = "path/to/csv"
    manifest_df = pd.DataFrame({"id": ["a", "b", "c"]})

    @pytest.fixture(scope="function", autouse=True)
    def setup_method(self, syn: synapseclient.Synapse):
        self.patch_login = patch.object(
            utils, "_login_to_synapse", return_value=syn
        ).start()
        self.patch_get_config = patch.object(
            utils,
            "_get_config",
            return_value={
                "destination": self.destination,
                "staging_path": STAGING_PATH,
                "team_images_id": self.team_images_id,
            },
        ).start()
        self.patch_create_temp_location = patch.object(
            load, "create_temp_location", return_value=None
        ).start()
        self.patch_create_data_manifest = patch.object(
            process,
            "create_data_manifest",
            return_value=self.manifest_df,
        ).start()
        self.patch_df_to_csv = patch.object(
            load, "df_to_csv", return_value=self.manifest_path
        ).start()
        self.patch_upload_manifest_and_dataversion = patch.object(
            process,
            "upload_manifest_and_dataversion",
            return_value=("syn123", 1),
        ).start()
        yield
        mock.patch.stopall()

    def test_release_manifest(self, syn: synapseclient.Synapse) -> None:
        """Test that release_manifest creates the manifest locally and uploads it to Synapse."""
        # WHEN release_manifest is called
        process.release_manifest(config_path=self.config_path)

        # THEN config and staging are set up correctly
        self.patch_get_config.assert_called_once_with(config_path=self.config_path)
        self.patch_create_temp_location.assert_called_once_with(
            staging_path=STAGING_PATH
        )

        # AND the manifest CSV is created locally
        self.patch_create_data_manifest.assert_called_once_with(
            syn=syn, parent=self.destination
        )
        self.patch_df_to_csv.assert_called_once_with(
            df=self.manifest_df,
            staging_path=STAGING_PATH,
            filename="data_manifest.csv",
        )

        # AND the manifest and dataversion.json are uploaded to Synapse
        self.patch_upload_manifest_and_dataversion.assert_called_once_with(
            syn=syn,
            manifest_path=self.manifest_path,
            manifest_df=self.manifest_df,
            destination=self.destination,
            team_images_id=self.team_images_id,
            staging_path=STAGING_PATH,
        )

    def test_release_manifest_without_team_images_id(
        self, syn: synapseclient.Synapse
    ) -> None:
        """Test that release_manifest works when team_images_id is absent from config."""
        # WHEN the config does not include a team_images_id
        self.patch_get_config.return_value = {
            "destination": self.destination,
            "staging_path": STAGING_PATH,
        }
        process.release_manifest(config_path=self.config_path)

        # THEN upload_manifest_and_dataversion is called with team_images_id=None
        self.patch_upload_manifest_and_dataversion.assert_called_once_with(
            syn=syn,
            manifest_path=self.manifest_path,
            manifest_df=self.manifest_df,
            destination=self.destination,
            team_images_id=None,
            staging_path=STAGING_PATH,
        )
