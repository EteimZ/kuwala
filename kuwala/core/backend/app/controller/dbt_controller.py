import os
from pathlib import Path
import subprocess


def create_empty_dbt_project(data_source_id: str, warehouse: str, target_dir: str):
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    subprocess.call(
        f"dbt-init --client {data_source_id} --warehouse {warehouse} --target_dir {target_dir} --project_name "
        f"'kuwala' --project_directory {data_source_id} --profile_name 'kuwala'",
        shell=True,
    )

    profiles_file = f"{target_dir}/{data_source_id}/sample.profiles.yml"

    os.rename(profiles_file, profiles_file.replace("sample.", ""))
