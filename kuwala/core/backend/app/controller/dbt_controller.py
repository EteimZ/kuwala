import os
from pathlib import Path
import subprocess

import yaml


def create_empty_dbt_project(data_source_id: str, warehouse: str, target_dir: str):
    Path(target_dir).mkdir(parents=True, exist_ok=True)
    subprocess.call(
        f"dbt-init --client {data_source_id} --warehouse {warehouse} --target_dir {target_dir} --project_name "
        f"'kuwala' --project_directory {data_source_id} --profile_name 'kuwala'",
        shell=True,
    )

    profiles_file_path = f"{target_dir}/{data_source_id}/sample.profiles.yml"
    project_file_path = f"{target_dir}/{data_source_id}/dbt_project.yml"

    os.rename(profiles_file_path, profiles_file_path.replace("sample.", ""))
    os.remove(f"{target_dir}/{data_source_id}/packages.yml")

    with open(project_file_path, "r") as file:
        project_yaml = yaml.safe_load(file)

        file.close()

    project_yaml["config-version"] = 2
    project_yaml["model-paths"] = project_yaml.pop("source-paths")
    project_yaml["seed-paths"] = project_yaml.pop("data-paths")

    with open(project_file_path, "w") as file:
        yaml.safe_dump(project_yaml, file)
        file.close()
