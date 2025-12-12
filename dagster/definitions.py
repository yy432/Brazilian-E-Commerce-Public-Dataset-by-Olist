# definitions.py
from dagster import Definitions
from dagster_module2_group_project_orchestration.jobs.dagster_elt_pipeline import elt_pipeline_job

defs = Definitions(
    jobs=[elt_pipeline_job]
)
