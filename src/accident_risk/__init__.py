"""Urban traffic accident risk analysis - Airflow-agnostic processing package.

Every pipeline step is a plain Python function with an explicit `run(settings,
logical_date)` entry point so it can be executed by Airflow, by the CLI in the
processing container, or by pytest without any orchestration dependency.
"""

__version__ = "1.0.0"
