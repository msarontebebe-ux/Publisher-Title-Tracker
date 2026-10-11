# Archived: Windows Task Scheduler scripts

These scripts registered the pipeline with the **Windows Task Scheduler** on the
developer's laptop (`python orchestrator.py ...`).

They are **superseded** by GitLab CI (`.gitlab-ci.yml` → `scheduled-pipeline`
job), which runs the same `python orchestrator.py --incremental` on the
university's servers on a daily schedule — independent of any laptop being
powered on. This is what makes the "extract increments at set intervals"
requirement reproducible and always-on.

They are kept here only for historical reference and can be safely deleted.
To run the pipeline locally, invoke the orchestrator directly:

```
python orchestrator.py --incremental
```
