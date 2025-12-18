# Data Directory

Centralized location for generated artifacts.

- raw/: Original downloads or source inputs
- parquet/: ETL outputs
- species/: Filtered species-specific Parquet
- tmp/: Scratch/temp during runs
- logs/: Run logs or metrics exports

Commit policy: data artifacts are ignored by git; only this README is tracked.
