# Lime

This repository contains data processing notebooks and scripts used for
analyzing payment conversions. The Python script generated from the
`Conv.ipynb` notebook expects database connection credentials to be
provided via environment variables:

- `DB_HOST` – address of the MySQL server
- `DB_USER` – database user
- `DB_PASSWORD` – password for the user
- `DB_NAME` – name of the database (defaults to `payments_yakassa` if not
  set)

Ensure these variables are defined before running `Conv.py`.
