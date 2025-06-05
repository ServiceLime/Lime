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

## Fetching daily AppMetrica installs

The repository also includes `appmetrica_report.py` which retrieves the
number of installations for the application **4661140** from Yandex AppMetrica.
The script connects to the AppMetrica Logs API and counts organic and total
installs for a selected day.

Before running it, define the API token via the `APPMETRICA_TOKEN` environment
variable:

```bash
export APPMETRICA_TOKEN=<your_api_token>
python appmetrica_report.py 2025-06-01
```

If no date is passed, the script uses yesterday's date.
