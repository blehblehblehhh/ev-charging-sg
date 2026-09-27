# EV Charger Availability — Singapore

Primary data collection for our COR1305 team project. A GitHub Action polls
LTA DataMall every 30 minutes and commits the result here. Nobody's laptop
needs to be on.

## Setup — do this once (about 15 minutes)

**1. Create the repo**

On github.com: **New repository** → name it something like `ev-charging-sg` →
**Public** (Actions are free and unlimited on public repos) → Create.

**2. Add the files**

Upload these four, keeping the folder structure:

```
ev_collect.py
README.md
.gitignore
.github/workflows/collect.yml
```

Easiest way if you've never used git: on the repo page click
**Add file → Upload files**, drag them in, commit. For the workflow file use
**Add file → Create new file** and type the path
`.github/workflows/collect.yml` — GitHub makes the folders for you.

**3. Add the API key as a secret**

Repo → **Settings** → **Secrets and variables** → **Actions** →
**New repository secret**

- Name: `LTA_KEY`
- Secret: your LTA DataMall **API** Account Key (not the SDK one)

Do NOT put the key in a file in the repo. `.gitignore` blocks `lta_key.txt`
so you can't do it by accident.

**4. Test it**

Repo → **Actions** tab → **Collect EV charger availability** →
**Run workflow**. It takes about a minute. Green tick means it worked, and a
file appears at `data/ev_YYYY-MM-DD.csv.gz`.

If it's red, click the run to see the log. A 401 means the key is wrong or
it's the SDK key.

**5. Nothing else**

It now runs every 30 minutes on its own. Check back in a few days.

## Getting the data out

```
git pull
python3 ev_collect.py aggregate --out data
```

That reads every daily file and writes `data/hourly.csv` — one row per
connector per hour, with `occupancy_rate` and `uptime_rate` computed. That's
the file that goes into the model.

## Narrowing to our study sites

The full network is about 11,500 connectors, which is more than we need and
more than Excel will hold. Once we've picked sites, make a text file listing
the `station_key` or `postal_code` values to keep, one per line:

```
179024|CLARKE QUAY
511142
```

Commit it as `scope.txt`, then change the workflow's run line to:

```
run: python ev_collect.py once --dir data --scope scope.txt
```

## What's in the data

One row per connector per poll.

| Column | Meaning |
|---|---|
| `ts_local`, `date`, `hour` | when we polled |
| `feed_updated` | LTA's own timestamp — we skip writing if it hasn't moved |
| `station_key`, `postal_code`, `station_name`, `address` | which site |
| `latitude`, `longitude` | for distance-to-competitor calculations |
| `operator` | SP Mobility, Charge+, CDG ENGIE, Shell, Strides YTL, … |
| `plug_type`, `current`, `power_rating_kw` | Type 2 / Combo 2, AC or DC, kW |
| `price`, `price_type` | $/kWh, to four decimal places |
| `ev_cp_id` | unique connector ID |
| `status_label` | `occupied`, `available`, or `out_of_service` |

Occupancy = occupied ÷ (occupied + available). Out-of-service connectors are
excluded from that and tracked separately as `uptime_rate`.

## Things that can go wrong

**Runs stop after 60 days of no repo activity.** GitHub disables scheduled
workflows on idle repos. Every successful run commits, so this won't bite us
inside the project, but if we pause collection for a long time, re-enable it
in the Actions tab.

**Scheduled runs fire late or occasionally get skipped.** Normal for GitHub
cron under load. We aggregate to hourly so it doesn't matter.

**Never commit the key.** If it ever leaks, regenerate it on DataMall and
update the secret.

## Source

LTA DataMall, EV Charging Points Batch (`EVCBatch`) — see the API User Guide,
section 2.29. The endpoint returns a temporary download link that expires in
15 minutes; the script follows it automatically. Feed refreshes every 5
minutes, so 30-minute polling is comfortably inside the useful resolution.

Attribute LTA DataMall as the source in the report.
