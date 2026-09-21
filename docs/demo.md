# A 5-minute demo of Nimbus

A script for showing the project to someone: what to run, what to say, and what they should see.
It assumes the stack is already up and loaded (do this beforehand - `make demo` takes about 15-25
minutes because it calls the real APIs):

```bash
cp .env.example .env
uv sync --extra ingestion --extra dashboard
make up
make demo                 # 30 days, 25 locations: bronze, silver, reconcile, gold, quality
make produce-forecasts ARGS=--once && make produce-observations ARGS=--once
make drain && make alerts ARGS=--drain      # one live cycle, then the anomaly detector
make dashboard            # leave it running on http://localhost:8501
```

Have two things open: the dashboard in a browser, and a terminal at the repository root.

The numbers below are what a real 30-day run produced (see [ADR 0005](decisions/0005-phase3-gold-quality-lineage.md)).
Yours will differ slightly because the providers publish new data every day - say "about", not
"exactly".

---

## 0:00 - The question (30 s)

> "Every weather app shows a forecast, but there are several global models and they disagree.
> Nimbus answers: **which one should you trust, where, and how far ahead?** It collects forecasts
> from three models and real observations from airport weather stations, scores every forecast
> against what actually happened, and ranks them."

Show the README's architecture diagram, and trace the path with a finger:
*APIs -> Kafka -> raw Parquet lake -> Postgres -> scored gold tables -> dashboard.*

## 0:30 - Real data, and it reconciles (45 s)

Terminal:

```bash
make reconcile
```

> "This is 1.5 million forecast rows and about 108,000 observations for 25 cities. The check replays
> the raw lake through the same transform and compares it with what's in the database - it's
> looking for rows that are missing *and* rows nothing can explain. `MATCH` means the database is
> exactly what a rebuild from the raw data would give."

You should see `[MATCH]` for both topics. Point at the "unparseable (-> DLQ)" line if it is not
zero: those are messages the quality gate rejected, and reconcile accounts for them.

## 1:15 - The answer: error grows with lead time (75 s)

Dashboard -> **Accuracy**. Choose *Temperature*, *last 30 days*, lead *1 day*, *All locations*.

> "Here's the leaderboard. For one-day-ahead temperature that month, ICON was closest, then ECMWF,
> then GFS - about 1.3, 1.5 and 1.5 degrees off on average. One month isn't a general verdict, and
> I'd say that out loud."

Scroll to **Error grows with lead time**.

> "And this is the sanity check that matters: the error climbs steadily as you forecast further
> ahead - roughly 1.4 degrees at one day to 2.2 at seven. If it didn't, I'd suspect a bug in my
> scoring, not celebrate."

Then **Best model by location**: "the winner changes by city - that's the point of measuring per
location."

## 2:30 - Forecast vs actual (30 s)

Dashboard -> **Forecast vs Actual**. Pick a city, *Temperature*, *1 day*.

> "Every dot is a forecast that was matched to the nearest station report within 30 minutes. Each
> model's line against the observed line - you can see them drift apart."

## 3:00 - Bad data is stopped, and I can prove it (60 s)

Dashboard -> **Pipeline Health**. Scroll to **Data quality**.

> "Every load passes schema checks. Physically impossible values are blocked and the message goes to
> a dead-letter queue; merely unusual values load and get flagged. In the real 30-day run this
> quarantined 3 of about 27,000 observations. I looked at one: the weather provider's own record was
> truncated mid-report, so its pressure came out as about 100 hectopascals instead of about 1,010. The gate caught a provider bug
> I didn't write."

Optional, if asked how I found bugs: *"The same checks also caught a unit bug of mine - I'd stored
observed pressure in hectopascals next to forecasts in pascals - before it corrupted a single
result."*

## 4:00 - Follow one event end to end (45 s)

Terminal:

```bash
make trace SAMPLE=forecast
```

> "Lineage. This one API response: which request, which Kafka partition and offset, which raw file, how
> many database rows it became, and which accuracy numbers it fed. If a number on the dashboard looks
> wrong, I can walk it back to the exact API call."

## 4:45 - Wrap up (15 s)

> "So: streaming ingestion, a replayable raw layer, a quality gate, an incremental scoring job that
> is safe to re-run - I checksummed both tables before and after a re-run on real data and they were
> identical - and a dashboard on top. Next are LLM-written briefings and an agent that can answer
> questions, both grounded in these tables."

---

## If someone asks...

| Question | Short answer |
|---|---|
| Is the data real? | Yes: Open-Meteo forecasts, aviationweather.gov and Iowa State's archive for observations. Fixtures exist only in tests. |
| What if the pipeline crashes mid-batch? | Offsets commit only after the write, and every write is idempotent, so a restart redelivers and nothing duplicates. There is an integration test that kills a consumer before it commits. |
| How do alerts work? | `make alerts`: three rules (a model run jumps, models disagree, an observation misses the forecast). It keeps no state in memory, so a restart loses nothing ([ADR 0006](decisions/0006-phase4-anomaly-detector.md)). |
| Why did pressure alerts disappear at Bogota and Mexico City? | The first real run alerted mostly there. Those stations don't report sea-level pressure, so I'd substituted the altimeter setting, which is wrong at altitude. I stopped doing that and the detector now ignores pressure above 300 m. |
| What are the weak spots? | One month of ranking isn't a verdict; thresholds for alerts are untuned; a full-history scoring build is extrapolated (~4 h), not measured; the dashboard hasn't been reviewed in a browser. See the README's Limitations. |
| Cost? | Free. The only optional paid piece is an LLM key, off by default. |

## Keeping it honest

- Do not quote numbers from this file as if freshly measured; re-read the dashboard.
- Freshness on **Pipeline Health** will say stations are stale if the live producers are not running.
  That is the correct answer, not a bug - say so rather than hiding the page.
- The **Lineage** page needs the bronze lake on local disk (`data/lake/bronze`); run the dashboard
  from the repository root.
