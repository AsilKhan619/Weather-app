"""Historical METAR from the Iowa Environmental Mesonet ASOS archive (brief
section 5, ADR 0001/0004). Same stations as the live feed, so verification is
consistent across live and backfilled data.

IEM returns CSV, not the aviationweather.gov JSON. `parse_iem_csv` normalizes
each row to the live shape (icaoId/obsTime/temp/dewp/wspd/slp/altim/rawOb) so
the existing `explode_observation_payload` transform is reused unchanged - one
code path, not two. The original row is kept under `_source` for lineage."""

import csv
import io
from datetime import UTC, date, datetime
from typing import Any

import httpx

from nimbus.common.http import patient_http_retry

IEM_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos.py"
INHG_TO_HPA = 33.8639

# report types 3 = routine METAR, 4 = SPECI (special observations)
_PARAMS: dict[str, Any] = {
    "data": ["tmpc", "dwpc", "sknt", "mslp", "alti", "metar"],
    "tz": "Etc/UTC",
    "format": "onlycomma",
    "latlon": "no",
    "elev": "no",
    "missing": "empty",
    "trace": "empty",
    "direct": "no",
    "report_type": [3, 4],
}


@patient_http_retry
def fetch_station_history(
    client: httpx.Client, station: str, start: date, end_exclusive: date
) -> str:
    """CSV text for [start, end_exclusive) in UTC. Half-open on purpose so
    adjacent chunks never overlap or leave a gap at midnight."""
    response = client.get(
        IEM_URL,
        params={
            **_PARAMS,
            "station": station,
            "sts": f"{start.isoformat()}T00:00Z",
            "ets": f"{end_exclusive.isoformat()}T00:00Z",
        },
    )
    response.raise_for_status()
    return response.text


def _num(raw: str | None) -> float | None:
    if raw is None or raw.strip() == "":
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_iem_csv(csv_text: str, station: str) -> list[dict[str, Any]]:
    """Normalize IEM rows to the live METAR JSON shape. Rows with no raw METAR
    text or an unparseable timestamp are skipped: without the text there is no
    stable event identity (brief section 6) and no correction flag to derive."""
    reports: list[dict[str, Any]] = []
    for row in csv.DictReader(io.StringIO(csv_text)):
        raw_text = (row.get("metar") or "").strip()
        valid = (row.get("valid") or "").strip()
        if not raw_text or not valid:
            continue
        try:
            observed_at = datetime.strptime(valid, "%Y-%m-%d %H:%M").replace(tzinfo=UTC)
        except ValueError:
            continue

        altimeter_inhg = _num(row.get("alti"))
        reports.append(
            {
                "icaoId": station,
                "obsTime": int(observed_at.timestamp()),
                "temp": _num(row.get("tmpc")),
                "dewp": _num(row.get("dwpc")),
                "wspd": _num(row.get("sknt")),
                "slp": _num(row.get("mslp")),
                "altim": None if altimeter_inhg is None else altimeter_inhg * INHG_TO_HPA,
                "rawOb": raw_text,
                "_source": {"provider": "iem_asos", "row": dict(row)},
            }
        )
    return reports
