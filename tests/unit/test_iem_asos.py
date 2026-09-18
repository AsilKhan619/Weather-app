from datetime import UTC, date, datetime
from pathlib import Path

import httpx
import pytest

from nimbus.common.schemas import ObservationRawPayload
from nimbus.ingestion.iem_asos import INHG_TO_HPA, fetch_station_history, parse_iem_csv
from nimbus.transform.observation import explode_observation_payload

FIXTURE = (Path(__file__).resolve().parents[1] / "fixtures" / "iem_asos_sample.csv").read_text()


def _payload(report: dict[str, object]) -> ObservationRawPayload:
    obs_time = report["obsTime"]
    assert isinstance(obs_time, int)
    return ObservationRawPayload(
        station="KDEN",
        observed_at=datetime.fromtimestamp(obs_time, tz=UTC),
        api_response=report,
    )


def test_normalizes_rows_to_the_live_metar_shape() -> None:
    reports = parse_iem_csv(FIXTURE, "KDEN")

    assert len(reports) == 3
    first = reports[0]
    assert first["icaoId"] == "KDEN"  # the ICAO id we asked for, not IEM's "DEN"
    assert first["obsTime"] == int(datetime(2024, 6, 1, 0, 53, tzinfo=UTC).timestamp())
    assert first["temp"] == pytest.approx(20.56)
    assert first["wspd"] == pytest.approx(5.0)  # knots, like the live API
    assert first["slp"] == pytest.approx(1011.4)
    assert first["altim"] == pytest.approx(30.03 * INHG_TO_HPA)  # inHg -> hPa
    assert first["rawOb"].startswith("KDEN 010053Z")
    assert first["_source"]["provider"] == "iem_asos"


def test_empty_fields_become_none_not_zero() -> None:
    third = parse_iem_csv(FIXTURE, "KDEN")[2]

    assert third["slp"] is None
    assert third["wspd"] is None
    assert third["altim"] == pytest.approx(30.04 * INHG_TO_HPA)


def test_rows_without_metar_text_or_a_valid_time_are_skipped() -> None:
    csv_text = (
        "station,valid,tmpc,dwpc,sknt,mslp,alti,metar\n"
        "DEN,2024-06-01 00:53,20,10,5,1011,30.0,\n"
        "DEN,not-a-time,20,10,5,1011,30.0,KDEN 010053Z 06005KT\n"
        "DEN,2024-06-01 01:53,20,10,5,1011,30.0,KDEN 010153Z 06005KT\n"
    )
    assert len(parse_iem_csv(csv_text, "KDEN")) == 1


def test_normalized_rows_flow_through_the_existing_observation_transform() -> None:
    df = explode_observation_payload(_payload(parse_iem_csv(FIXTURE, "KDEN")[0]), "backfill")

    by_var = df.set_index("variable")["value"]
    assert by_var["temperature_2m"] == pytest.approx(20.56 + 273.15)
    assert by_var["wind_speed_10m"] == pytest.approx(5.0 * 0.514444)
    assert by_var["pressure_msl"] == pytest.approx(1011.4)  # true SLP preferred
    assert (df["ingestion_mode"] == "backfill").all()


def test_a_correction_in_iem_text_is_still_detected() -> None:
    csv_text = (
        "station,valid,tmpc,dwpc,sknt,mslp,alti,metar\n"
        "DEN,2024-06-01 00:53,20,10,5,1011,30.0,KDEN 010053Z COR 06005KT 10SM 20/10 A3000\n"
    )
    df = explode_observation_payload(_payload(parse_iem_csv(csv_text, "KDEN")[0]), "backfill")

    assert df["is_corrected"].all()


def test_fetch_uses_a_half_open_utc_window_for_one_station() -> None:
    captured: dict[str, list[str]] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        for name in ("station", "sts", "ets", "report_type"):
            captured[name] = request.url.params.get_list(name)
        return httpx.Response(200, text="station,valid\n")

    with httpx.Client(transport=httpx.MockTransport(handler)) as client:
        fetch_station_history(client, "EGLL", date(2024, 6, 1), date(2024, 6, 8))

    assert captured["station"] == ["EGLL"]
    assert captured["sts"] == ["2024-06-01T00:00Z"]
    assert captured["ets"] == ["2024-06-08T00:00Z"]  # exclusive end
    assert captured["report_type"] == ["3", "4"]  # METAR and SPECI
