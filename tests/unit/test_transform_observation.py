from datetime import UTC, datetime

import pandas as pd
import pytest

from nimbus.common.schemas import ObservationRawPayload
from nimbus.transform.observation import explode_observation_payload


def _payload(api_response: dict[str, object]) -> ObservationRawPayload:
    return ObservationRawPayload(
        station="KDEN",
        observed_at=datetime(2026, 9, 17, 23, 53, tzinfo=UTC),
        api_response=api_response,
    )


def _value(df: pd.DataFrame, variable: str) -> float:
    row = df.loc[df["variable"] == variable, "value"]
    return float(row.iloc[0])


def test_converts_units_to_si() -> None:
    payload = _payload(
        {
            "temp": 27.2,
            "dewp": 10.0,
            "wspd": 7.0,  # knots
            "slp": 1014.1,
            "rawOb": "METAR KDEN 172353Z 28007KT 10SM SCT080 27/10 A3018 RMK AO2 SLP141",
        }
    )
    df = explode_observation_payload(payload, "live")

    assert _value(df, "temperature_2m") == pytest.approx(300.35)  # 27.2 degC -> K
    assert _value(df, "dew_point_2m") == pytest.approx(283.15)
    assert _value(df, "wind_speed_10m") == pytest.approx(7.0 * 0.514444)
    assert _value(df, "pressure_msl") == pytest.approx(101410.0)  # hPa -> Pa


def test_falls_back_to_altimeter_when_slp_missing() -> None:
    payload = _payload(
        {
            "temp": 20.0,
            "dewp": 15.0,
            "wspd": 5.0,
            "altim": 1013.0,  # no "slp" key at all
            "rawOb": "METAR KDEN 172353Z 05005KT 10SM 20/15 A2992",
        }
    )
    df = explode_observation_payload(payload, "live")

    assert _value(df, "pressure_msl") == pytest.approx(101300.0)  # altimeter fallback, hPa -> Pa


def test_missing_fields_become_nan_not_dropped() -> None:
    payload = _payload({"temp": 20.0, "rawOb": "METAR KDEN 172353Z 05005KT 10SM 20/M NCD"})
    df = explode_observation_payload(payload, "live")

    assert len(df) == 4  # all 4 variables still get a row
    assert df["value"].isna().sum() == 3  # dewp, wspd, pressure all missing


def test_variable_wind_direction_does_not_affect_wind_speed() -> None:
    # "VRB" direction (calm/shifting winds) - direction isn't a stored
    # variable, so wind speed extraction must be unaffected.
    payload = _payload(
        {
            "wdir": "VRB",
            "wspd": 3.0,
            "rawOb": "METAR KDEN 172353Z VRB03KT 10SM 20/15 A2992",
        }
    )
    df = explode_observation_payload(payload, "live")

    assert _value(df, "wind_speed_10m") == pytest.approx(3.0 * 0.514444)


def test_corrected_report_flagged_true() -> None:
    payload = _payload(
        {
            "temp": 20.0,
            "rawOb": "METAR KDEN 172353Z COR 05005KT 10SM 20/15 A2992",
        }
    )
    df = explode_observation_payload(payload, "live")

    assert df["is_corrected"].all()


def test_uncorrected_report_flagged_false() -> None:
    payload = _payload({"temp": 20.0, "rawOb": "METAR KDEN 172353Z 05005KT 10SM 20/15 A2992"})
    df = explode_observation_payload(payload, "live")

    assert not df["is_corrected"].any()


def test_correction_token_is_word_bounded_not_a_substring_match() -> None:
    # A station or remark token that merely *contains* "COR" must not be
    # mistaken for the COR indicator.
    payload = _payload(
        {"temp": 20.0, "rawOb": "METAR KDEN 172353Z 05005KT 10SM 20/15 A2992 RMK CORONA"}
    )
    df = explode_observation_payload(payload, "live")

    assert not df["is_corrected"].any()


def test_categorical_dtypes_set_explicitly() -> None:
    payload = _payload({"temp": 20.0, "rawOb": "METAR KDEN 172353Z 05005KT 10SM 20/15 A2992"})
    df = explode_observation_payload(payload, "backfill")

    assert isinstance(df["station"].dtype, pd.CategoricalDtype)
    assert isinstance(df["variable"].dtype, pd.CategoricalDtype)
    assert isinstance(df["ingestion_mode"].dtype, pd.CategoricalDtype)
    assert df["ingestion_mode"].iloc[0] == "backfill"
