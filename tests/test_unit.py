import json
from src.jobs.spark_etl_forecast import transform_weather_data


def test_transform_weather_data_logic(spark, tmp_path):
    """
    Test the transformation using a temporary file to avoid the PicklingError.
    This occurs when attempting to serialize data from Python's memory to the JVM.
    """
    input_file = tmp_path / "test_data.json"

    data = {
        "park_id": "PARK_01",
        "raw_payload": {
            "hourly": {
                "time": ["2026-05-18T10:00", "2026-05-18T11:00", "2026-05-18T12:00"],
                "wind_speed_100m": [2.0, 10.0, 30.0]
            }
        }
    }

    with open(input_file, "w") as f:
        json.dump(data, f)


    df = spark.read.option("multiline", "true").json(str(input_file))

    result_df = transform_weather_data(df)
    results = sorted(result_df.collect(), key=lambda x: x["forecast_date"])

    assert results[0]["estimated_generation_mwh"] == 0.0
    assert results[1]["estimated_generation_mwh"] == 25.0
    assert results[2]["estimated_generation_mwh"] == 0.0