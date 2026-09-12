import dlt

@dlt.table(name="dimdate_staging")
def DimDateStaging():
    df = spark.read.table("spotify_data.silver.dimdate")
    return df

dlt.create_streaming_table("dimdate")

dlt.create_auto_cdc_flow(
  target="dimdate",
  source="dimdate_staging",
  keys=["date_key"],
  sequence_by="date",
  stored_as_scd_type=2,
  track_history_column_list = None,
  name = None
)