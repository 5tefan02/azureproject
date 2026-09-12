import dlt

@dlt.table(name="factstream_staging")
def FactStreamStaging():
    df = spark.read.table("spotify_data.silver.factstream")
    return df

dlt.create_streaming_table("factstream")

dlt.create_auto_cdc_flow(
  target="factstream",
  source="factstream_staging",
  keys=["stream_id"],
  sequence_by="stream_timestamp",
  stored_as_scd_type=1
)