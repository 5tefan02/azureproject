import dlt

@dlt.table(name = "dimtrack_staging")
def DimTrackStaging():
    df = spark.read.table("spotify_data.silver.dimtrack")
    return df

dlt.create_streaming_table("dimtrack")

dlt.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_staging",
  keys = ["track_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None, 
  once = False 
)