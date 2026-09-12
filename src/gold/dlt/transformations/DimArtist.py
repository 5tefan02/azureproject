import dlt

@dlt.table(name="dimartist_staging")
def DimArtistStaging():
    df = spark.read.table("spotify_data.silver.dimartist")
    return df

dlt.create_streaming_table("dimartist")

dlt.create_auto_cdc_flow(
  target="dimartist",
  source="dimartist_staging",
  keys=["artist_id"],
  sequence_by="updated_at",
  stored_as_scd_type=2
)