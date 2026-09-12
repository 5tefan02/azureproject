import dlt

expectations = {
  "rule_1" : "user_id IS NOT NULL"
}

@dlt.table(name = "dimuser_staging")
@dlt.expect_all_or_drop(expectations)
def DimUserStaging():
    df = spark.read.table("spotify_data.silver.dimuser")
    return df

dlt.create_streaming_table(
  name="dimuser",
  expect_all_or_drop=expectations
  )

dlt.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_staging",
  keys = ["user_id"],
  sequence_by = "updated_at",
  stored_as_scd_type = 2,
  track_history_except_column_list = None,
  name = None, 
  once = False 
)