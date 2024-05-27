# Pandas
# security_master_pd = pd.DataFrame(
#     security_master,
#     columns=["security_id", "effective_start_date", "effective_end_date", *attributes],
# )

# security_master_pd_copy = security_master_pd.copy(deep=True)
# assert security_master_pd.equals(security_master_pd_copy)

# PyArrow
# cols = list(zip(*security_master))
# security_ids = pa.array(cols[0], type=pa.int8())
# effective_start_dates = pa.array(cols[1], type=pa.date64())
# effective_end_dates = pa.array(cols[2], type=pa.date64())
# asset_classes = pa.array(cols[3], type=pa.string())
# tickers = pa.array(cols[4], type=pa.string())
# names = pa.array(cols[5], type=pa.string())
# market_cap = pa.array(cols[6], type=pa.float64())
# gics_sectors = pa.array(cols[7], type=pa.string())
# gics_industries = pa.array(cols[8], type=pa.string())

# arrow_table = pa.table(
#     [
#         security_ids,
#         effective_start_dates,
#         effective_end_dates,
#         asset_classes,
#         tickers,
#         names,
#         market_cap,
#         gics_sectors,
#         gics_industries,
#     ],
#     names=["security_id", "effective_start_date", "effective_end_date", *attributes],
# )


# pq.write_table(arrow_table, "security_master.parquet")
# reloaded_table = pq.read_table("security_master.parquet")

# csv.write_csv(reloaded_table, "security_master.csv")
# reloaded_table = csv.read_csv("security_master.csv")
