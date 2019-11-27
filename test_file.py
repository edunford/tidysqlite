# %% Test

db = tidysqlite()
db.connect(db_file="~/Dropbox/Dataverse/conflict_database.sqlite")
db.table("gtd-1970-2018")

# %%

db.group_by("country_txt,iyear")
# db.range("event_date")
db.prop()
db.head(10)

# %%
print(db)

# %%
# db.list_fields()
# db.select("iyear")
# db.rename("country = country_txt, year = iyear")
# # db.filter("imonth == 7 and iday==2 and iyear == 2018")
# # db.arrange("country_txt")
# # db.clear()
# db.distinct()
# db.head()


# %%
[f"max({i}) as {i}_max" for i in db.grouped_vars]





# %%

# con = sqlite3.connect(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite"))
pd.read_sql("""SELECT
            year,
            1.0 * count(*)/(select count(*) FROM ucdp_ged_v17_1) as prop
            FROM ucdp_ged_v17_1
            where year > 2000
            group by year
            order by year
            """,con)
