# %% Test

db = tidysqlite()
db.connect(db_file="~/Dropbox/Dataverse/conflict_database.sqlite")


db.table("ucdp_ged_v17_1")
db.list_fields()
# db.select("country_txt,eventid:iday,addnotes")
# db.rename("country = country_txt, year = iyear")
# # db.filter("imonth == 7 and iday==2 and iyear == 2018")
# # db.arrange("country_txt")
# # db.clear()
db.head()
print(db)


# %%

# con = sqlite3.connect(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite"))
d = pd.read_sql("""SELECT
            max(cast((JulianDay(date_end) - JulianDay(date_start)) as Integer)) as diff_bt_start_end,
            count(*) as n_days
            FROM ucdp_ged_v17_1
            group by cast((JulianDay(date_end) - JulianDay(date_start)) as Integer)
            """,con)


d['prop'] = (d.n_days/d.n_days.sum()).round(3)
d['prop_cumsum'] = d.prop.cumsum().round(3)
d.to_csv("/Users/edunford/Desktop/ged_episode_census.csv",index=False)
