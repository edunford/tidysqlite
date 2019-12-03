# %% Test
db = tidyDB(db_file="~/Dropbox/Dataverse/conflict_database.sqlite")
db.select_table("gtd-1970-2018")
db.pipe_on()

db.select("iyear,country_txt")


# %%
db .\
 filter("iyear == 2000") .\
 select("iyear,country_txt") .\
 distinct() .\
 arrange("desc(country_txt)") .\
 group_by("country_txt") .\
 prop() .\
 arrange("prop") .\
 head()

# %%

class tt:

    def __init__(self,a):
        self.a = a

    def __add__(self, b):
        return self.a + b.a

a = tt(1)
a + a


def t(*args):
    try:
        yy = args
        print(yy)
    except:
        print(yy)



def tt(**kwargs):
    for u in kwargs.items():
        print(u)

tt(this)



# %%
db.fields
self = db
[v for v in vars if v in self.fields]

import re
re.match(r'^count', [r"country_text","this"])

w = "country_txt"
w.find("count")

def valid_variables(vars):
    '''[Aux] Only return variables that are in the fields set.
    Ensure that there are no invalid queried fields.'''
    valid = [v for v in vars if v in self.fields]
    if len(valid)==0:
        return "*"
    return valid

def expand_variable_range(var):
    '''
    [AUX] Expand variable range from the string selection.
    '''
    var_range = var.split(":")
    add_vars = []; on = False
    for feature in self.fields:
        if feature == var_range[0]:
            on = True
            add_vars.append(feature)
        elif feature == var_range[1]:
            on = False
            add_vars.append(feature)
            break
        elif on:
            add_vars.append(feature)
    return add_vars

query= "iyear,country_txt,*"
def parse_query(query):
    '''
    [Aux] Parse string provided selection as query list of queried varibles.
    '''
    vars = [v.strip() for v in query.split(",")] # remove white space
    flagged_ranges = [i for i in range(len(vars)) if ":" in vars[i]] # detect ranges
    flagged_fuzzy = [i for i in range(len(vars)) if "*" in vars[i]] # detect fuzzy
    if len(flagged_ranges) > 0:
        ext = 0
        for i in flagged_ranges:
            er = expand_variable_range(vars[i+ext])
            vars = vars[:i+ext] + er + vars[i+ext+1:]
            ext += len(er)-1
    if len(flagged_fuzzy) > 0:
        for i in flagged_fuzzy:
            if vars[i] == "*":
                everything = [v for v in self.fields if v not in vars]
                vars = vars[:i] + everything + vars[i+1:]

    return vars

parse_query("iyear,country_txt")

"^yy"[0]


[ i for i in self.fields if i[:2] == ]


# %%

# Isolate the different data types for each field
tmp = self.head(n=10)
self.prior_query = None
cols = tmp.columns.values.tolist()
dtypes = [f"{d.str}" for d in tmp.dtypes.values.tolist()]
new_cols = [val + "\n(" + dtypes[i] + ")" for i, val in enumerate(cols)]

# Generate print for all left over fields.
cnt = 0; store = []
out = f"\nwith {len(new_cols[5:])} additional variables:\n"
for i in [c.replace("\n"," ") for c in new_cols[5:]]:
    if cnt == 3:
        out += ", ".join(store) + "\n"
        cnt = 0; store = []
    else:
        store.append(i)
        cnt += 1

msg = tabulate(tmp.iloc[:,:5],headers=new_cols[:5],
               tablefmt='plain',showindex=False,
               missingval=".",stralign="center")
msg += out

print(msg)

# %%

help(tabulate)

# db.list_fields()
db.select("iyear,country_txt")
db.rename("Year = iyear")
# db.rename("country = country_txt, year = iyear")
# # db.filter("imonth == 7 and iday==2 and iyear == 2018")
# # db.arrange("country_txt")
# # db.clear()
# db.distinct()
db.head()


# %%
[f"max({i}) as {i}_max" for i in db.grouped_vars]





# %%

# con = sqlite3.connect(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite"))
pd.read_sql("""SELECT * FROM "dbstat" WHERE name='TABLENAME';""",con)

os.path.exists(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite"))


# %%
# conn = sqlite3.connect("testDB.sqlite")
# cursor = conn.cursor().execute(f"DROP TABLE {};")

#
# D = pd.DataFrame(dict(A = [1,2,3,4]))
# D.to_sql('test_a', conn, if_exists='append', index = False)
#
# db.copy_to()
#
#
# db = tidysqlite()
# db.connect("testDB.sqlite")
# db.tables
# db.select_table("test_a")
# db.head()
# db.delete_table("test_a")
print(db)
