'''
Method for easy manipulation of a SQLite database using sqlite3 and pandas.

Motivation:
    Current methods to call in and explore a SQLite database are cumbersome, and unintuitive.
    For example, trying to list all the available tables in a SQLite database is not straightforward.
    The module aims to generate a handy wrapper for most main query function using R's dplyr
    syntax. In essence, tidysqlite aims to function like dbplyr.
'''
import pandas as pd
import sqlite3
import os

class tidysqlite:
    '''
    Method for easy manipulation of a SQLite database using sqlite3.
    '''
    def __init__(self,db_file=None):
        self.db_loc = db_file
        self.conn = conn=sqlite3.connect(os.path.expanduser(self.db_loc))
        self.tables = None
        self.target_table = None
        self.fields = None
        self.selected_fields = "*"
        self.filter_statement = ""
        self.arrange_statement = ""
        self.prior_query = None

    def gather_tables(self):
        '''
        Gather all available tables in the SQL database.
        '''
        if self.tables is None:
            cursor = self.conn.cursor()
            tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.tables = [i[0] for i in tables]

    def list_tables(self):
        '''
        List all available tables.
        '''
        self.gather_tables()
        for i in self.tables:
            print(i)

    def table(self,table_name=""):
        '''
        Target a specific table for query.
        '''
        if self.tables is  None:
            self.gather_tables()
        if table_name in self.tables:
            self.target_table = table_name
        else:
            print(f"{table_name} not in available tables.")

    def gather_fields(self):
        '''
        Gather all available fields
        '''
        self.fields = pd.read_sql(f"SELECT * FROM '{self.target_table}' LIMIT 1",self.conn).columns.values.tolist()

    def list_fields(self,print_span = 7):
        '''
        List all fields within a specific table
        '''
        if self.fields is None:
            self.gather_fields()
        cnt = 0
        print(f"Available fields in table '{self.target_table}'")
        for i in self.fields:
            if (cnt % print_span) == 0:
                print("")
            print(i,end =", ")
            cnt +=  1

    def is_queued(self,message=True):
        '''
        Helper method to determine if the data table is queued.
        '''
        if self.tables is None:
            self.gather_tables()
        if self.target_table is None:
            self.table(self.tables[0])
            if message:
                print(f"""No table queued. Queuing the first table in the table list: '{self.tables[0]}'""",end="\n\n")

    def expand_variable_range(self,var):
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

    def valid_variables(self,vars):
        '''[Aux] Only return variables that are in the fields set.
        Ensure that there are no invalid queried fields.'''
        valid = [v for v in vars if v in self.fields]
        if len(valid)==0:
            return "*"
        return valid

    def parse_query(self,query):
        '''
        [Aux] Parse string provided selection as query list of queried varibles.
        '''
        vars = query.split(",") # split via comma separated values
        vars = [v.strip() for v in vars] # remove white space
        flagged_ranges = [i for i in range(len(vars)) if ":" in vars[i]]
        if len(flagged_ranges) > 0:
            ext = 0
            for i in flagged_ranges:
                er = self.expand_variable_range(vars[i+ext])
                vars = vars[:i+ext] + er + vars[i+ext+1:]
                ext += len(er)-1
        return self.valid_variables(vars)

    def select(self,query):
        '''Select method to access specific fields for sql query'''
        self.is_queued()
        if self.fields is None:
            self.gather_fields()
        self.selected_fields = ", ".join(self.parse_query(query))

    def filter(self,query):
        '''
        Filter method to drop specific feature operations.
        '''
        self.filter_statement = "where " + query

    # Clear fields for analysis
    def clear_selected(self):
        '''Clear selected fields'''
        self.selected_fields = "*"

    def clear_filter(self):
        '''Clear filtered fields'''
        self.filter_statement = ""

    def clear_arrange(self):
        '''Clear filtered fields'''
        self.arrange_statement = ""

    def clear(self):
        '''
        Clear all table settings
        '''
        self.target_table = None
        self.fields = None
        self.selected_fields = "*"
        self.filter_statement = ""
        self.arrange_statement = ""
        self.prior_query = None

    def arrange(self,query):
        '''
        Arrange data by some variable(s) in desc/ascending order.

        Example:
            db.arrange("desc(var1),var2")
        '''
        def clean(var):
            if "desc(" in var:
                var = var.replace("desc(","").replace(")","").strip()
                var += " desc"
            else:
                var += " asc"
            return var

        entries = [clean(var.strip()) for var in query.split(',')]
        self.arrange_statement = "order by " + ", ".join(entries)

    def rename(self,query):
        '''
        Rename a field.
        '''
        def arrange_statement(entry):
            '''[AUX] Generate a rearranged statement.'''
            entry = [e.strip() for e in entry.split("=")]
            return (entry[1],entry[1] + " as " + entry[0])

        # Locate which fields set to alter.
        if self.selected_fields == "*":
            self.is_queued()
            self.gather_fields()
            avail_fields = self.fields
        else:
            avail_fields = self.selected_fields.split(", ")

        # Store alterations in dictionary
        entries = {}
        for e in query.split(","):
            if "=" in e:
                key,val = arrange_statement(e.strip())
                entries.update({key:val})

        # iterate through available fields and replace queried field names.
        db.selected_fields = ", ".join([entries[f] if f in entries else f  for f in avail_fields])


    def summarize(self):
        '''
        Summarize method for quick aggregation of variables.

        Requires both a group_by and summarize methodology.
        '''
        pass

    def collect(self):
        '''
        Execute constructed query on all available data.
        '''
        self.is_queued() # Ensure a table is queued .
        self.prior_query = pd.read_sql(f"""
                                       SELECT {self.selected_fields}
                                       FROM '{self.target_table}'
                                       {self.filter_statement}
                                       {self.arrange_statement}
                                       """.strip(),
                                       self.conn)
        return self.prior_query

    def head(self,n=5):
        '''
        Execute constructed query on first n entries of the data base.
        '''
        self.is_queued() # Ensure a table is queued .
        self.prior_query = pd.read_sql(f"""
                                       SELECT {self.selected_fields}
                                       FROM '{self.target_table}'
                                       {self.filter_statement}
                                       {self.arrange_statement}
                                       LIMIT {n}
                                       """.strip(),
                                       self.conn)
        return self.prior_query

    def custom_query(self,query=""):
        '''
        Method to build and specify your own query from scratch.
        '''
        self.prior_query = pd.read_sql(query,self.conn)
        return self.prior_query

    # method attributes
    def __str__(self):
        self.gather_tables()
        msg = \
        f"""Connection Summary
        Database: {self.db_loc}
        No. Available Tables: {len(self.tables)}

        Current Query State:

            SELECT
                {self.selected_fields}
            FROM '{self.target_table}'
            {self.filter_statement}
            {self.arrange_statement}
        """.strip()
        return msg

# %% Test

# db = tidysqlite(db_file="~/Dropbox/Dataverse/conflict_database.sqlite")
# # db.list_tables()
# db.table("gtd-1970-2018")
# # db.list_fields()
# db.select("country_txt,eventid:iday,addnotes")
# db.rename("country = country_txt, year = iyear")
# # db.filter("imonth == 7 and iday==2 and iyear == 2018")
# # db.arrange("country_txt")
# # db.clear()
# db.head()
# print(db)


# %%

# con = sqlite3.connect(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite"))
# pd.read_sql("""SELECT
#             year,
#             conflict_name,
#             deaths_a as new
#             FROM ucdp_ged_v17_1
#             where year > 2010 and deaths_a > 2
#             LIMIT 5
#             """,con)
