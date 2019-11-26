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
    def __init__(self,conn=None):
        self.conn = conn
        self.tables = None
        self.target_table = None
        self.features = None
        self.selected_features = "*"
        self.filter_conditions = None
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

    def queue_table(self,table_name=""):
        '''
        Flag a specific table for downstream operations.
        '''
        if self.tables is  None:
            self.gather_tables()
        if table_name in self.tables:
            self.target_table = table_name
        else:
            print(f"{table_name} not in available tables.")

    def gather_features(self):
        '''
        Gather all available features
        '''
        self.features = pd.read_sql(f"SELECT * FROM '{self.target_table}' LIMIT 1",self.conn).columns.values.tolist()

    def list_features(self,print_span = 7):
        '''
        List all features within a specific table
        '''
        if self.features is None:
            self.gather_features()
        cnt = 0
        print(f"Available features in table '{self.target_table}'")
        for i in self.features:
            if (cnt % print_span) == 0:
                print("")
            print(i,end =", ")
            cnt +=  1

    def is_queued(self):
        '''
        Helper method to determine if the data table is queued.
        '''
        if self.tables is None:
            self.gather_tables()
        if self.target_table is None:
            self.queue_table(self.tables[0])
            print(f'''
                  No table queued.
                  Queuing the first table in the table list: {self.tables[0]}
                  ''')

    def expand_variable_range(self,var):
        '''
        [AUX] Expand variable range from the string selection.
        '''
        var_range = var.split(":")
        add_vars = []; on = False
        for feature in self.features:
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
        '''[Aux] Only return variables that are in the features set.
        Ensure that there are no invalid queried features.'''
        return [v for v in vars if v in self.features]

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
        '''Select method to access specific features for sql query'''
        if self.features is None:
            self.gather_features()
        self.selected_features = ", ".join(self.parse_query(query))

    def clear_selected(self):
        '''Clear selected features'''
        self.selected_features = "*"

    def filter(self,query):
        '''
        Filter method to drop specific feature operations.
        '''
        self.filter_conditions = "where " + query

    def clear_filter(self):
        '''Clear filtered features'''
        self.filter_conditions = None

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
                                       SELECT {self.selected_features}
                                       FROM '{self.target_table}'
                                       {self.filter_conditions}
                                       """,
                                       self.conn)
        return self.prior_query

    def head(self,n=5):
        '''
        Execute constructed query on first n entries of the data base.
        '''
        self.is_queued() # Ensure a table is queued .
        self.prior_query = pd.read_sql(f"""
                                       SELECT {self.selected_features}
                                       FROM '{self.target_table}'
                                       {self.filter_conditions}
                                       LIMIT {n}
                                       """,
                                       self.conn)
        return self.prior_query

    def custom_query(self,query=""):
        '''
        Method to build and specify your own query from scratch.
        '''
        self.prior_query = pd.read_sql(query,self.conn)
        return self.prior_query

# %% Test


# db = tidysqlite(conn=sqlite3.connect(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite")))
# # db.list_tables()
# db.queue_table("gtd-1970-2018")
# # db.list_features()
# db.select("country_txt,eventid:iday,addnotes")
# db.filter("imonth == 7 and iday==2")
# db.head(n=10)
#
#
# # %%
#
# con = sqlite3.connect(os.path.expanduser("~/Dropbox/Dataverse/conflict_database.sqlite"))
# pd.read_sql("""SELECT
#             year,
#             conflict_name,
#             deaths_a
#             FROM ucdp_ged_v17_1
#             where year == 2010 and deaths_a == 2
#             LIMIT 5
#             """,con)
