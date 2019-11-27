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
    def __init__(self):
        self.db_loc = ''
        self.conn = None
        self.tables = None
        self.target_table = None
        self.fields = None
        self.selected_fields = "*"
        self.filter_statement = ""
        self.groupby_statement = ""
        self.arrange_statement = ""
        self.distinct_statement = ""
        self.prior_query = None

    def connect(self,db_file=None):
        self.db_loc = db_file
        self.conn = conn=sqlite3.connect(os.path.expanduser(self.db_loc))
        self.clear()
        self.gather_tables()

    def is_connected(self):
        if self.conn is None:
            raise ValueError("No database connection established.")

    def gather_tables(self):
        '''
        Gather all available tables in the SQL database.
        '''
        self.is_connected()
        if self.tables is None:
            cursor = self.conn.cursor()
            tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.tables = [i[0] for i in tables]

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
        self.is_connected()
        self.fields = pd.read_sql(f"SELECT * FROM '{self.target_table}' LIMIT 1",self.conn).columns.values.tolist()

    def list_fields(self,print_span = 7):
        '''
        List all fields within a specific table
        '''
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
        self.is_connected()
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

    def clear_groupby(self):
        '''Clear filtered fields'''
        self.groupby_statement = ""

    def clear(self):
        '''
        Clear all table settings
        '''
        self.target_table = None
        self.fields = None
        self.selected_fields = "*"
        self.filter_statement = ""
        self.arrange_statement = ""
        self.distinct_statement = ""
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
        self.is_queued()
        if self.selected_fields == "*":
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

    def distinct(self):
        '''
        Reduce to only distinct entries from all selected variables.
        '''
        self.distinct_statement = "distinct"

    # Summarization/aggregation methods
    def group_by(self,query):
        '''
        Select variable(s) to group gy
        '''
        self.is_queued()
        self.grouped_vars = [v.strip() for v in query.split(",")]
        self.groupby_statement = "group by " + ", ".join(self.grouped_vars)

    def is_grouped(self):
        if len(self.grouped_vars) > 0:
            return True
        return False

    def mean(self,query=""):
        '''
        Calculate the mean value of the queried variables by the grouped by variables
        '''
        if self.is_grouped():
            if query == "":
                vars = self.grouped_vars
                front = ""
            else:
                vars = [v.strip() for v in query.split(",")]
                front = ", ".join(self.grouped_vars) + ", "
            self.selected_fields = front + ", ".join([f"avg({i}) as {i}_mean" for i in vars])

    def min(self,query=""):
        '''
        Calculate the min value of queried variabe by the grouped by variables
        '''
        if self.is_grouped():
            if query == "":
                vars = self.grouped_vars
                front = ""
            else:
                vars = [v.strip() for v in query.split(",")]
                front = ", ".join(self.grouped_vars) + ", "
            self.selected_fields = front + ", ".join([f"min({i}) as {i}_min" for i in vars])

    def max(self,query=""):
        '''
        Calculate the max value of queried variabe by the grouped by variables
        '''
        if self.is_grouped():
            if query == "":
                vars = self.grouped_vars
                front = ""
            else:
                vars = [v.strip() for v in query.split(",")]
                front = ", ".join(self.grouped_vars) + ", "
            self.selected_fields = front + ", ".join([f"max({i}) as {i}_max" for i in vars])

    def range(self,query=""):
        '''
        Calculate the range (min/max) value of the grouped by variables.
        '''
        if self.is_grouped():
            if query == "":
                vars = self.grouped_vars
                front = ""
            else:
                vars = [v.strip() for v in query.split(",")]
                front = ", ".join(self.grouped_vars) + ", "
            self.selected_fields = front + ", ".join([f"min({i}) as {i}_min, max({i}) as {i}_max" for i in vars])

    def sum(self,query=""):
        '''
        Calculate the sum value of queried variabe by the grouped by variables
        '''
        if self.is_grouped():
            if query == "":
                vars = self.grouped_vars
                front = ""
            else:
                vars = [v.strip() for v in query.split(",")]
                front = ", ".join(self.grouped_vars) + ", "
            self.selected_fields = front + ", ".join([f"sum({i}) as {i}_sum" for i in vars])

    def count(self):
        '''
        Count up the number of entries by the grouped variables.
        '''
        if self.is_grouped():
            self.selected_fields = ", ".join(self.grouped_vars) + ", count(*) as n"

    def prop(self):
        '''
        Count up the number of entries by the grouped variables.
        '''
        if self.is_grouped():
            self.selected_fields = ", ".join(self.grouped_vars) + f", 1.0 * count(*)/(select count(*) FROM '{self.target_table}') as prop"


    # Render data
    def collect(self):
        '''
        Execute constructed query on all available data.
        '''
        self.is_queued() # Ensure a table is queued .
        self.prior_query = pd.read_sql(f"""
                                       SELECT {self.distinct_statement}
                                       {self.selected_fields}
                                       FROM '{self.target_table}'
                                       {self.groupby_statement}
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
                                       SELECT {self.distinct_statement}
                                       {self.selected_fields}
                                       FROM '{self.target_table}'
                                       {self.filter_statement}
                                       {self.groupby_statement}
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

    def copy_to(self):
        '''
        Copy a data frame to the SQLite DB
        '''
        pass

    def delete_table(self):
        '''
        Delete a table from the connected SQLite DB
        '''
        pass

    # method attributes
    def __str__(self):
        self.gather_tables()
        msg = \
        f"""Connection Summary
        Database: {self.db_loc}
        No. Available Tables: {len(self.tables)}

        Current Query State:

            SELECT {self.distinct_statement}
                {self.selected_fields}
            FROM '{self.target_table}'
            {self.filter_statement}
            {self.groupby_statement}
            {self.arrange_statement}
        """.strip()
        return msg
