'''
Method for easy manipulation of a SQLite database using sqlite3 and pandas.

Motivation:
    Current methods to call in and explore a SQLite database are cumbersome, and unintuitive. For example, trying to list all the available tables in a SQLite database is not as straightforward as it could be. The module aims to generate a handy wrapper for most main query function using R's dplyr syntax. In essence, tidysqlite aims to function like dbplyr in Python.
'''

import pandas as pd
import sqlite3
import os
from tabulate import tabulate

class tidyDB:
    '''
    Method for easy manipulation of a SQLite database using sqlite3.
    '''
    def __init__(self,db_file=""):
        self.db_loc = ""
        self.conn = None
        self.tables = None
        self.target_table = None
        self.fields = None
        self.pipe_status = True
        self.selected_fields = "*"
        self.filter_statement = ""
        self.groupby_statement = ""
        self.arrange_statement = ""
        self.distinct_statement = ""
        self.prior_query = None
        self.connect(db_file=db_file)

    def connect(self,db_file=""):
        """Establish a connection to an existing local SQLite database.

        Parameters
        ----------
        db_file : str
            File path to SQLite database object.

        Returns
        -------
        None
            Establishes a connection in the instantiated object.

        Raises
        ------
        FileExistsError
            When no connection file exists.

        Examples
        -------
        from tidysqlite import tidyDB
        db = tidyDB()
        db.connect("~/my_database.sqlite")
        """
        db_file_complete = os.path.expanduser(db_file)
        if os.path.exists(db_file_complete):
            self.db_loc = db_file_complete
            self.conn = sqlite3.connect(db_file_complete)
            self.tables = None
            self.target_table = None
            self.fields = None
            self.prior_query = None
            self.clear()
            self.gather_tables()
        else:
            raise FileExistsError(f"The file path {db_file_complete} does not exist. Please specify a new file path or create a new database with .create_database().")

    def is_connected(self):
        """Check if a connection to a SQLite database has been established.
        Serves as an internal check to ensure a connection is established before performing any query operations

        Returns
        -------
        None
            if database connection has been established.

        Raises
        ------
        ValueError
            When no connection file exists.

        Examples
        -------
        from tidysqlite import tidyDB
        db = tidyDB()
        db.is_connected()
        """
        if self.conn is None:
            raise ValueError("No database connection established.")

    def gather_tables(self):
        '''
        [Aux] Gather all available tables in the SQL database.
        '''
        self.is_connected()
        if self.tables is None:
            cursor = self.conn.cursor()
            tables = cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            self.tables = [i[0] for i in tables]

    def tbl(self,table_name=""):
        """Load a specific data from the connect SQLite database.

        Parameters
        ----------
        table_name : str
            Name of existing table in connected SQLite database.

        Returns
        -------
        type
            Description of returned object.

        Raises
        ------
        ValueError
            When no table is specified or the table specified does not exist.

        Examples
        -------
        from tidysqlite import tidyDB
        import pandas as pd

        # Create an example database
        db = tidyDB()
        db.create_database("example_db.sqlite")
        dat = pd.DataFrame(dict(foo=[1,2,3,4,5,6],
                                bar=['z','z','z','x','x','y'],
                                x = [.2,.4,.1,.8,.9,1],
                                y = [0,0,1,0,1,1]))
        db.create_table(dat,table_name="tableA")

        # Connect to the data base and table.
        db.connect("example_db.sqlite")
        db.tables
        db.tbl("tableA")
        """
        if self.tables is None:
            self.gather_tables()
        elif table_name in self.tables:
            self.target_table = table_name
        elif table_name == "":
            raise ValueError(f"No table specified.")
        else:
            raise ValueError(f"{table_name} not in available tables.")
        if self.pipe_status:
            return self

    def gather_fields(self):
        '''
        [Aux.] Gather all available fields
        '''
        self.is_connected()
        self.fields = pd.read_sql(f"SELECT * FROM '{self.target_table}' LIMIT 1",self.conn).columns.values.tolist()

    def list_fields(self,print_span = 7):
        """List all fields within the selected table.

        Parameters
        ----------
        print_span : int
            int specifying the number of fields that should be printed on each line.

        Returns
        -------
        str
            List of all available fields in selected table. If no table is selected, the the fields from the first table in the database is selected.


        Examples
        -------
        from tidysqlite import tidyDB
        import pandas as pd

        # Create an example database
        db = tidyDB()
        db.create_database("example_db.sqlite")
        dat = pd.DataFrame(dict(foo=[1,2,3,4,5,6],
                                bar=['z','z','z','x','x','y'],
                                x = [.2,.4,.1,.8,.9,1],
                                y = [0,0,1,0,1,1]))
        db.create_table(dat,table_name="tableA")

        # Connect to the data base and table.
        db.connect("example_db.sqlite")
        db.tables
        db.tbl("tableA")
        db.list_fields()
        """
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
        [AUX] Helper method to determine if the data table is queued.
        '''
        self.is_connected()
        if self.tables is None:
            self.gather_tables()
        if self.target_table is None:
            self.tbl(self.tables[0])
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
        vars = [v.strip() for v in query.split(",")] # remove white space
        flagged_ranges = [i for i in range(len(vars)) if ":" in vars[i]]
        if len(flagged_ranges) > 0:
            ext = 0
            for i in flagged_ranges:
                er = self.expand_variable_range(vars[i+ext])
                vars = vars[:i+ext] + er + vars[i+ext+1:]
                ext += len(er)-1
        return self.valid_variables(vars)

    def select(self,query):
        """.select() method reduces data to only the specified fields.

        In practices, it constructs the SELECT portion of the SQLit query.

        Parameters
        ----------
        query : str


        Returns
        -------
        type
            Description of returned object.

        """
        '''Select method to access specific fields for sql query'''
        self.is_queued()
        if self.fields is None:
            self.gather_fields()
        self.selected_fields = ", ".join(self.parse_query(query))
        if self.pipe_status:
            return self

    def pipe_on(self):
        '''
        Determine if fluid programming should be allowed. That is, the passing of the object instantiation. Allow for a more seemless manipulation of commands.

        Default = T
        '''
        self.pipe_status = True
        self.clear()

    def pipe_off(self):
        '''
        Determine if fluid programming should be turned off.
        '''
        self.pipe_status = False

    def filter(self,query):
        '''
        Filter method to drop specific feature operations.
        '''
        self.filter_statement = "where " + query
        if self.pipe_status:
            return self

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
        if self.pipe_status:
            return self

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
        self.selected_fields = ", ".join([entries[f] if f in entries else f  for f in avail_fields])
        if self.pipe_status:
            return self

    def distinct(self):
        '''
        Reduce to only distinct entries from all selected variables.
        '''
        self.distinct_statement = "distinct"
        if self.pipe_status:
            return self

    # Summarization/aggregation methods
    def group_by(self,query):
        '''
        Select variable(s) to group gy
        '''
        self.is_queued()
        self.grouped_vars = [v.strip() for v in query.split(",")]
        self.groupby_statement = "group by " + ", ".join(self.grouped_vars)
        if self.pipe_status:
            return self

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
        if self.pipe_status:
            return self

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
        if self.pipe_status:
            return self

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
        if self.pipe_status:
            return self

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
        if self.pipe_status:
            return self

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
        if self.pipe_status:
            return self

    def count(self):
        '''
        Count up the number of entries by the grouped variables.
        '''
        if self.is_grouped():
            self.selected_fields = ", ".join(self.grouped_vars) + ", count(*) as n"
        if self.pipe_status:
            return self

    def prop(self):
        '''
        Count up the number of entries by the grouped variables.
        '''
        if self.is_grouped():
            self.selected_fields = ", ".join(self.grouped_vars) + f", count(*) as n, 1.0 * count(*)/(select count(*) FROM '{self.target_table}') as prop"
        if self.pipe_status:
            return self

    # Clear fields for analysis
    def unselect(self):
        '''Clear selected fields'''
        self.selected_fields = "*"
        if self.pipe_status:
            return self

    def unfilter(self):
        '''Clear filtered fields'''
        self.filter_statement = ""
        if self.pipe_status:
            return self

    def unarrange(self):
        '''Clear filtered fields'''
        self.arrange_statement = ""
        if self.pipe_status:
            return self

    def ungroup(self):
        '''Clear filtered fields'''
        self.groupby_statement = ""
        if self.pipe_status:
            return self

    def clear(self):
        '''
        Clear all table settings
        '''
        # self.target_table = None
        # self.fields = None
        # self.prior_query = None
        self.selected_fields = "*"
        self.filter_statement = ""
        self.arrange_statement = ""
        self.distinct_statement = ""
        self.groupby_statement = ""

    # Render data
    def collect(self):
        '''
        Execute constructed query on all available data.
        '''
        self.is_queued() # Ensure a table is queued.
        self.prior_query = pd.read_sql(f"""
                                       SELECT {self.distinct_statement}
                                       {self.selected_fields}
                                       FROM '{self.target_table}'
                                       {self.groupby_statement}
                                       {self.filter_statement}
                                       {self.arrange_statement}
                                       """.strip(),
                                       self.conn)
        if self.pipe_status:
            self.clear()
            self.target_table = None
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
        if self.pipe_status:
            self.clear()
            self.target_table = None
        return self.prior_query

    def custom_query(self,query=""):
        '''
        Method to build and specify your own query from scratch.
        '''
        self.prior_query = pd.read_sql(query,self.conn)
        return self.prior_query

    def create_table(self,data=None,table_name="",append=False,overwrite=False):
        '''
        Copy a data frame to the SQLite DB.
        '''
        self.gather_tables()
        rule = "fail"
        if table_name in self.tables:
            if overwrite:
                rule = "replace"
        else:
            if append:
                rule = "append"
        data.to_sql(table_name, self.conn, if_exists=rule, index = False)

    def create_database(self,path=""):
        '''
        Create a new SQLlite DB
        '''
        sqlite3.connect(path) # first treat new database
        self.connect(path) # then connect to it

    def delete_table(self,table_name=""):
        '''
        Delete a table from the connected SQLite DB
        '''
        out = input(f""" Do you really want to delete {table_name}? Y: yes N: No""")
        if out == "Y" or out.lower() == "yes":
            cursor = self.conn.cursor().execute(f"DROP TABLE {table_name};")
            self.tables = [t for t in self.tables if t != table_name]

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

    def __repr__(self):
        self.gather_tables()
        if self.target_table is None:
            msg = f"Database: {self.db_loc}\n" + "Tables:\n\t" + "\n\t".join([i for i in self.tables])
        else:
            tmp = self.head(n=10)
            self.prior_query = None # don't save

            # Isolate the different data types for each field
            cols = tmp.columns.values.tolist()
            dtypes = [f"{d.str}" for d in tmp.dtypes.values.tolist()]
            new_cols = [val + "\n(" + dtypes[i] + ")" for i, val in enumerate(cols)]

            # Generate print for all left over fields.
            cnt = 0; store = []; ongoing = True
            if len(new_cols[5:]) > 0:
                out = f"\nwith {len(new_cols[5:])} additional variables:\n"
                for i in [c.replace("\n"," ") for c in new_cols[5:]]:
                    if cnt == 3:
                        out += ", ".join(store) + "\n"
                        cnt = 0; store = []
                        ongoing = False
                    else:
                        store.append(i)
                        cnt += 1
                        ongoing = True
            else:
                out = ""

            if ongoing:
                out += ", ".join(store) + "\n"

            # Clear target table
            self.target_table = None

            # generate table
            msg = tabulate(tmp.iloc[:,:5],headers=new_cols[:5],
                           tablefmt='plain',showindex=False,
                           missingval=".",stralign="center") + out
        return msg
