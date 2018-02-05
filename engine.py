""" Mini SQL Engine """

import sys
import errno
import os
import copy
import sqlparse
from sqlparse.tokens import Keyword, Wildcard

# Keyword.DML ---> INSERT, SELECT, UPDATE
# Keyword.DDL ---> CREATE, ALTER, DROP

"""
    AVAILABLE COMMANDS
    ------------------
    1. CREATE TABLE table_name(col1, col2, col3, ......)
    2. DROP TABLE table_name
    3. SELECT * FROM table1, table2
    4. SELECT col1, col2, col3 FROM table1, table2
    5. SELECT col1, col2, col3 FROM table1, table2 WHERE A = 10 and B = 5 or C = 10
    6. SELECT col1, col2, col3 FROM table1, table2 WHERE table1.A = table2.A
            and table2.B = table1.C or table1.A = table2.C
    7. SELECT col1, col2 FROM table1, table2 WHERE A = 10 and table1.C = table2.A
    8. SELECT distinct col1, col2 FROM table1, table2 WHERE A = 10 and table1.C = table2.A
    9. SELECT count(col1) from table1
    Muliple Table Join
    Multiple AND/OR Conditions -- with or without paranthesis
    Joining Column is not printed only in case of a simple equijoin of two tables with a single condition
    For all other types of joins, all the columns that are asked to be projected are projected.
"""

# List of Table Names
TABLE_NAMES_LIST = []

# List of Lists of Columns of Tables
COLUMNS_LIST = []

# Columns Dict(key == table_name and value == list_of_columns_of_table_with_name_table_name)
COLUMNS_DICT = {}

# Records Dict(key == table_name and value == list_of_records_of_table_with_name_table_name)
RECORDS_DICT = {}



# ----------PARSE STATIC DATA---------------
def parse_metadata():
    """
        Parse the metadata about tables
        1. Fill TABLE_NAMES_LIST
        2. FILL COLUMNS_LIST
        3. FILL_COLUMNS_DICT
    """

    with open("metadata.txt", "r") as f:
        tables_data = f.read().split("<begin_table>\n")

        # first entry of tables_data is empty
        del tables_data[0]

        for table_data in tables_data:
            """
                table_data will look like the following
                table_name
                col1
                col2
                col3
                <end_table>
            """
            table_name = table_data.split("\n")[0].strip()
            TABLE_NAMES_LIST.append(table_name)
            columns = []
            cols = table_data.splitlines()

            # first entry of cols is table_name
            del cols[0]

            # last entry of cols is "<end_table>"
            del cols[len(cols)-1]

            for col in cols:
                columns.append(col.strip())
            COLUMNS_LIST.append(columns)
            COLUMNS_DICT[table_name] = columns

def parse_data():
    """
        Parse the data present in the tables
        1. Fill RECORDS_DICT
    """
    for table_name in TABLE_NAMES_LIST:
        file_name = table_name + ".csv"
        records = []

        with open(file_name, "r") as f:
            records_data = f.readlines()
            for record_data in records_data:
                record = record_data.split(",")
                for i, col in enumerate(record):
                    col = col.strip()
                    if col[0] == '"':
                        col = col.split('"')[1]
                    elif col[0] == "'":
                        col = col.split("'")[1]
                    record[i] = col
                records.append(record)
        RECORDS_DICT[table_name] = records

def pre_parse_data():
    """
        Pre-Parse the metadata and tables
        1. Fill TABLE_NAMES_LIST with Table Names
        2. Fill COLUMNS_LIST with List of Columns of Tables
        3. Fill COLUMNS_DICT
        4. Fill RECORDS_DICT
    """
    parse_metadata()
    parse_data()

# ------------SQL STATEMENTS----------------
def format_sql_statements(sql_statements):
    """
        FORMAT and PARSE the sql_statements using sqlparse
    """
    sql_statements = sql_statements.strip()
    # Create a list of SQL statements with delimiter as ";"
    sql_statements = sqlparse.split(SQL_STMNTS)
    # print "sqls",sql_statements

    for i, sql_statement in enumerate(sql_statements):
        sql_statements[i] = sql_statement.strip()

    for i, sql_statement in enumerate(sql_statements):
        # Format the SQL Statement by making all the Keywords uppercase
        # Keywords --> SELECT, WHERE, FROM
        if sql_statement[-1] == ';':
            sql_statements[i] = sql_statement[:-1]
            sql_statement = sql_statements[i]
        sql_statements[i] = sqlparse.format(sql_statement, keyword_case='upper')
        sql_statements[i] = sqlparse.parse(sql_statements[i])[0]
        # print sql_statements[i].tokens
    return sql_statements

def get_tokens(sql):
    """
        Return all tokens present in the given parse_sql statement
    """
    ddl_keyword = ""
    dml_keyword = ""
    none_identifiers = []
    wildcard_token = ""
    keywords = []
    aggregate_keyword = ""

    for token in sql.tokens:
        # print token.ttype, token.value
        if token.ttype == Keyword.DDL:
            ddl_keyword = token.value.upper()
        elif token.ttype == Keyword.DML:
            dml_keyword = token.value.upper()
        elif token.ttype == Wildcard:
            wildcard_token = token.value
        elif token.ttype == Keyword:
            keywords.append(token.value)
        elif token.ttype is None:
            none_identifiers.append(token.value)

    if wildcard_token != "*":
        if len(none_identifiers[0].split("(")) > 1:
            aggregate_keyword = none_identifiers[0].split("(")[0].upper()
    return ddl_keyword, dml_keyword, none_identifiers, wildcard_token, keywords, aggregate_keyword

# ------------DDL Execution-----------------
def xml_metadata(table_name, column_names):
    """
        Returns the xml metadata of table
    """
    meta_data = "<begin_table>\n" + table_name + "\n"
    for column_name in column_names:
        meta_data += column_name.strip() + "\n"
    meta_data += "<end_table>\n"
    return meta_data

def create_table(table_name, column_names):
    """
        Creates Table with name as table_name with columns as column_names
        1. Adds relevant meta_data about the table to metadata.txt
        2. Creates a file table_name.csv
    """
    meta_data = xml_metadata(table_name, column_names)
    file_name = str(table_name).strip() + ".csv"
    with open("metadata.txt", "a") as f:
        f.write(meta_data)
    os.system("touch " + file_name)

def drop_table(table_name):
    """
        Drops the table with name as table_name
        1. Removes the meta_data related to table_name
            i. Rewrite the metadata.txt omitting the metadata related to table_name
        2. Removes the file table_name.csv
    """

    file_name = str(table_name).strip() + ".csv"

    try:
        os.remove(file_name)
    except OSError as e:
        if e.errno == errno.ENOENT:
            print "Table " + table_name + " doesn't exist. Please \
                check the table name and tru again."
            print "Related metadata will be removed if exists"

    os.remove("metadata.txt")
    os.system("touch metadata.txt")
    meta_data = ""

    for table in TABLE_NAMES_LIST:
        if table != table_name:
            meta_data += xml_metadata(table, COLUMNS_DICT[table])

    with open("metadata.txt", "a") as f:
        f.write(meta_data)

def ddl_execute(ddl_keyword, none_identifiers):
    """
        Method to execute DDL type queries
        1. CREATE TABLE table_name (col1, col2, col3)
        2. DROP TABLE table_name
    """

    if ddl_keyword == "CREATE":
        table_name = none_identifiers[0]
        raw_columns_data = none_identifiers[1]
        column_names = raw_columns_data.split("(")[1].split(")")[0].split(",")
        for i, col in enumerate(column_names):
            column_names[i] = col.strip()
        create_table(table_name, column_names)

    elif ddl_keyword == "DROP":
        table_name = none_identifiers[0]
        drop_table(table_name)

# ------------DML Execution-----------------
def execute_aggregate_query(aggregate_keyword, query_tables, query_column):
    """
        Executes queries which have aggregate functions
        1. SELECT SUM(A) FROM table1, table2
        2. SELECT AVERAGE(A) FROM table1, table2
        3. SELECT MAX(A) FROM table1, table2
        4. SELECT MIN(A) FROM table1, table2
    """

    sum = 0
    number_of_records = 0
    maximum = -99999999
    minimum = 99999999
    for table in query_tables:
        number_of_records += len(RECORDS_DICT[table])
        for record in RECORDS_DICT[table]:
            for i, col in enumerate(COLUMNS_DICT[table]):
                if col == query_column:
                    sum += int(record[i])
                    maximum = max(maximum, int(record[i]))
                    minimum = min(minimum, int(record[i]))
                    break

    if aggregate_keyword == "SUM":
        print "SUM(" + query_column + ")"
        print sum
    elif aggregate_keyword == "AVG":
        print "AVG(" + query_column + ")"
        print "%.2f" % (float(sum)/number_of_records)
    elif aggregate_keyword == "COUNT":
        print "COUNT(" + query_column + ")"
        print number_of_records
    elif aggregate_keyword == "MAX":
        print "MAX(" + query_column + ")"
        print maximum
    elif aggregate_keyword == "MIN":
        print "MIN(" + query_column + ")"
        print minimum

def get_query_tables_and_columns(wildcard_token, none_identifiers):
    """
        Returns the tables and columns on which the query should happen
    """
    query_columns = []
    if wildcard_token != "*":
        query_columns = none_identifiers[0].split(",")
        for i, col in enumerate(query_columns):
            query_columns[i] = col.strip()
        del none_identifiers[0]

    query_tables = none_identifiers[0].split(",")
    for i, table in enumerate(query_tables):
        query_tables[i] = table.strip()

    modified_query_tables = []
    for i, table in enumerate(query_tables):
        table_keyerrors = 0
        if query_tables[i] not in TABLE_NAMES_LIST:
            table_keyerrors += 1
            print "table " + query_tables[i] + " doesn't exist"
        else:
            modified_query_tables.append(table)

    query_tables = modified_query_tables

    if wildcard_token != '*':
        col_ambiguous_errors = 0
        col_key_errors = 0
        for i, col in enumerate(query_columns):
            if len(col.split(".")) == 1:
                counter = 0
                for table_name in query_tables:
                    if col in COLUMNS_DICT[table_name]:
                        counter += 1
                if counter > 1:
                    col_ambiguous_errors += 1
                    print "column " + col + " is ambiguous"
                if counter == 0:
                    col_key_errors += 1
                    print "column " + col + " doesn't exist"

        if col_ambiguous_errors > 0 or col_key_errors > 0 or table_keyerrors > 0:
            return -1, -1, -1


        # Append table_name to columns
        modified_query_columns = []
        for i, col in enumerate(query_columns):
            if len(col.split(".")) == 1:
                for table_name in query_tables:
                    if col in COLUMNS_DICT[table_name]:
                        modified_query_columns.append(table_name + "." + col)
                        break
            else:
                modified_query_columns.append(col)
        query_columns = modified_query_columns

    if wildcard_token == "*":
        if table_keyerrors > 0:
            return -1, -1, -1
        for table in query_tables:
            columns = []
            for col in COLUMNS_DICT[table]:
                columns.append(table + "." + col)
            query_columns += columns

    return query_tables, query_columns, none_identifiers

def join_util(all_records, present_table_records):
    """
        Helper Function for joining tables
    """

    modified_all_records = []
    if not all_records:
        for present_table_record in present_table_records:
            new_record = []
            new_record.append(present_table_record)
            modified_all_records.append(new_record)
        return modified_all_records

    for record in all_records:
        for present_table_record in present_table_records:
            new_record = []
            for col in record:
                new_record.append(col)
            new_record.append(present_table_record)
            modified_all_records.append(new_record)
    return modified_all_records

def join_tables(table_names):
    """
        Joins all the tables present in table_names list
    """
    all_records = []

    for table_name in table_names:
        present_table_records = RECORDS_DICT[table_name]
        all_records = join_util(all_records, present_table_records)

    for i, record in enumerate(all_records):
        new_record = []
        for x in record:
            new_record += x
        all_records[i] = new_record

    modified_column_names = []
    for table_name in table_names:
        present_table_column_names = copy.deepcopy(COLUMNS_DICT[table_name])
        for i, column_name in enumerate(present_table_column_names):
            present_table_column_names[i] = table_name + "." + column_name
        modified_column_names += present_table_column_names

    return all_records, modified_column_names

def is_int(element):
    """
        returns True if element is int
        returns False otherwise
    """
    try:
        modified_element = int(element)
        return True
    except:
        return False

def split_conditional_statement(conditional_statement):
    """
        Split with delimiters as "=", ">", "<", ">=", "<="
    """
    #--------------------------------------------------------------------
    #    Splitiing 'where A = 1 and B>= 1 and table1.A <=3 or C>=4'
    #                        into
    #    ['WHERE', A', '=', '1', 'AND', 'B', '>=', '1', 'AND',
    #               'table1.A', '<=', '3', 'OR', 'C', '>=', '4']
    # -------------------------------------------------------------------
    modified_conditional_statement = []
    for c in conditional_statement:
        if c == "=":
            l = len(modified_conditional_statement)
            last_char = modified_conditional_statement[l-1]
            if last_char == ">" or last_char == "<":
                modified_conditional_statement[l-1] += "="
            else:
                modified_conditional_statement.append("=")
        elif c == ">":
            modified_conditional_statement.append(">")
        elif c == "<":
            modified_conditional_statement.append("<")
        else:
            if not modified_conditional_statement:
                modified_conditional_statement.append(c)
            else:
                l = len(modified_conditional_statement)
                last_char = modified_conditional_statement[l-1]
                if last_char == ">" or last_char == "<" or last_char == "=" \
                    or last_char == "<=" or last_char == ">=":
                    modified_conditional_statement.append(c)
                else:
                    modified_conditional_statement[l-1] += c
    conditional_statement = modified_conditional_statement
    modified_conditional_statement = []

    # print conditional_statement

    for i, ele in enumerate(conditional_statement):
        if ele == "=" or ele == ">" or ele == ">=" or ele == "<=" or ele == "<":
            modified_conditional_statement.append(ele)
        else:
            new_condition = ele.split()
            modified_conditional_statement += new_condition
    # conditional_statement = modified_conditional_statement

    return modified_conditional_statement

def get_conditions(conditional_statement, query_tables):
    """
        Return all the conditions in the form of list
        [["A", "=", "4"], "AND", ["B", "=", "5"]]
    """
    conditional_statement = split_conditional_statement(conditional_statement)

    conditions = []
    condition = []
    conditional_keyword = conditional_statement[0]
    del conditional_statement[0]
    counter = 0

    if conditional_keyword == "WHERE":
        for element in conditional_statement:
            element = str(element.strip())

            if counter == 3:
                conditions.append(condition)
                conditions.append(element)
                counter = 0
                condition = []

            elif element[0] == "(":
                for i, e in enumerate(element.split("(")):
                    if e == "":
                        conditions.append("(")
                    else:
                        condition.append(e)
                        counter += 1

            elif element[-1] == ")":
                for i, e in enumerate(element.split(")")):
                    if i == 0:
                        condition.append(e)
                        conditions.append(condition)
                        counter = 0
                        condition = []
                    elif e == "":
                        conditions.append(")")
            elif element == "AND" or element == "OR":
                conditions.append(element)
            else:
                condition.append(element)
                counter += 1
        if counter == 3:
            conditions.append(condition)

        modified_conditions = []

        col_ambiguous_errors = 0
        col_key_erros = 0
        for condition in conditions:
            if isinstance(condition, list):
                if not is_int(condition[0]):
                    if not isinstance(condition[0], list):
                        count = 0
                        for table in query_tables:
                            if condition[0] in COLUMNS_DICT[table]:
                                count += 1
                        if count > 1:
                            print "column " + condition[0] + " is ambiguous"
                            col_ambiguous_errors += 1
                if not is_int(condition[2]):
                    if not isinstance(condition[0], list):
                        count = 0
                        for table in query_tables:
                            if condition[0] in COLUMNS_DICT[table]:
                                count += 1
                        if count > 1:
                            print "column " + condition[0] + " is ambiguous"
                            col_ambiguous_errors += 1
                        elif count == 0:
                            print "column " + condition[0] + " doesn't exist"
                            col_key_erros += 1

        if col_ambiguous_errors > 0 or col_key_erros > 0:
            return -1

        for condition in conditions:
            if isinstance(condition, list):
                if len(condition[0].split(".")) == 1:
                    for table in query_tables:
                        if str(condition[0]) in COLUMNS_DICT[str(table)]:
                            new_condition = []
                            new_condition.append(table + "." + condition[0])
                            new_condition.append(condition[1])
                            if is_int(condition[2]):
                                new_condition.append(condition[2])
                            else:
                                for temp_table in query_tables:
                                    if str(condition[2]) in COLUMNS_DICT[temp_table]:
                                        new_condition.append(temp_table + "." + condition[2])
                            modified_conditions.append(new_condition)
                            modified_conditions.append("AND")
                    del modified_conditions[len(modified_conditions)-1]
                else:
                    modified_conditions.append(condition)
            else:
                modified_conditions.append(condition)
        return modified_conditions

def hide_query_columns(conditions, query_columns):
    """
        In the case of equi-join, either of the columns need to be hidden
    """
    # return []
    query_columns_to_hide = []

    if len(conditions) == 1:
        condition = conditions[0]
        # print condition
        if (str(condition[0]) in query_columns) and (str(condition[2]) in query_columns):
            query_columns_to_hide.append(condition[0])
        elif str(condition[0]) in query_columns:
            query_columns_to_hide.append(condition[2])
        elif str(condition[2]) in query_columns:
            query_columns_to_hide.append(condition[0])
        else:
            query_columns_to_hide.append(condition[0])
            query_columns_to_hide.append(condition[2])
        return query_columns_to_hide

    for j, condition in enumerate(conditions):
        if isinstance(condition, list):
            if len(condition[2].split(".")) > 1 and str(condition[1]) == "=":
                if str(condition[0]) in query_columns_to_hide:
                    if str(condition[2]) not in query_columns_to_hide:
                        query_columns_to_hide.append(condition[2])
                elif str(condition[2]) in query_columns_to_hide:
                    if str(condition[0]) not in query_columns_to_hide:
                        query_columns_to_hide.append(condition[0])
                else:
                    query_columns_to_hide.append(condition[2])

    # return query_columns_to_hide
    return []

def set_record_flags(all_records, conditions, modified_column_names_dict):
    """
        if all_records_flags[i] == True, all_records[i] should be shown
    """
    all_records_flags = []

    for i, record in enumerate(all_records):
        all_records_flags.append(False)
        evaluation_string = ""
        ops = []
        for j, condition in enumerate(conditions):
            current = False
            if isinstance(condition, list):
                index = modified_column_names_dict[str(condition[0])]
                val1 = int(record[index])
                val2 = 0
                if len(condition[2].split(".")) > 1:
                    other_index = modified_column_names_dict[str(condition[2])]
                    val2 = int(record[other_index])
                else:
                    val2 = int(condition[2])
                if str(condition[1]) == "=":
                    current = (val1 == val2)
                elif str(condition[1]) == ">":
                    current = (val1 > val2)
                elif str(condition[1]) == ">=":
                    current = (val1 >= val2)
                elif str(condition[1]) == "<":
                    current = (val1 < val2)
                elif str(condition[1]) == "<=":
                    current = (val1 <= val2)
                evaluation_string += str(current)
            else:
                ops.append(condition)
                if condition == "AND":
                    evaluation_string += " and "
                elif condition == "OR":
                    evaluation_string += " or "
                elif condition == "(":
                    evaluation_string += "("
                elif condition == ")":
                    evaluation_string += ")"
        all_records_flags[i] = eval(evaluation_string)

    return all_records_flags

def project_output(query_columns, all_records, \
    modified_column_names, is_distinct, all_records_flags, query_columns_to_hide):
    """
        Show Final Output after join when there are conditions
    """

    for hidden_col in query_columns_to_hide:
        for i, col in enumerate(query_columns):
            if hidden_col == col:
                del query_columns[i]
                break

    query_columns_indices = []
    for i, col in enumerate(modified_column_names):
        if col in query_columns:
            query_columns_indices.append(i)

    meta_data_output = ""
    for i, query_columns_index in enumerate(query_columns_indices):
        if i == len(query_columns_indices) - 1:
            meta_data_output += str(modified_column_names[query_columns_index])
            print meta_data_output
        else:
            meta_data_output += str(modified_column_names[query_columns_index]) + ","

    output_records = []
    for j, record in enumerate(all_records):
        if all_records_flags[j] is True:
            output = ""
            for i, query_columns_index in enumerate(query_columns_indices):
                if i == len(query_columns_indices) - 1:
                    output += str(record[query_columns_index])
                else:
                    output += str(record[query_columns_index]) + ","
            if is_distinct:
                if output not in output_records:
                    output_records.append(output)
            else:
                output_records.append(output)

    for record in output_records:
        print record

def dml_execute(none_identifiers, wildcard_token, aggregate_keyword, keywords):
    """
        SELECT A, B from table1, table2
        None_Identifiers = [u'A, B', u'table1, table2']

        SELECT * from table1, table2
        None_Identifiers = [u'table1, table2']
    """
    if aggregate_keyword:
        try:
            query_tables = none_identifiers[1].split(",")
        except Exception as e:
            print "Incorrect Query", e
            return

        for i, table in enumerate(query_tables):
            query_tables[i] = table.strip()

        modified_query_tables = []
        table_keyerrors = 0
        for i, table in enumerate(query_tables):
            table_keyerrors = 0
            if query_tables[i] not in TABLE_NAMES_LIST:
                table_keyerrors += 1
                print "table " + query_tables[i] + " doesn't exist"
            else:
                modified_query_tables.append(table)

        query_tables = modified_query_tables

        try:
            query_column = none_identifiers[0].split("(")[1].split(")")[0]
        except Exception as e:
            print "Incorrect Query", e
            return

        counter = 0
        for table in query_tables:
            try:
                if query_column in COLUMNS_DICT[table]:
                    counter += 1
            except Exception as e:
                print "Incorrect Query", e
                return

        if counter > 1:
            print "column " + query_column + " is ambiguous"

        if counter > 1 or table_keyerrors > 0:
            return
        try:
            execute_aggregate_query(aggregate_keyword, query_tables, query_column)
        except Exception as e:
            print "Incorrect Query", e
        return 0

    try:
        [query_tables, query_columns, none_identifiers] = \
            get_query_tables_and_columns(wildcard_token, none_identifiers)
    except Exception as e:
        print "Incorrect Query", e
        return

    if query_tables == -1 and query_columns == -1 and none_identifiers == -1:
        return

    try:
        [all_records, modified_column_names] = join_tables(query_tables)
    except Exception as e:
        print "Incorrect Query", e
        return

    if len(none_identifiers) == 1:
        # No WHERE conditions
        all_records_flags = [True] * len(all_records)
        # No Equi-Join
        query_columns_to_hide = []
        try:
            project_output(query_columns, all_records, \
                modified_column_names, "DISTINCT" in keywords, \
                all_records_flags, query_columns_to_hide)
        except Exception as e:
            print "Incorrect Query", e
            return
        return 0

    # project with conditions

    modified_column_names_dict = {}
    for i, column_name in enumerate(modified_column_names):
        modified_column_names_dict[column_name] = i

    conditional_statement = None_Identifiers[1]
    try:
        conditions = get_conditions(conditional_statement, query_tables)
        if conditions == -1:
            return
    except Exception as e:
        print "Incorrect Query", e
        return

    try:
        # if wildcard_token == "*":
        query_columns_to_hide = hide_query_columns(conditions, query_columns)
        # else:
            # query_columns_to_hide = []
    except Exception as e:
        print "Incorrect Query", e
        return

    try:
        all_records_flags = set_record_flags(all_records, conditions, modified_column_names_dict)
    except Exception as e:
        print "Incorrect Query", e
        return

    try:
        project_output(query_columns, all_records, modified_column_names, \
            "DISTINCT" in keywords, all_records_flags, query_columns_to_hide)
    except Exception as e:
        print "Incorrect Query", e
        return

# ------------------------------------------

if __name__ == '__main__':
    pre_parse_data()
    if len(sys.argv) == 1:
        print "Add any SQL statement as an argument"
    elif len(sys.argv) == 2:
        SQL_STMNTS = sys.argv[1]
        PARSED_SQL_STMNTS = format_sql_statements(SQL_STMNTS)

        for l, parsed_sql in enumerate(PARSED_SQL_STMNTS):
            if l > 0:
                print ""
            if parsed_sql[-1] == ';':
                parsed_sql = parsed_sql[-1:]

            DDL_Keyword, DML_Keyword, None_Identifiers, \
                Wildcard_Token, Keywords, Aggregate_Keyword = get_tokens(parsed_sql)

            if DDL_Keyword:
                ddl_execute(DDL_Keyword, None_Identifiers)

            elif DML_Keyword == "SELECT":
                dml_execute(None_Identifiers, Wildcard_Token, Aggregate_Keyword, Keywords)
