#!/usr/bin/python2.7

import psycopg2
import os
import sys
import threading

##################### This needs to changed based on what kind of table we want to sort. ##################

FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'rating'
SORT_COLUMN_NAME_SECOND_TABLE = 'Movieid1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'MovieId'
JOIN_COLUMN_NAME_SECOND_TABLE = 'MovieId1'
##########################################################################################################


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def Total_Range(InputTable, SortingColumnName, openconnection):
    cur = openconnection.cursor()

    # Gets maximum and minimum value of SortingColumnName
    cur.execute("SELECT MIN(" + SortingColumnName + ") FROM " + InputTable + "")
    Minimum_Value = cur.fetchone()
    range_minimum_value = (float)(Minimum_Value[0])

    cur.execute("SELECT MAX(" + SortingColumnName + ") FROM " + InputTable + "")
    Maximum_Value = cur.fetchone()
    range_maximum_value = (float)(Maximum_Value[0])

    interval = (range_maximum_value - range_minimum_value) / 5
    return interval, range_minimum_value


# Inserts in sorted values to the table
def range_insert_sorted_value(InputTable, SortingColumnName, table_index, minm_value, maxm_value, openconnection):
    cur = openconnection.cursor()

    table_name = "range_part" + str(table_index)

    # Check for minimum value of column
    if table_index == 0:
        query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">=" + str(
            minm_value) + " AND " + SortingColumnName + " <= " + str(maxm_value) + " ORDER BY " + SortingColumnName + " ASC"
    else:
        query = "INSERT INTO " + table_name + " SELECT * FROM " + InputTable + "  WHERE " + SortingColumnName + ">" + str(
            minm_value) + " AND " + SortingColumnName + " <= " + str(maxm_value) + " ORDER BY " + SortingColumnName + " ASC"

    cur.execute(query)
    cur.close()
    return

#parallel sort function
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    try:
        # Getting cursor of openconnection
        cur = openconnection.cursor()

        # Gets the range and mininum value of range from function Range
        interval_sort, range_Minimum = Total_Range(InputTable, SortingColumnName, openconnection)

        # gets the schema of InputTable
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable + "'")
        schema = cur.fetchall()

        # tables to store and sort range partitions
        for i in range(5):

            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + tableName + "")
            cur.execute("CREATE TABLE " + tableName + " (" + schema[0][0] + " " + schema[0][1] + ")")

            for d in range(1, len(schema)):
                cur.execute("ALTER TABLE " + tableName + " ADD COLUMN " + schema[d][0] + " " + schema[d][1] + ";")

        # creating of threads - count 5
        thread = [0, 0, 0, 0, 0]
        for i in range(5):

            if i == 0:
                lower_Bound = range_Minimum
                upper_Bound = range_Minimum + interval_sort
            else:
                lower_Bound = upper_Bound
                upper_Bound = upper_Bound + interval_sort

            thread[i] = threading.Thread(target=range_insert_sorted_value, args=(InputTable, SortingColumnName, i, lower_Bound, upper_Bound, openconnection))

            thread[i].start()

        for p in range(0, 5):
            thread[i].join()

        # filling up the OutputTable

        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + schema[0][0] + " " + schema[0][1] + ")")

        for i in range(1, len(schema)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema[i][0] + " " + schema[i][1] + ";")

        for i in range(5):
            query = "INSERT INTO " + OutputTable + " SELECT * FROM " + "range_part" + str(i) + ""
            cur.execute(query)

    except Exception as message:
        print "Exception :", message

        # table Clean up
    finally:

        for i in range(5):
            tableName = "range_part" + str(i)
            cur.execute("DROP TABLE IF EXISTS " + tableName + "")
    cur.close()
    openconnection.commit()



def Minimum_Maximum_values(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection):

    cur = openconnection.cursor()

    # Gets maximum and minimum value of the columns
    cur.execute("SELECT MIN(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    min_1_orginal = cur.fetchone()
    Min_1_float = (float)(min_1_orginal[0])

    cur.execute("SELECT MIN(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    min_2_orginal = cur.fetchone()
    Min_2_float = (float)(min_2_orginal[0])

    cur.execute("SELECT MAX(" + Table1JoinColumn + ") FROM " + InputTable1 + "")
    max_1_orginal = cur.fetchone()
    Max_1_float = (float)(max_1_orginal[0])

    cur.execute("SELECT MAX(" + Table2JoinColumn + ") FROM " + InputTable2 + "")
    max_2_orginal = cur.fetchone()
    Max_2_float = (float)(max_2_orginal[0])

    if Max_1_float > Max_2_float:
        range_Maximum = Max_1_float
    else:
        range_Maximum = Max_2_float

    if Min_1_float > Min_2_float:
        range_Minimum = Min_2_float
    else:
        range_Minimum = Min_1_float

    interval = (range_Maximum - range_Minimum) / 5


    return interval, range_Minimum


def Output_Range_Table(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2, interval,
                     range_minimum_value, openconnection):
    cur = openconnection.cursor();
    for i in range(5):

        range_new_table1_name = "table1_range" + str(i)
        range_new_table2_name = "table2_range" + str(i)

        if i == 0:
            lower_Bound = range_minimum_value
            upper_Bound = range_minimum_value + interval
        else:
            lower_Bound = upper_Bound
            upper_Bound = upper_Bound + interval

        cur.execute("DROP TABLE IF EXISTS " + range_new_table1_name + ";")
        cur.execute("DROP TABLE IF EXISTS " + range_new_table2_name + ";")

        if i == 0:
            cur.execute(
                "CREATE TABLE " + range_new_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " >= " + str(
                    lower_Bound) + ") AND (" + Table1JoinColumn + " <= " + str(upper_Bound) + ");")
            cur.execute(
                "CREATE TABLE " + range_new_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " >= " + str(
                    lower_Bound) + ") AND (" + Table2JoinColumn + " <= " + str(upper_Bound) + ");")

        else:
            cur.execute("CREATE TABLE " + range_new_table1_name + " AS SELECT * FROM " + InputTable1 + " WHERE (" + Table1JoinColumn + " > " + str(
                    lower_Bound) + ") AND (" + Table1JoinColumn + " <= " + str(upper_Bound) + ");")
            cur.execute("CREATE TABLE " + range_new_table2_name + " AS SELECT * FROM " + InputTable2 + " WHERE (" + Table2JoinColumn + " > " + str(
                    lower_Bound) + ") AND (" + Table2JoinColumn + " <= " + str(upper_Bound) + ");")

        # Output table(range)
        output_range_table = "output_table" + str(i)

        cur.execute("DROP TABLE IF EXISTS " + output_range_table + "")
        cur.execute("CREATE TABLE " + output_range_table + " (" + schema1[0][0] + " " + schema2[0][1] + ")")

        for j in range(1, len(schema1)):
            cur.execute("ALTER TABLE " + output_range_table + " ADD COLUMN " + schema1[j][0] + " " + schema1[j][1] + ";")

        for j in range(len(schema2)):
            cur.execute("ALTER TABLE " + output_range_table + " ADD COLUMN " + schema2[j][0] + "1" + " " + schema2[j][1] + ";")

def range_insert_join_tables(Table1JoinColumn, Table2JoinColumn, openconnection, TempTableId):

    cur = openconnection.cursor()

    query = "INSERT INTO output_table" + str(TempTableId) + " SELECT * FROM table1_range" + str(TempTableId) + " INNER JOIN table2_range" + str(TempTableId) + " ON table1_range" + str(
            TempTableId) + "." + Table1JoinColumn + "=" + "table2_range" + str(TempTableId) + "." + Table2JoinColumn + ";"

    cur.execute(query)
    cur.close()
    return

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    try:

        cur = openconnection.cursor()


        interval, range_minimum_value = Minimum_Maximum_values(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, openconnection)

        # obtaining input table schemas
        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable1 + "'")
        schema1 = cur.fetchall()

        cur.execute("SELECT COLUMN_NAME,DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='" + InputTable2 + "'")
        schema2 = cur.fetchall()

        # output table
        cur.execute("DROP TABLE IF EXISTS " + OutputTable + "")
        cur.execute("CREATE TABLE " + OutputTable + " (" + schema1[0][0] + " " + schema2[0][1] + ")")

        for i in range(1, len(schema1)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema1[i][0] + " " + schema1[i][1] + ";")

        for i in range(len(schema2)):
            cur.execute("ALTER TABLE " + OutputTable + " ADD COLUMN " + schema2[i][0] + "1" + " " + schema2[i][1] + ";")

        # Calls the OuputRangeTable

        Output_Range_Table(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, schema1, schema2, interval,
                         range_minimum_value, openconnection)

        # creating of threads - count 5
        thread = [0, 0, 0, 0, 0]

        for i in range(5):
            thread[i] = threading.Thread(target=range_insert_join_tables,args=(Table1JoinColumn, Table2JoinColumn, openconnection, i))

            thread[i].start()

        for a in range(0, 5):
            thread[i].join()

        # Inserts into output table
        for i in range(5):
            cur.execute("INSERT INTO " + OutputTable + " SELECT * FROM output_table" + str(i))

    except Exception as detail:
        print "Exception in ParallelJoin is ==>>", detail

        # Clean up
    finally:
        for i in range(5):
            cur.execute("DROP TABLE IF EXISTS table1_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS table2_range" + str(i))
            cur.execute("DROP TABLE IF EXISTS output_table" + str(i))

        cur.close()
        openconnection.commit()


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment3
        print "Creating Database named as ddsassignment3"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();
        con.commit()

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);



        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
