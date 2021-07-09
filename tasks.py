from sqlalchemy import create_engine
import psycopg2
from config import connection as cfg

# Creating connection string to the respective database
pagila_conn = "postgresql://" + cfg["username"] + ":" + cfg["password"] + "@" + cfg["host"] + ":" + cfg[
    "port"] + "/" + cfg["source"] + ""
rental_conn = "postgresql://" + cfg["username"] + ":" + cfg["password"] + "@" + cfg["host"] + ":" + cfg[
    "port"] + "/" + cfg["destination"] + ""

# Creating engine using connection string
pagiladb = create_engine(pagila_conn)
rentaldb = create_engine(rental_conn)


# Function to aggregate the data from pagila database
def aggregate_data():
    print(
        "---------------------------------------\nTask2: Aggregating the data \n---------------------------------------")

    try:
        result_set = pagiladb.execute("SELECT \
          DATE(date_trunc('week', rental_date)) AS WeekBeginning, \
          COUNT(*) AS OutstandingRentals, \
          SUM (CASE WHEN return_date >= date_trunc ('week', rental_date) \
          AND return_date <= date_trunc ('week', rental_date) + INTERVAL '7 day' \
          THEN 1 ELSE 0 END) AS ReturnedRentals \
        FROM rental \
        GROUP BY date_trunc('week', rental_date) \
        ORDER BY WeekBeginning")

        li = [r for r in result_set]
        print("Data Aggregation Completed!")
        incr_changes(li)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if pagiladb is not None:
            pagiladb.dispose()


# Function to check connection to the database
def connection():
    print(
        "---------------------------------------\nTask1: Checking Database "
        "Connectivity\n---------------------------------------")
    try:
        version = pagiladb.execute("SELECT version()")
        rental_version = rentaldb.execute("SELECT version()")

        for r in version:
            print("Connection Successful to Database: pagila | Version: " + str(r[0]))
        for r in rental_version:
            print("Connection Successful to Database: rentaldb | Version: " + str(r[0]))

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if pagiladb is not None and rentaldb is not None:
            pagiladb.dispose()
            rentaldb.dispose()


def incr_changes(li):
    print(
        "--------------------------------------------\nTask3: Inserting/Updating Incremental "
        "Changes\n--------------------------------------------")
    try:
        # Getting the latest data from destination db
        data = rentaldb.execute("SELECT WeekBeginning, OutstandingRentals, ReturnedRentals FROM rental")
        rental_li = [d for d in data]

        # Initial load
        if data.rowcount == 0:
            for r in li:
                rentaldb.execute("INSERT INTO rental (WeekBeginning, OutstandingRentals, ReturnedRentals) "
                                 "VALUES ('" + str(r[0].date()) + "', " + str(r[1]) + ", " + str(r[2]) + ")")

        # Inserting new records
        elif data.rowcount < len(li):
            diff = list(filter(lambda x: x not in rental_li, li))
            for r in diff:
                rentaldb.execute("INSERT INTO rental (WeekBeginning, OutstandingRentals, ReturnedRentals) "
                                 "VALUES ('" + str(r[0]) + "', " + str(r[1]) + ", " + str(r[2]) + ")")

        # Updating existing records if any
        elif data.rowcount == len(li):
            diff = list(filter(lambda x: x not in rental_li, li))
            if len(diff) == 0:
                pass
            for r in diff:
                rentaldb.execute(
                    "UPDATE rental SET OutstandingRentals=" + str(r[1]) + ", ReturnedRentals=" + str(r[2]) +
                    " WHERE WeekBeginning='" + str(r[0]) + "'")

        print("Data is Up-to-date!")

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if rentaldb is not None:
            rentaldb.dispose()

