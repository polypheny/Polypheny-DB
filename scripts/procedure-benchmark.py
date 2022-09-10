import polypheny
from stopwatch import Stopwatch
import statistics as stat
import csv

def calculateMetrics(list, name):
    deviation = stat.pstdev(list)
    print("std deviation: " + str(deviation))
    variance = stat.pvariance(list)
    print("Variance: " + str(variance))
    mean = stat.mean(list)
    print("Mean: " + str(mean))
    row = [name, mean, variance, deviation]
    writer.writerow(row)

def createStopwatch():
    return Stopwatch(3)

def insert():
    return "INSERT INTO dummy VALUES (407 , 'de')"

def update():
    return "UPDATE dummy set text='myValue' where id=407)"

def delete():
    return "DELETE FROM dummy where id=407"

def runProcedure(cursor):
    stopwatch = createStopwatch()
    stopwatch.start()
    cursor.execute("EXEC PROCEDURE \"APP.public.spNoParam\"")
    connection.commit()
    stopwatch.stop()
    execTime = stopwatch.duration
    return execTime

def runQuery(cursor):
    stopwatch = createStopwatch()
    stopwatch.start()
    cursor.execute("INSERT INTO dummy VALUES (407 , 'de')")
    connection.commit()
    stopwatch.stop()
    execTime = stopwatch.duration
    return execTime

def removeEntities(cursor):
    print("\nRemoving entities...")
    cursor.execute("DROP PROCEDURE spNoParam")
    cursor.execute("DROP TABLE dummy")

def connect():
    print("Connecting to Polypheny...")
    connection = polypheny.connect('localhost', 20591, user='pa', password='')
    print("Connection established")
    # Get a cursor
    return connection

def disconnect(connection):
    connection.close()

def benchmarkInsert(cursor, writer):
    procedureTime = []
    queryTime = []
    ## measurements
    print("\nRunning measurements...")
    for i in range(n):
        time = runQuery(cursor)
        queryTime.append(time)
        time = runProcedure(cursor)
        procedureTime.append(time)
    writer.writerow(["insertProcedure"] + procedureTime)
    writer.writerow(["insertQuery"] + queryTime)

## Setup
n = 1000


# Connect to Polypheny
connection = connect()
cursor = connection.cursor()

# Create a new table
print("\nSetting up entities...")
print("Deleting previous instances...")
#cursor.execute("DROP TABLE IF EXISTS dummy")
#cursor.execute("DROP PROCEDURE \"spNoParam\"")
print("Creating new instances...")
cursor.execute("CREATE TABLE dummy (id INT NOT NULL, text VARCHAR(20), PRIMARY KEY(id))")
cursor.execute("CREATE PROCEDURE spNoParam $ insert into dummy VALUES(101, 'Harold') $")

# Print statistics summary
with open('polypheny-benchmarks.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    procedureTime = []
    queryTime = []
    ## measurements
    print("\nRunning measurements...")
    for i in range(n):
        time = runQuery(cursor)
        queryTime.append(time)
        time = runProcedure(cursor)
        procedureTime.append(time)
    writer.writerow(["insertProcedure"] + procedureTime)
    writer.writerow(["insertQuery"] + queryTime)

# remove entities
removeEntities(cursor)

# Close the connection
disconnect(connection)
file.close()
print("done...")
