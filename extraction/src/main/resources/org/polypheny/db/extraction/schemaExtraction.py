#!/usr/bin/env python
import sys
import polypheny
import json

# Debug information
print("\nInput file:", sys.argv[1])
print("\nOutput file:", sys.argv[2])

# Connect to Polypheny
# TODO: Generalize connection details?
connection = polypheny.connect('localhost', 20591, user='pa', password='')

# Get a Polypheny cursor
cursor = connection.cursor()

with open(sys.argv[1]) as f:
    # returns JSON object as a dictionary
    data = json.load(f)

    datamodel = data["datamodel"]
    print("datamodel:", datamodel)
    if datamodel != "RELATIONAL":
        raise ValueError('Datamodel is not relational. Not implemented!')
        sys.exit('Fatal schema extractor error')

    for i in data["tables"]:
        print("table name:", i["tableName"] + ".", "number of columns:", len(i["columnNames"]))

        # Execute a Polypheny query
        cursor.execute("SELECT * FROM " + i["tableName"])
        result = cursor.fetchone()
        print("Result Set: ", result)

# Close the Polypheny connection
connection.close()