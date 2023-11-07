/*
 * Copyright 2019-2023 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.backup.datainserter;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.backup.BupInformationObject;
import org.polypheny.db.backup.BupSuperEntity;
import org.polypheny.db.catalog.entity.logical.*;

import java.util.List;
import java.util.Map;

@Slf4j
public class InsertSchema {
    private BupInformationObject bupInformationObject;

    public InsertSchema() {
    }

    /**
     * Manages the insertion process of the schema
     * @param bupInformationObject contains all the metadata of the schema to be inserted
     */
    public void start( BupInformationObject bupInformationObject ) {
        log.debug( "insert schemas" );
        this.bupInformationObject = bupInformationObject;

        //TODO(FF): create something to test that only available data is tried to be inserted

        /*
        insertion order (schema):
        1. Relational
            1.1 Namespaces (Create and Alter (owner))
            1.2 Tables (Create)
                1.2.1 Columns
                1.2.2 Primary Keys
            1.3 Tables (Alter)
                1.3.1 Constraints
                1.3.2 Foreign Keys
                1.3.3 Owner (alter)
        2. Graph
            2.1 Namespaces (Create and Alter (owner)) & case sensitivity?
        3. Document
            3.1 Namespaces (Create and Alter (owner))
            3.2 Collections

        After Insertion of Entries (provisional, can also be before... but then need to update materialized view, and insertion takes longer (bcs. idx):
        4. Indexes (create)
        5. Views (create)
        6. Materialized Views (create)
         */

        //1.1 Relational Namespaces
    }

    private void insertRelationalNamespaces() {
        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> relNamespaces = bupInformationObject.getBupRelNamespaces();
        //String query = "INSERT INTO " + "relational_namespace" + " (id, name, owner, case_sensitive) VALUES (?, ?, ?, ?)";
        String query = new String();

        for ( Map.Entry<Long, BupSuperEntity<LogicalNamespace>> ns : relNamespaces.entrySet() ) {
            query = "CREATE RELATIONAL NAMESPACE " + ns.getValue().getEntityObject().name + ";";
        }

    }

    private void createNamespace(ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> namespaces) {
        String query = new String();
        //TODO(FF): check if namespace already exists, give rename or overwrite option (here, or earlier?), if new name, write it to bupInformationObject

        for ( Map.Entry<Long, BupSuperEntity<LogicalNamespace>> ns : namespaces.entrySet() ) {
            query = "CREATE " + ns.getValue().getEntityObject().namespaceType.toString() + " NAMESPACE " + ns.getValue().getEntityObject().name + ";";
            //TODO(FF): execute query in polypheny, alter owner, set case sensitivity (how?)
        }
    }

    private void createTable ( ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tables ) {
        String query = new String();
        String columnDefinitions = new String();
        String pkConstraint = new String();
        ImmutableMap<Long, List<LogicalColumn>> columns = bupInformationObject.getColumns();
        ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeys = bupInformationObject.getPrimaryKeysPerTable();

        // key: namespace id, value: list of tables for the namespace
        for ( Map.Entry<Long, List<BupSuperEntity<LogicalTable>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            String namespaceName = bupInformationObject.getBupRelNamespaces().get( nsID ).getNameForQuery();

            List<BupSuperEntity<LogicalTable>> tablesList = tablesPerNs.getValue();

            // go through each table in the list (of tables for one namespace)
            for ( BupSuperEntity<LogicalTable> table : tablesList ) {
                LogicalTable logicalTable = table.getEntityObject();
                Long tableID = logicalTable.getId();
                List<LogicalColumn> colsPerTable = columns.get( tableID );
                List<LogicalPrimaryKey> pksPerTable = primaryKeys.get( tableID );

                // create the column defintion statement for the table
                for ( LogicalColumn col : colsPerTable ) {
                    String colName = col.getName();
                    String colDataType = col.getType().toString();
                    Boolean colNullable = col.isNullable(); //TODO(FF): is this corresponding to [NULL | NOT NULL]?
                    //TODO(FF): test out how to write "DEFAULT expression" bzw. how to get info from metadata?
                    //TODO(FF): how to get case sensitivtiy from metadata?

                    columnDefinitions = columnDefinitions + colName + " " + colDataType + " " + nullableBoolToString( colNullable ) + ", ";
                }
                if ( columnDefinitions.length() > 0 ) {
                    columnDefinitions = columnDefinitions.substring( 0, columnDefinitions.length() - 2 ); // remove last ", "
                }


                // create the primary key constraint statement for the table
                List<String> colNamesForPK = pksPerTable.get( 0 ).getColumnNames();
                String listOfCols = new String();
                for (String colName : colNamesForPK) {
                    listOfCols = listOfCols + colName + ", ";
                }
                if (listOfCols.length() > 0) {
                    listOfCols = listOfCols.substring( 0, listOfCols.length() - 2 ); // remove last ", "
                    pkConstraint = "PRIMARY KEY (" + listOfCols + ")";
                }

                //query to create one table (from the list of tables, from the list of namespaces)
                query = "CREATE TABLE " + namespaceName + "." + table.getNameForQuery() + " ( " + columnDefinitions + ", " + pkConstraint + " );"; //TODO(FF): ON STORE storename PARTITION BY partionionInfo
            }
        }
    }

    public String nullableBoolToString (boolean nullable) {
        if (nullable) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }
}
