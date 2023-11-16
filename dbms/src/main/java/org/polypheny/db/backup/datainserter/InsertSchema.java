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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.backup.BupInformationObject;
import org.polypheny.db.backup.BupSuperEntity;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.logistic.Collation;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

import java.util.List;
import java.util.Map;

@Slf4j
public class InsertSchema {

    private BupInformationObject bupInformationObject;
    private final TransactionManager transactionManager;


    public InsertSchema( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    /**
     * Manages the insertion process of the schema
     *
     * @param bupInformationObject contains all the metadata of the schema to be inserted
     */
    public void start( BupInformationObject bupInformationObject ) {
        log.debug( "insert schemas" );
        this.bupInformationObject = bupInformationObject;
        ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tables;

        /*
        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> temp = bupInformationObject.transformNamespacesToBupSuperEntityMap( bupInformationObject.getRelNamespaces() );
        bupInformationObject.setBupRelNamespaces( temp );
        insertCreateNamespace( bupInformationObject.getBupRelNamespaces() );
        insertCreateNamespace( bupInformationObject.getBupDocNamespaces() );
        insertCreateNamespace( bupInformationObject.getBupGraphNamespaces() );

         */

        //TODO(FF): figure out how to create collections
        //insertCreateCollection(); //error: Caused by: org.polypheny.db.mql.parser.TokenMgrError: Lexical error at line 1, column 21.  Encountered: '39' (39),
        log.info( "created collection" );
        // got until prepared statement execution, then got the following error: -> do i have to create mqlQueryParameters, not queryParameters??
        // class org.polypheny.db.languages.QueryParameters cannot be cast to class org.polypheny.db.languages.mql.MqlQueryParameters (org.polypheny.db.languages.QueryParameters is in unnamed module of loader 'app'; org.polypheny.db.languages.mql.MqlQueryParameters is in unnamed module of loader org.pf4j.PluginClassLoader @60743cdb)

        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> namespaces = bupInformationObject.transformNamespacesToBupSuperEntityMap( bupInformationObject.getNamespaces() );
        bupInformationObject.setBupNamespaces( namespaces );
        insertCreateNamespace( bupInformationObject.getBupNamespaces() );
        /*
        String query = String.format("CREATE GRAPH NAMESPACE testGraph");
        executeStatementInPolypheny( query, "sql", NamespaceType.GRAPH);
        String queery = "CREATE PLACEMENT OF testGraph ON STORE hsqldb"; //error: Caused by: org.polypheny.db.languages.sql.parser.impl.ParseException: Encountered "" at line 1, column 1.
        executeStatementInPolypheny( queery, "sql", NamespaceType.GRAPH);
         */

        /*
        //List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();
        Map<Long, List<BupSuperEntity<LogicalTable>>> tempMap = new HashMap<>();

        for ( Map.Entry<Long, List<LogicalTable>> a : bupInformationObject.getTables().entrySet()) {
            List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();
            //TODO(FF): doesn't work with return value :(
            bupEntityList = bupInformationObject.transformLogigalEntityToSuperEntity( a.getValue().stream().map( e -> (LogicalEntity) e ).collect( Collectors.toList()) );
            tempMap.put( a.getKey(), bupEntityList.stream().map( e -> (BupSuperEntity<LogicalTable>) e ).collect( Collectors.toList()));
        }

         */
        // create table
        //bupInformationObject.transformLogicalEntitiesToBupSuperEntity( bupInformationObject.getTables() );
        tables = bupInformationObject.tempTableTransformation( bupInformationObject.getTables(), true );
        bupInformationObject.setBupTables( tables );
        insertCreateTable( bupInformationObject.getBupTables() );

        // alter table - add unique constraint
        insertAlterTableUQ( bupInformationObject.getBupTables(), bupInformationObject.getConstraints() );
        insertAlterTableFK( bupInformationObject.getBupTables(), bupInformationObject.getForeignKeysPerTable() );

        //TODO(FF): create something to test that only available data is tried to be inserted
        //TODO(FF): don't insert tables from source (check for entityType SOURCE (not default is ENTITY))

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
            LogicalNamespace namespace = catalog.getSnapshot().getNamespace( graphName ).orElseThrow();
            useful??
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

            //query = "CREATE RELATIONAL NAMESPACE " + ns.getValue().getEntityObject().name + ";";
            query = String.format( "CREATE RELATIONAL NAMESPACE %s", ns.getValue().getEntityObject().name );
        }

    }


    private void insertCreateNamespace( ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> namespaces ) {
        String query = new String();
        //TODO(FF): check if namespace already exists, give rename or overwrite option (here, or earlier?), if new name, write it to bupInformationObject

        //TODO(FF): check if namespaces is empty, throw error if it is
        for ( Map.Entry<Long, BupSuperEntity<LogicalNamespace>> ns : namespaces.entrySet() ) {
            //query = "CREATE " + ns.getValue().getEntityObject().namespaceType.toString() + " NAMESPACE " + ns.getValue().getEntityObject().name + ";";
            query = String.format( "CREATE %s NAMESPACE %s11", ns.getValue().getEntityObject().namespaceType.toString(), ns.getValue().getEntityObject().name );

            //TODO(FF): execute query in polypheny, alter owner, set case sensitivity (how?)
            if ( !ns.getValue().getEntityObject().name.equals( "public" ) ) {
                executeStatementInPolypheny( query, "sql", NamespaceType.RELATIONAL );
            }
        }


    }


    private void insertCreateTable( ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tables ) {
        String query = new String();

        // key: namespace id, value: list of tables for the namespace
        for ( Map.Entry<Long, List<BupSuperEntity<LogicalTable>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            //String namespaceName = bupInformationObject.getBupRelNamespaces().get( nsID ).getNameForQuery();
            //only get rel namespaces from all bup namespaces
            String namespaceName = bupInformationObject.getBupNamespaces().get( nsID ).getNameForQuery();

            List<BupSuperEntity<LogicalTable>> tablesList = tablesPerNs.getValue();

            // go through each table in the list (of tables for one namespace)
            for ( BupSuperEntity<LogicalTable> table : tablesList ) {

                // only create tables that don't (exist by default in polypheny)
                if ( !(table.getEntityObject().entityType.equals( EntityType.SOURCE )) ) {
                    query = createTableQuery( table, namespaceName );
                    executeStatementInPolypheny( query, "sql", NamespaceType.RELATIONAL );
                }
            }
        }
    }


    private void insertAlterTableUQ( ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tables, ImmutableMap<Long, List<LogicalConstraint>> constraints ) {
        String query = new String();

        for ( Map.Entry<Long, List<BupSuperEntity<LogicalTable>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            String namespaceName = bupInformationObject.getBupNamespaces().get( nsID ).getNameForQuery();

            List<BupSuperEntity<LogicalTable>> tablesList = tablesPerNs.getValue();

            // go through each constraint in the list (of tables for one namespace)
            for ( BupSuperEntity<LogicalTable> table : tablesList ) {
                //TODO(FF - cosmetic): exclude source tables (for speed)

                // compare the table id with the constraint keys, and if they are the same, create the constraint
                if ( constraints.containsKey( table.getEntityObject().getId() ) ) {
                    List<LogicalConstraint> constraintsList = constraints.get( table.getEntityObject().getId() );
                    List<LogicalColumn> logicalColumns = bupInformationObject.getColumns().get( table.getEntityObject().getId() );

                    // go through all constraints per table
                    for ( LogicalConstraint constraint : constraintsList ) {
                        String tableName = table.getNameForQuery();
                        String constraintName = constraint.name;
                        String listOfCols = new String();

                        List<Long> colIDs = constraint.getKey().columnIds;

                        // get all column-names used in the constraint from the columns
                        listOfCols = getListOfCol( colIDs, logicalColumns );

                        query = String.format( "ALTER TABLE %s11.%s11 ADD CONSTRAINT %s UNIQUE (%s)", namespaceName, tableName, constraintName, listOfCols );
                        log.info( query );
                        executeStatementInPolypheny( query, "sql", NamespaceType.RELATIONAL );
                    }
                }
            }
        }
    }


    private void insertAlterTableFK( ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> bupTables, ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable ) {
        String query = new String();

        // go through foreign key constraints and collect the necessary data
        for ( Map.Entry<Long, List<LogicalForeignKey>> fkListPerTable : foreignKeysPerTable.entrySet() ) {
            for ( LogicalForeignKey foreignKey : fkListPerTable.getValue() ) {
                String namespaceName = bupInformationObject.getBupNamespaces().get( foreignKey.namespaceId ).getNameForQuery();
                String tableName = bupInformationObject.getBupTables().get( foreignKey.namespaceId ).stream().filter( e -> e.getEntityObject().getId() == foreignKey.tableId ).findFirst().get().getNameForQuery();
                String constraintName = foreignKey.name;
                String listOfCols = getListOfCol( foreignKey.columnIds, bupInformationObject.getColumns().get( foreignKey.tableId ) );
                String referencedNamespaceName = bupInformationObject.getBupNamespaces().get( foreignKey.referencedKeySchemaId ).getNameForQuery();
                String referencedTableName = bupInformationObject.getBupTables().get( foreignKey.referencedKeySchemaId ).stream().filter( e -> e.getEntityObject().getId() == foreignKey.referencedKeyTableId ).findFirst().get().getNameForQuery();
                String referencedListOfCols = getListOfCol( foreignKey.referencedKeyColumnIds, bupInformationObject.getColumns().get( foreignKey.referencedKeyTableId ) );
                String updateAction = foreignKey.updateRule.foreignKeyOptionToString();
                String deleteAction = foreignKey.deleteRule.foreignKeyOptionToString();
                //TODO(FF): enforcementTime (on commit) - how to set?? how to change?? possible to change?

                query = String.format( "ALTER TABLE %s11.%s11 ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s11.%s11 (%s) ON UPDATE %s ON DELETE %s", namespaceName, tableName, constraintName, listOfCols, referencedNamespaceName, referencedTableName, referencedListOfCols, updateAction, deleteAction );
                log.info( query );
                executeStatementInPolypheny( query, "sql", NamespaceType.RELATIONAL );
            }
        }
    }


    private void insertCreateCollection() {
        //db.createCollection('users')
        //createCollection( long namespaceId, String name, boolean ifNotExists, List<DataStore<?>> stores, PlacementType placementType, Statement statement );

        /*
        //from mqlCreateCollection
        DdlManager.getInstance().createCollection(
                namespaceId,
                name,
                true,
                dataStores.isEmpty() ? null : dataStores,
                placementType,
                statement );

         */

        executeStatementInPolypheny( "db.createCollection(\"users\")", "mql", NamespaceType.DOCUMENT );
    }


    private void setGraphStore() {
        //query:CREATE PLACEMENT OF graf11 ON STORE 0 (comes from error message from trying to insert it via ui)
        //TODO(FF): how do i know on which store the graph is on? how do i set this with the create query?????
    }


    /**
     * Gets a list of the column names (seperated by ", ") without brackets
     *
     * @param colIDs list of column ids from which the name is wanted
     * @param logicalColumns list of all logical columns
     * @return list, seperated by a semicolon, with the names of the wanted columns (from the ids)
     */
    private String getListOfCol( List<Long> colIDs, List<LogicalColumn> logicalColumns ) {
        String listOfCols = new String();

        for ( Long colID : colIDs ) {
            String colName = logicalColumns.stream().filter( e -> e.getId() == colID ).findFirst().get().getName();
            listOfCols = listOfCols + colName + ", ";

        }
        if ( listOfCols.length() > 0 ) {
            listOfCols = listOfCols.substring( 0, listOfCols.length() - 2 ); // remove last ", "
        }

        return listOfCols;
    }


    private String createTableQuery( BupSuperEntity<LogicalTable> table, String namespaceName ) {
        String query = new String();
        String columnDefinitions = new String();
        String pkConstraint = new String();
        ImmutableMap<Long, List<LogicalColumn>> columns = bupInformationObject.getColumns();
        ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeys = bupInformationObject.getPrimaryKeysPerTable();
        LogicalTable logicalTable = table.getEntityObject();
        Long tableID = logicalTable.getId();
        List<LogicalColumn> colsPerTable = columns.get( tableID );
        List<LogicalPrimaryKey> pksPerTable = primaryKeys.get( tableID );

        // create the column defintion statement for the table
        for ( LogicalColumn col : colsPerTable ) {
            columnDefinitions = columnDefinitions + createColumnDefinition( col );
            //log.info( columnDefinitions );
        }
        if ( columnDefinitions.length() > 0 ) {
            columnDefinitions = columnDefinitions.substring( 0, columnDefinitions.length() - 2 ); // remove last ", "
        }

        // create the primary key constraint statement for the table
        List<String> colNamesForPK = pksPerTable.get( 0 ).getColumnNames();
        String listOfCols = new String();
        for ( String colName : colNamesForPK ) {
            listOfCols = listOfCols + colName + ", ";
        }
        if ( listOfCols.length() > 0 ) {
            listOfCols = listOfCols.substring( 0, listOfCols.length() - 2 ); // remove last ", "
            pkConstraint = "PRIMARY KEY (" + listOfCols + ")";
        }

        //query to create one table (from the list of tables, from the list of namespaces)
        //TODO(FF): ON STORE storename PARTITION BY partionionInfo
        query = String.format( "CREATE TABLE %s11.%s11 (%s, %s)", namespaceName, table.getNameForQuery(), columnDefinitions, pkConstraint );
        log.info( query );

        return query;
    }


    private String createColumnDefinition( LogicalColumn col ) {
        String columnDefinitionString = new String();
        String colName = col.getName();
        String colDataType = col.getType().toString();
        String colNullable = col.nullableBoolToString();

        String defaultValue = new String();
        if ( !(col.defaultValue == null) ) {
            defaultValue = String.format( " DEFAULT %s", col.defaultValue.value );
        }

        String caseSensitivity = new String();
        if ( !(col.collation == null) ) {
            caseSensitivity = String.format( "COLLATE %s", col.collation.collationToString() );
        }

        String varcharLength = new String();
        if ( !(col.length == null) ) {
            varcharLength = String.format( "(%s) ", col.length.toString() );
        }

        columnDefinitionString = String.format( "%s %s%s %s%s %s, ", colName, colDataType, varcharLength, colNullable, defaultValue, caseSensitivity );
        //log.info( columnDefinitionString );

        return columnDefinitionString;
    }

    /*
    private String collationToString( Collation collation ) {
        try {
            if (collation.equals( Collation.CASE_SENSITIVE )) {
                return "CASE SENSITIVE";
            }
            if ( collation.equals( Collation.CASE_INSENSITIVE ) ) {
                return "CASE INSENSITIVE";
            }
            else {
                throw new RuntimeException( "Collation not supported" );
            }
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }

     */

    /*
    private String nullableBoolToString (boolean nullable) {
        if (nullable) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }

     */


    private void executeStatementInPolypheny( String query, String queryLanguageType, NamespaceType namespaceType ) {
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        try {
            // get a transaction and a statement
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
            statement = transaction.createStatement();

        } catch ( Exception e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }

        try {
            // get algRoot
            Processor queryProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( queryLanguageType ) );
            Node parsed = queryProcessor.parse( query ).get( 0 );

            if ( namespaceType == NamespaceType.RELATIONAL ) {
                //TODO(FF): MqlQueryParamters would require dependency... am i allwoed?
                //MqlQueryParameters parameters = new MqlQueryParameters
            }

            QueryParameters parameters = new QueryParameters( query, namespaceType );

            //ddl?
            result = queryProcessor.prepareDdl( statement, parsed, parameters );



            /*
            AlgRoot algRoot = queryProcessor.translate(
                    statement,
                    queryProcessor.validate( statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean()).left,
                    new QueryParameters( query, NamespaceType.RELATIONAL )
            );
            //get PolyResult from AlgRoot
            final QueryProcessor processor = statement.getQueryProcessor();
            result = processor.prepareQuery( algRoot, true );
             */

        } catch ( Exception e ) {
            log.info( e.getMessage() );
            throw new RuntimeException( e );
        }

    }

}
