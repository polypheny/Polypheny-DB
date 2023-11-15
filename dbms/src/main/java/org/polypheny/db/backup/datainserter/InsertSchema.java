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
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.backup.BupInformationObject;
import org.polypheny.db.backup.BupSuperEntity;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryProcessor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class InsertSchema {
    private BupInformationObject bupInformationObject;
    private final TransactionManager transactionManager;


    public InsertSchema( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }

    /**
     * Manages the insertion process of the schema
     * @param bupInformationObject contains all the metadata of the schema to be inserted
     */
    public void start( BupInformationObject bupInformationObject ) {
        log.debug( "insert schemas" );
        this.bupInformationObject = bupInformationObject;

        /*
        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> temp = bupInformationObject.transformNamespacesToBupSuperEntityMap( bupInformationObject.getRelNamespaces() );
        bupInformationObject.setBupRelNamespaces( temp );
        insertCreateNamespace( bupInformationObject.getBupRelNamespaces() );
        insertCreateNamespace( bupInformationObject.getBupDocNamespaces() );
        insertCreateNamespace( bupInformationObject.getBupGraphNamespaces() );

         */

        ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> namespaces = bupInformationObject.transformNamespacesToBupSuperEntityMap( bupInformationObject.getNamespaces() );
        bupInformationObject.setBupNamespaces( namespaces );
        insertCreateNamespace( bupInformationObject.getBupNamespaces() );

        for ( Map.Entry<Long, List<LogicalTable>> a : bupInformationObject.getTables().entrySet()) {
            bupInformationObject.transformLogigalEntityToSuperEntity( a.getValue().stream().map( e -> (LogicalEntity) e ).collect( Collectors.toList()) );
        }
        //bupInformationObject.transformLogicalEntitiesToBupSuperEntity( bupInformationObject.getTables() );
        ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tables = bupInformationObject.tempTableTransformation( bupInformationObject.getTables(), true );
        bupInformationObject.setBupTables( tables );
        insertCreateTable( bupInformationObject.getBupTables() );

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
            query = String.format("CREATE RELATIONAL NAMESPACE %s", ns.getValue().getEntityObject().name);
        }

    }

    private void insertCreateNamespace( ImmutableMap<Long, BupSuperEntity<LogicalNamespace>> namespaces) {
        String query = new String();
        //TODO(FF): check if namespace already exists, give rename or overwrite option (here, or earlier?), if new name, write it to bupInformationObject

        //TODO(FF): check if namespaces is empty, throw error if it is
        for ( Map.Entry<Long, BupSuperEntity<LogicalNamespace>> ns : namespaces.entrySet() ) {
            //query = "CREATE " + ns.getValue().getEntityObject().namespaceType.toString() + " NAMESPACE " + ns.getValue().getEntityObject().name + ";";
            query = String.format("CREATE %s NAMESPACE %s11", ns.getValue().getEntityObject().namespaceType.toString(), ns.getValue().getEntityObject().name);

            //TODO(FF): execute query in polypheny, alter owner, set case sensitivity (how?)
            if (!ns.getValue().getEntityObject().name.equals( "public" )) {
                executeStatementinPolypheny( query, "sql", NamespaceType.RELATIONAL );
            }
        }


    }

    private void insertCreateTable( ImmutableMap<Long, List<BupSuperEntity<LogicalTable>>> tables ) {
        String query = new String();
        //SQL query = new SQL();

        // key: namespace id, value: list of tables for the namespace
        for ( Map.Entry<Long, List<BupSuperEntity<LogicalTable>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            //String namespaceName = bupInformationObject.getBupRelNamespaces().get( nsID ).getNameForQuery();
            //only get rel namespaces from all bup namespaces
            String namespaceName = bupInformationObject.getBupNamespaces().get( nsID ).getNameForQuery();

            List<BupSuperEntity<LogicalTable>> tablesList = tablesPerNs.getValue();

            // go through each table in the list (of tables for one namespace)
            for ( BupSuperEntity<LogicalTable> table : tablesList ) {
                //TODO(FF): Insert if clause to filter for preexisting tables (like emps)
                EntityType lol = table.getEntityObject().entityType;
                if (table.getEntityObject().entityType.equals( EntityType.SOURCE )) {
                    //dont create table
                    int lool = 0;
                } else {
                    String columnDefinitions = new String();
                    String pkConstraint = new String();
                    ImmutableMap<Long, List<LogicalColumn>> columns = bupInformationObject.getColumns();
                    ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeys = bupInformationObject.getPrimaryKeysPerTable();
                    LogicalTable logicalTable = table.getEntityObject();
                    Long tableID = logicalTable.getId();
                    List<LogicalColumn> colsPerTable = columns.get( tableID );
                    List<LogicalPrimaryKey> pksPerTable = primaryKeys.get( tableID );

                    // create the column defintion statement for the table
                    //java.lang.NullPointerException: Cannot read field "value" because "col.defaultValue" is null
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
                    for (String colName : colNamesForPK) {
                        listOfCols = listOfCols + colName + ", ";
                    }
                    if (listOfCols.length() > 0) {
                        listOfCols = listOfCols.substring( 0, listOfCols.length() - 2 ); // remove last ", "
                        pkConstraint = "PRIMARY KEY (" + listOfCols + ")";
                    }

                    //query to create one table (from the list of tables, from the list of namespaces)
                    //TODO(FF): ON STORE storename PARTITION BY partionionInfo
                    //query = "CREATE TABLE " + namespaceName + "." + table.getNameForQuery() + " ( " + columnDefinitions + ", " + pkConstraint + " );";
                    query = String.format("CREATE TABLE %s.%s ( %s, %s )", namespaceName, table.getNameForQuery(), columnDefinitions, pkConstraint);
                    log.info( query );

                }

            }
        }
    }

    private String createColumnDefinition( LogicalColumn col) {
        String columnDefinitionString = new String();
        String colName = col.getName();
        String colDataType = col.getType().toString();
        String colNullable = nullableBoolToString( col.isNullable() );

        String defaultValue = new String();
        if ( !(col.defaultValue == null) ) {
            defaultValue = String.format( " DEFAULT %s", col.defaultValue.value);
        }

        String caseSensitivity = new String();
        if ( !(col.collation == null) ) {
            //caseSensitivity = col.collation.toString();
            caseSensitivity = String.format( "COLLATE %s", col.collation.toString());
        }

        columnDefinitionString = String.format("%s %s %s%s %s , ", colName, colDataType, colNullable, defaultValue, caseSensitivity);
        //log.info( columnDefinitionString );

        return columnDefinitionString;
    }

    private String nullableBoolToString (boolean nullable) {
        if (nullable) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }

    private void executeStatementinPolypheny (String query, String queryLanguageType, NamespaceType namespaceType) {
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        //TODO(FF): aber öberd jdbc connection mache macht jo au gaar kei senn...???????
        try {
            // get a transaction and a statement
            transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Index Manager" );
            statement = transaction.createStatement();

        } catch ( Exception e ) {
            throw new RuntimeException( "Error while starting transactoin", e );
        }

        try {
            // get algRoot
            Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( queryLanguageType ));
            //TODO(FF): fails at this step... error:org.polypheny.db.catalog.exceptions.GenericRuntimeException: org.polypheny.db.languages.NodeParseException: Encountered " ";" "; "" at line 1, column 35.
            // Was expecting:
            //     <EOF>
            //my query looks like this (works via ui): CREATE RELATIONAL NAMESPACE reli11;
            Node parsed = sqlProcessor.parse( query ).get( 0 );
            QueryParameters parameters = new QueryParameters( query, namespaceType );

            //ddl?
            result = sqlProcessor.prepareDdl( statement, parsed, parameters );



            /*
            AlgRoot algRoot = sqlProcessor.translate(
                    statement,
                    sqlProcessor.validate( statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean()).left,
                    new QueryParameters( query, NamespaceType.RELATIONAL )
            );
            //get PolyResult from AlgRoot
            final QueryProcessor processor = statement.getQueryProcessor();
            result = processor.prepareQuery( algRoot, true );
             */

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }

    }
}
