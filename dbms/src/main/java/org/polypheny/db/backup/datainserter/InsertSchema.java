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
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.backup.BackupInformationObject;
import org.polypheny.db.backup.BackupEntityWrapper;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.*;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.util.List;
import java.util.Map;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyUserDefinedValue;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class InsertSchema {

    private BackupInformationObject backupInformationObject;
    private final TransactionManager transactionManager;


    public InsertSchema( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    /**
     * Manages the insertion process of the schema
     *
     * @param backupInformationObject contains all the metadata of the schema to be inserted
     */
    public void start( BackupInformationObject backupInformationObject ) {
        log.debug( "insert schemas" );
        this.backupInformationObject = backupInformationObject;
        ImmutableMap<Long, List<BackupEntityWrapper<LogicalTable>>> tables;

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

        //ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> namespaces = backupInformationObject.wrapNamespaces( backupInformationObject.getNamespaces() );
        //backupInformationObject.setWrappedNamespaces( namespaces );
        insertCreateNamespace( backupInformationObject.getWrappedNamespaces() );
        /*
        String query = String.format("CREATE GRAPH NAMESPACE testGraph");
        executeStatementInPolypheny( query, "sql", DataModel.GRAPH);
        String queery = "CREATE PLACEMENT OF testGraph ON STORE hsqldb"; //error: Caused by: org.polypheny.db.languages.sql.parser.impl.ParseException: Encountered "" at line 1, column 1.
        executeStatementInPolypheny( queery, "sql", DataModel.GRAPH);
         */

        //tables = bupInformationObject.transformLogicalEntitiesToBupSuperEntityyy( bupInformationObject.getTables() );
        //bupInformationObject.setBupTables( tables );


        //List<BupSuperEntity<LogicalEntity>> bupEntityList = new ArrayList<>();
        Map<Long, List<BackupEntityWrapper<? extends LogicalEntity>>> tempMap = new HashMap<>();
        //luege öbs goht eso (a de räschtleche ort, ond söcht met instance of caste)

        /*
        for ( Map.Entry<Long, List<LogicalTable>> a : backupInformationObject.getTables().entrySet()) {
            List<BackupEntityWrapper<LogicalEntity>> bupEntityList = new ArrayList<>();
            //TODO(FF): doesn't work with return value :(
            bupEntityList = backupInformationObject.wrapLogicalEntity( a.getValue().stream().map( e -> (LogicalEntity) e ).collect( Collectors.toList()) );
            tempMap.put( a.getKey(), bupEntityList.stream().map( e -> (BackupEntityWrapper<LogicalEntity>) e ).collect( Collectors.toList()));
        }
        //bupInformationObject.setBupTables( ImmutableMap.copyOf( tempMap ) );

         */


        // create table
        //bupInformationObject.transformLogicalEntitiesToBupSuperEntity( bupInformationObject.getTables() );
        //tables = backupInformationObject.tempWrapLogicalTables( backupInformationObject.getTables(), true );
        //backupInformationObject.setWrappedTables( tables );


        //LogicalTable lol = backupInformationObject.getWrappedTables().get( 0 ).get( 0 ).getEntityObject().unwrap( LogicalTable.class );
        //use LogicalEntity.unwrap(LogicalTable.class) for each of the LogicalEntities in wrappedTables
        //ImmutableMap<Long, List<BackupEntityWrapper<LogicalTable>>> tableTables = backupInformationObject.getWrappedTables()

        insertCreateTable( backupInformationObject.getWrappedTables() );

        // alter table - add unique constraint
        //TODO(FF): only call if there are any relational schemas (machts senn??)
        insertAlterTableUQ( backupInformationObject.getWrappedTables(), backupInformationObject.getConstraints() );
        insertAlterTableFK( backupInformationObject.getWrappedTables(), backupInformationObject.getForeignKeysPerTable() );

        // create Collections
        insertCreateCollection( backupInformationObject.getWrappedCollections());

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


    /*
    private void insertRelationalNamespaces() {
        ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> relNamespaces = backupInformationObject.getBupRelNamespaces();
        //String query = "INSERT INTO " + "relational_namespace" + " (id, name, owner, case_sensitive) VALUES (?, ?, ?, ?)";
        String query = new String();

        for ( Map.Entry<Long, BackupEntityWrapper<LogicalNamespace>> ns : relNamespaces.entrySet() ) {

            //query = "CREATE RELATIONAL NAMESPACE " + ns.getValue().getEntityObject().name + ";";
            query = String.format( "CREATE RELATIONAL NAMESPACE %s", ns.getValue().getEntityObject().name );
        }

    }

     */


    private void insertCreateNamespace( ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> namespaces ) {
        String query = new String();
        //TODO(FF): check if namespace already exists, give rename or overwrite option (here, or earlier?), if new name, write it to bupInformationObject

        //TODO(FF): check if namespaces is empty, throw error if it is
        for ( Map.Entry<Long, BackupEntityWrapper<LogicalNamespace>> ns : namespaces.entrySet() ) {
            //only insert namespaces that are marked to be inserted
            if (ns.getValue().getToBeInserted()) {
                //query = "CREATE " + ns.getValue().getEntityObject().dataModel.toString() + " NAMESPACE " + ns.getValue().getEntityObject().name + ";";
                query = String.format( "CREATE %s NAMESPACE %s11", ns.getValue().getEntityObject().dataModel.toString(), ns.getValue().getEntityObject().name );

                //TODO(FF): execute query in polypheny, alter owner, set case sensitivity (how?)
                if ( !ns.getValue().getEntityObject().name.equals( "public" ) ) {

                    switch ( ns.getValue().getEntityObject().dataModel ) {
                        case RELATIONAL:
                            query = String.format( "CREATE %s NAMESPACE %s11", ns.getValue().getEntityObject().dataModel.toString(), ns.getValue().getEntityObject().name );
                            executeStatementInPolypheny( query, ns.getKey(), ns.getValue().getEntityObject().dataModel );
                            break;

                        case DOCUMENT:
                            query = String.format( "CREATE %s NAMESPACE %s11", ns.getValue().getEntityObject().dataModel.toString(), ns.getValue().getEntityObject().name );
                            executeStatementInPolypheny( query, ns.getKey(), DataModel.RELATIONAL );
                            break;

                        case GRAPH:
                            query = String.format( "CREATE DATABASE %s11", ns.getValue().getEntityObject().name );
                            executeStatementInPolypheny( query, ns.getKey(), ns.getValue().getEntityObject().dataModel );
                            break;
                        default:
                            throw new GenericRuntimeException( "During backup schema insertions not supported data model detected" + ns.getValue().getEntityObject().dataModel );
                    }



                    //executeStatementInPolypheny( "CREATE DATABASE product2 IF NOT EXISTS", Catalog.defaultNamespaceId, DataModel.GRAPH );
                    //executeStatementInPolypheny( query, ns.getKey(), ns.getValue().getEntityObject().dataModel );

                    //executeStatementInPolypheny( query, "sql", ns.getValue().getEntityObject().dataModel );

                }
            }
        }


    }


    private void insertCreateTable( ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> tables ) {
        String query = new String();

        // key: namespace id, value: list of tables for the namespace
        for ( Map.Entry<Long, List<BackupEntityWrapper<LogicalEntity>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            //String namespaceName = bupInformationObject.getBupRelNamespaces().get( nsID ).getNameForQuery();
            //only get rel namespaces from all bup namespaces
            String namespaceName = backupInformationObject.getWrappedNamespaces().get( nsID ).getNameForQuery();

            List<BackupEntityWrapper<LogicalEntity>> tablesList = tablesPerNs.getValue();

            // go through each table in the list (of tables for one namespace)
            for ( BackupEntityWrapper<LogicalEntity> table : tablesList ) {
                // only create tables that should be inserted
                if ( table.getToBeInserted()) {
                    // only create tables that don't (exist by default in polypheny)
                    if ( !(table.getEntityObject().entityType.equals( EntityType.SOURCE )) ) {
                        query = createTableQuery( table, namespaceName );
                        executeStatementInPolypheny( query, nsID, DataModel.RELATIONAL );
                    }
                }
            }
        }
    }


    private void insertAlterTableUQ( ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> tables, ImmutableMap<Long, List<LogicalConstraint>> constraints ) {
        String query = new String();

        for ( Map.Entry<Long, List<BackupEntityWrapper<LogicalEntity>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            String namespaceName = backupInformationObject.getWrappedNamespaces().get( nsID ).getNameForQuery();

            List<BackupEntityWrapper<LogicalEntity>> tablesList = tablesPerNs.getValue();

            // go through each constraint in the list (of tables for one namespace)
            for ( BackupEntityWrapper<LogicalEntity> table : tablesList ) {
                //TODO(FF - cosmetic): exclude source tables (for speed)

                // compare the table id with the constraint keys, and if they are the same, create the constraint, and check if it schoult be inserted
                if ( (constraints.containsKey( table.getEntityObject().unwrap( LogicalTable.class ).get().getId() )) && table.getToBeInserted()) {
                    List<LogicalConstraint> constraintsList = constraints.get( table.getEntityObject().unwrap( LogicalTable.class ).get().getId() );
                    List<LogicalColumn> logicalColumns = backupInformationObject.getColumns().get( table.getEntityObject().unwrap( LogicalTable.class ).get().getId() );

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
                        executeStatementInPolypheny( query, nsID, DataModel.RELATIONAL );
                    }
                }
            }
        }
    }


    private void insertAlterTableFK( ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> bupTables, ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable ) {
        String query = new String();
        //if (!foreignKeysPerTable.isEmpty()) {
            // go through foreign key constraints and collect the necessary data
            for ( Map.Entry<Long, List<LogicalForeignKey>> fkListPerTable : foreignKeysPerTable.entrySet() ) {
                if (!(fkListPerTable.getValue().isEmpty())) {
                    Long tableId = fkListPerTable.getKey();

                    for ( LogicalForeignKey foreignKey : fkListPerTable.getValue() ) {
                        // get the table where the foreign key is saved
                        Long nsId = foreignKey.namespaceId;
                        BackupEntityWrapper<LogicalEntity> table = backupInformationObject.getWrappedTables().get( nsId ).stream().filter( e -> e.getEntityObject().unwrap( LogicalTable.class ).get().getId() == tableId ).findFirst().get();
                        //boolean lol = table.getToBeInserted();
                        // check if the table is marked to be inserted
                        if (table.getToBeInserted()) {
                            String namespaceName = backupInformationObject.getWrappedNamespaces().get( foreignKey.namespaceId ).getNameForQuery();
                            String tableName = backupInformationObject.getWrappedTables().get( foreignKey.namespaceId ).stream().filter( e -> e.getEntityObject().unwrap( LogicalTable.class ).get().getId() == foreignKey.tableId ).findFirst().get().getNameForQuery();
                            String constraintName = foreignKey.name;
                            String listOfCols = getListOfCol( foreignKey.columnIds, backupInformationObject.getColumns().get( foreignKey.tableId ) );
                            String referencedNamespaceName = backupInformationObject.getWrappedNamespaces().get( foreignKey.referencedKeySchemaId ).getNameForQuery();
                            String referencedTableName = backupInformationObject.getWrappedTables().get( foreignKey.referencedKeySchemaId ).stream().filter( e -> e.getEntityObject().unwrap( LogicalTable.class ).get().getId() == foreignKey.referencedKeyTableId ).findFirst().get().getNameForQuery();
                            String referencedListOfCols = getListOfCol( foreignKey.referencedKeyColumnIds, backupInformationObject.getColumns().get( foreignKey.referencedKeyTableId ) );
                            String updateAction = foreignKey.updateRule.foreignKeyOptionToString();
                            String deleteAction = foreignKey.deleteRule.foreignKeyOptionToString();
                            //enforcementTime (on commit) - right now is manually set to the same thing everywhere (in the rest of polypheny)

                            query = String.format( "ALTER TABLE %s11.%s11 ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s11.%s11 (%s) ON UPDATE %s ON DELETE %s", namespaceName, tableName, constraintName, listOfCols, referencedNamespaceName, referencedTableName, referencedListOfCols, updateAction, deleteAction );
                            log.info( query );
                            executeStatementInPolypheny( query, nsId, DataModel.RELATIONAL );
                        }
                    }
                }

            }
        //}


    }


    private void insertCreateCollection(ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> wrappedCollections) {
        String query = new String();

        //FIXME(FF): collections are not wrapped yet!!
        // go through all collections per namespace and create and execute a query
        for ( Map.Entry<Long, List<BackupEntityWrapper<LogicalEntity>>> collectionsPerNs : wrappedCollections.entrySet() ) {
            Long nsID = collectionsPerNs.getKey();
            String namespaceName = backupInformationObject.getWrappedNamespaces().get( nsID ).getNameForQuery();

            List<BackupEntityWrapper<LogicalEntity>> collectionsList = collectionsPerNs.getValue();

            // go through each collection in the list (of collections for one namespace)
            for ( BackupEntityWrapper<LogicalEntity> collection : collectionsList ) {
                // only create collections that should be inserted
                if ( collection.getToBeInserted()) {
                    // only create tables that don't (exist by default in polypheny)
                    query = String.format( "db.createCollection(\"%s11\")", collection.getNameForQuery() );
                    log.info( query );
                    executeStatementInPolypheny( query, nsID, DataModel.DOCUMENT );
                }
            }
        }
        //db.createCollection('users')
        //executeStatementInPolypheny( "db.createCollection(\"users\")", Catalog.defaultNamespaceId, DataModel.DOCUMENT );
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


    private String createTableQuery( BackupEntityWrapper<LogicalEntity> table, String namespaceName ) {
        String query = new String();
        String columnDefinitions = new String();
        String pkConstraint = new String();
        ImmutableMap<Long, List<LogicalColumn>> columns = backupInformationObject.getColumns();
        ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeys = backupInformationObject.getPrimaryKeysPerTable();
        LogicalTable logicalTable = table.getEntityObject().unwrap( LogicalTable.class ).get();
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
        if (!(pksPerTable.isEmpty())) {
            List<String> colNamesForPK = pksPerTable.get( 0 ).getColumnNames(); //FIXME(FF): index out of bounds (if there are no pks at all)
            String listOfCols = new String();
            for ( String colName : colNamesForPK ) {
                listOfCols = listOfCols + colName + ", ";
            }
            if ( listOfCols.length() > 0 ) {
                listOfCols = listOfCols.substring( 0, listOfCols.length() - 2 ); // remove last ", "
                pkConstraint = ", PRIMARY KEY (" + listOfCols + ")";
            }
        }



        //query to create one table (from the list of tables, from the list of namespaces)
        //TODO(FF): ON STORE storename PARTITION BY partionionInfo
        //query = String.format( "CREATE TABLE %s11.%s11 (%s, %s)", namespaceName, table.getNameForQuery(), columnDefinitions, pkConstraint );
        query = String.format( "CREATE TABLE %s11.%s11 (%s%s)", namespaceName, table.getNameForQuery(), columnDefinitions, pkConstraint );
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

            /*
            String value = col.defaultValue.value.toTypedJson( ); //'hallo', {"@class":"org.polypheny.db.type.entity.PolyBigDecimal","value":2}
            String value2 = col.defaultValue.value.toJson( );
            String value3 = value2.replaceAll( "\"", "''" );
            value2 = value2.replaceAll( "'", "''" );
            //for string it is this: {"@class":"org.polypheny.db.type.entity.PolyString","value":"hallo","charset":"UTF-16"}, figure out regex to only get value
            //from the string value (in json format), get only the value with regex from string {"@class":"org.polypheny.db.type.entity.PolyString","value":"hallo","charset":"UTF-16"}
            String regexString = value.replaceAll( ".*\"value\":", "" ); //is now value":"hallo","charset":"UTF-16"}, dont want ,"charset":"UTF-16"} part
            regexString = regexString.replaceAll( ",.*", "" );  //correct for varchar, for int it is still 2}, dont want last }
            regexString = regexString.replaceAll( "}", "" );
            regexString = regexString.replaceAll( "\"", "" );


            PolyValue reverse = PolyValue.fromTypedJson( value, PolyValue.class );
            Boolean testing = PolyType.DATETIME_TYPES.contains( col.defaultValue.type );
            */

            //replace ' to '', in case there is a " in the default value
            String value = col.defaultValue.value.toJson( );
            value = value.replaceAll( "'", "''" );

            if (PolyType.CHAR_TYPES.contains( col.defaultValue.type ) || PolyType.DATETIME_TYPES.contains( col.defaultValue.type ) ) {
                //defaultValue = String.format( " DEFAULT '%s'", regexString );
                defaultValue = String.format( " DEFAULT '%s'", value );
                //String test = " DEFAULT '" + regexString + "'";
                String test = " DEFAULT '" + value + "'";
            } else {
                defaultValue = String.format( " DEFAULT %s", value );
            }
            //defaultValue = String.format( " DEFAULT %s", col.defaultValue.value );
            //log.info( "default for " + colDataType + ": " + value2 + " || " + value3);
            //log.info( "default for " + colDataType + ": " + value);
        }

        String caseSensitivity = new String();
        if ( !(col.collation == null) ) {
            caseSensitivity = String.format( "COLLATE %s", col.collation.collationToString() );
        }

        if (colNullable.equals( "NULL" )) {
            colNullable = "";
        } else if (colNullable.equals( "NOT NULL" )) {
            colNullable = String.format( " %s ", colNullable );
        } else {
            throw new GenericRuntimeException( "During backup schema insertions not supported nullable value detected" + colNullable);
        }

        String dataTypeString = new String();
        switch ( colDataType ) {
            case "BIGINT":
            case "BOOLEAN":
            case "DOUBLE":
            case "INTEGER":
            case "REAL":
            case "SMALLINT":
            case "TINYINT":
            case "DATE":
            case "AUDIO":
            case "FILE":
            case "IMAGE":
            case "VIDEO":
                dataTypeString = colDataType;
                break;


            case "TIME":
            case "TIMESTAMP":
                if ( !(col.length == null) ) {
                    dataTypeString = String.format( "%s(%s) ", colDataType, col.length.toString() );
                } else {
                    dataTypeString = colDataType;
                }
                break;


            case "VARCHAR":
                dataTypeString = String.format( "%s(%s) ", colDataType, col.length.toString() );
                break;


            case "DECIMAL":
                if (!(col.length == null))  {
                    if (!(col.scale == null)) {
                        dataTypeString = String.format( "%s(%s, %s) ", colDataType, col.length.toString(), col.scale.toString() );
                    } else {
                        dataTypeString = String.format( "%s(%s) ", colDataType, col.length.toString() );
                    }
                } else {
                    dataTypeString = colDataType;
                }
                break;

            default:
                throw new GenericRuntimeException( "During backup schema insertions not supported datatype detected" + colDataType);
        }

        String arrayString = new String();

        if (!(col.collectionsType == null)) {
            String collectionsType = col.collectionsType.toString();

            switch ( collectionsType ) {
                case "ARRAY":
                    arrayString = String.format( " ARRAY (%s, %s) ", col.dimension.toString(), col.cardinality.toString() );
                    break;

                default:
                    throw new GenericRuntimeException( "During backup schema insertions not supported collectionstype detected" + collectionsType);
            }
        }


        columnDefinitionString = String.format( "%s %s%s%s%s %s, ", colName, dataTypeString, arrayString, colNullable, defaultValue, caseSensitivity );
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


    private void executeStatementInPolypheny( String query, Long namespaceId, DataModel dataModel ) {
        log.info( "entered execution with query:"+query );
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        //TODO: use anyquery, rest not necessary

        switch ( dataModel ) {
            case RELATIONAL:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    transaction.commit();

                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while starting transaction: " + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while starting transaction: " + e.getMessage() );
                }
                break;

            case DOCUMENT:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "mql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    transaction.commit();

                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while starting transaction" + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while starting transaction" + e.getMessage() );
                }
                break;

            case GRAPH:
                try {
                    // get a transaction and a statement
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
                    statement = transaction.createStatement();
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "cypher" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    transaction.commit();

                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while starting transaction"+ e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while starting transaction"+ e.getMessage() );
                }
                break;

            default:
                throw new RuntimeException( "Backup - InsertSchema: DataModel not supported" );
        }

        //just to keep it
        int i = 1;
        if(i == 0) {
            try {
                // get a transaction and a statement
                transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
                statement = transaction.createStatement();
                ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( Catalog.defaultNamespaceId ).build(), statement ).get( 0 );
                // in case of results
                ResultIterator iter = executedQuery.getIterator();
                while ( iter.hasMoreRows() ) {
                    // liste mit tuples
                    iter.getNextBatch();
                }

            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Error while starting transaction" + e.getMessage() );
            }
        }

        /*

        try {
            // get algRoot
            Processor queryProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( queryLanguageType ) );
            Node parsed = queryProcessor.parse( query ).get( 0 );

            if ( dataModel == DataModel.RELATIONAL ) {
                //TODO(FF): MqlQueryParamters would require dependency... am i allwoed?
                //MqlQueryParameters parameters = new MqlQueryParameters
            }

            QueryParameters parameters = new QueryParameters( query, dataModel );

            //ddl?
            //result = queryProcessor.prepareDdl( statement, parsed, parameters );


            //from here on again comment
            AlgRoot algRoot = queryProcessor.translate(
                    statement,
                    queryProcessor.validate( statement.getTransaction(), sqlNode, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean()).left,
                    new QueryParameters( query, DataModel.RELATIONAL )
            );
            //get PolyResult from AlgRoot
            final QueryProcessor processor = statement.getQueryProcessor();
            result = processor.prepareQuery( algRoot, true );


        } catch ( Exception e ) {
            log.info( e.getMessage() );
            log.info( "exception while executing query: "+ query );
            throw new RuntimeException( e );
        }
        */

    }

}
