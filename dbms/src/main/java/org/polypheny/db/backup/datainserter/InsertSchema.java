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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.backup.BackupEntityWrapper;
import org.polypheny.db.backup.BackupInformationObject;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;

/**
 * This class inserts the schema of the backup into Polypheny-DB
 */
@Slf4j
public class InsertSchema {

    public static final String BACKUP_MANAGER = "Backup Manager";
    private BackupInformationObject backupInformationObject;
    private final TransactionManager transactionManager;


    public InsertSchema( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    /**
     * Manages the insertion process of the schema
     * @param backupInformationObject contains all the metadata of the schema to be inserted
     */
    public void start( BackupInformationObject backupInformationObject ) {
        log.debug( "insert schemas" );
        this.backupInformationObject = backupInformationObject;
        ImmutableMap<Long, List<BackupEntityWrapper<LogicalTable>>> tables;

        insertCreateNamespace( backupInformationObject.getWrappedNamespaces() );

        Map<Long, List<BackupEntityWrapper<? extends LogicalEntity>>> tempMap = new HashMap<>();


        insertCreateTable( backupInformationObject.getWrappedTables() );

        // alter table - add unique constraint
        //TODO(FF): only call if there are any relational schemas
        insertAlterTableUQ( backupInformationObject.getWrappedTables(), backupInformationObject.getConstraints() );
        insertAlterTableFK( backupInformationObject.getWrappedTables(), backupInformationObject.getForeignKeysPerTable() );

        // create Collections
        insertCreateCollection( backupInformationObject.getWrappedCollections() );

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


    /**
     * creates a "create namespace" query and executes it in polypheny for all namespaces that are marked to be inserted and are passed in the namespaces map
     * @param namespaces map of namespaces to be inserted, where the key is the namespace id and the value is the wrapped namespace
     */
    private void insertCreateNamespace( ImmutableMap<Long, BackupEntityWrapper<LogicalNamespace>> namespaces ) {
        String query = new String();
        //TODO(FF): check if namespace already exists, give rename or overwrite option (here, or earlier?), if new name, write it to bupInformationObject

        //TODO(FF): check if namespaces is empty, throw error if it is
        for ( Map.Entry<Long, BackupEntityWrapper<LogicalNamespace>> ns : namespaces.entrySet() ) {
            //only insert namespaces that are marked to be inserted
            if ( ns.getValue().getToBeInserted() ) {
                //query = "CREATE " + ns.getValue().getEntityObject().dataModel.toString() + " NAMESPACE " + ns.getValue().getEntityObject().name + ";";
                query = String.format( "CREATE %s NAMESPACE %s", ns.getValue().getEntityObject().dataModel.toString(), ns.getValue().getEntityObject().name );

                //TODO(FF): execute query in polypheny, alter owner, set case sensitivity (how?)
                if ( !ns.getValue().getEntityObject().name.equals( "public" ) ) {

                    switch ( ns.getValue().getEntityObject().dataModel ) {
                        case RELATIONAL:
                            query = String.format( "CREATE %s NAMESPACE %s", ns.getValue().getEntityObject().dataModel.toString(), ns.getValue().getEntityObject().name );
                            executeStatementInPolypheny( query, ns.getKey(), ns.getValue().getEntityObject().dataModel );
                            break;

                        case DOCUMENT:
                            query = String.format( "CREATE %s NAMESPACE %s", ns.getValue().getEntityObject().dataModel.toString(), ns.getValue().getEntityObject().name );
                            executeStatementInPolypheny( query, ns.getKey(), DataModel.RELATIONAL );
                            break;

                        case GRAPH:
                            query = String.format( "CREATE DATABASE %s", ns.getValue().getEntityObject().name );
                            executeStatementInPolypheny( query, ns.getKey(), ns.getValue().getEntityObject().dataModel );
                            break;
                        default:
                            throw new GenericRuntimeException( "During backup schema insertions not supported data model detected" + ns.getValue().getEntityObject().dataModel );
                    }
                }
            }
        }
    }


    /**
     * Sets an order for a "create table" query and executes it in polypheny for all table in the tables map. The creation query only contains the columns and the primary key
     * @param tables map of tables to be inserted, where the key is the namespace id and the value is a list of wrapped tables that are in this namespace
     */
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
                if ( table.getToBeInserted() ) {
                    // only create tables that don't (exist by default in polypheny)
                    if ( !(table.getEntityObject().entityType.equals( EntityType.SOURCE )) ) {
                        query = createTableQuery( table, namespaceName );
                        executeStatementInPolypheny( query, nsID, DataModel.RELATIONAL );
                    }
                }
            }
        }
    }


    /**
     * Creates a "alter table" query, that sets a unique constraint on a table, and executes it in polypheny for all constraints in the constraints map
     * @param tables map of tables to be altered, where the key is the table id and the value is a list of wrapped tables that are in this namespace
     * @param constraints map of constraints to be inserted, where the key is the table id and the value is a list of constraints that are in this table
     */
    private void insertAlterTableUQ( ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> tables, ImmutableMap<Long, List<LogicalConstraint>> constraints ) {
        String query = new String();

        for ( Map.Entry<Long, List<BackupEntityWrapper<LogicalEntity>>> tablesPerNs : tables.entrySet() ) {
            Long nsID = tablesPerNs.getKey();
            String namespaceName = backupInformationObject.getWrappedNamespaces().get( nsID ).getNameForQuery();

            List<BackupEntityWrapper<LogicalEntity>> tablesList = tablesPerNs.getValue();

            // go through each constraint in the list (of tables for one namespace)
            for ( BackupEntityWrapper<LogicalEntity> table : tablesList ) {
                //TODO:FF (low priority): exclude source tables (for speed)

                // compare the table id with the constraint keys, and if they are the same, create the constraint, and check if it schoult be inserted
                if ( (constraints.containsKey( table.getEntityObject().unwrap( LogicalTable.class ).get().getId() )) && table.getToBeInserted() ) {
                    List<LogicalConstraint> constraintsList = constraints.get( table.getEntityObject().unwrap( LogicalTable.class ).get().getId() );
                    List<LogicalColumn> logicalColumns = backupInformationObject.getColumns().get( table.getEntityObject().unwrap( LogicalTable.class ).get().getId() );

                    // go through all constraints per table
                    for ( LogicalConstraint constraint : constraintsList ) {
                        if ( constraint.type.equals( ConstraintType.UNIQUE ) ) {
                            String tableName = table.getNameForQuery();
                            String constraintName = constraint.name;
                            String listOfCols = new String();

                            List<Long> colIDs = constraint.getKey().fieldIds;

                            // get all column-names used in the constraint from the columns
                            listOfCols = getListOfCol( colIDs, logicalColumns );

                            query = String.format( "ALTER TABLE %s.%s ADD CONSTRAINT %s UNIQUE (%s)", namespaceName, tableName, constraintName, listOfCols );
                            log.info( query );
                            executeStatementInPolypheny( query, nsID, DataModel.RELATIONAL );
                        }
                    }
                }
            }
        }
    }


    /**
     * Creates a "alter table" query, that sets a foreign key constraint on a table, and executes it in polypheny for all foreign keys in the foreignKeysPerTable map
     * @param bupTables (deprecated) map of tables to be altered, where the key is the table id and the value is a list of wrapped tables that are in this namespace
     * @param foreignKeysPerTable map of foreign keys to be inserted, where the key is the table id and the value is a list of foreign keys that are in this table
     */
    private void insertAlterTableFK( ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> bupTables, ImmutableMap<Long, List<LogicalForeignKey>> foreignKeysPerTable ) {
        String query = new String();
        //if (!foreignKeysPerTable.isEmpty()) {
        // go through foreign key constraints and collect the necessary data
        for ( Map.Entry<Long, List<LogicalForeignKey>> fkListPerTable : foreignKeysPerTable.entrySet() ) {
            if ( !(fkListPerTable.getValue().isEmpty()) ) {
                Long tableId = fkListPerTable.getKey();

                for ( LogicalForeignKey foreignKey : fkListPerTable.getValue() ) {
                    // get the table where the foreign key is saved
                    Long nsId = foreignKey.namespaceId;
                    BackupEntityWrapper<LogicalEntity> table = backupInformationObject.getWrappedTables().get( nsId ).stream().filter( e -> e.getEntityObject().unwrap( LogicalTable.class ).orElseThrow().getId() == tableId ).findFirst().orElseThrow();
                    //boolean lol = table.getToBeInserted();
                    // check if the table is marked to be inserted
                    if ( table.getToBeInserted() ) {
                        String namespaceName = backupInformationObject.getWrappedNamespaces().get( foreignKey.namespaceId ).getNameForQuery();
                        String tableName = backupInformationObject.getWrappedTables().get( foreignKey.namespaceId ).stream().filter( e -> e.getEntityObject().unwrap( LogicalTable.class ).orElseThrow().getId() == foreignKey.entityId ).findFirst().orElseThrow().getNameForQuery();
                        String constraintName = foreignKey.name;
                        String listOfCols = getListOfCol( foreignKey.fieldIds, backupInformationObject.getColumns().get( foreignKey.entityId ) );
                        String referencedNamespaceName = backupInformationObject.getWrappedNamespaces().get( foreignKey.referencedKeyNamespaceId ).getNameForQuery();
                        String referencedTableName = backupInformationObject.getWrappedTables().get( foreignKey.referencedKeyNamespaceId ).stream().filter( e -> e.getEntityObject().unwrap( LogicalTable.class ).orElseThrow().getId() == foreignKey.referencedKeyEntityId ).findFirst().orElseThrow().getNameForQuery();
                        String referencedListOfCols = getListOfCol( foreignKey.referencedKeyFieldIds, backupInformationObject.getColumns().get( foreignKey.referencedKeyEntityId ) );
                        String updateAction = foreignKey.updateRule.name();
                        String deleteAction = foreignKey.deleteRule.name();
                        //enforcementTime (on commit) - right now is manually set to the same thing everywhere (in the rest of polypheny)

                        query = String.format( "ALTER TABLE %s.%s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s.%s (%s) ON UPDATE %s ON DELETE %s", namespaceName, tableName, constraintName, listOfCols, referencedNamespaceName, referencedTableName, referencedListOfCols, updateAction, deleteAction );
                        log.info( query );
                        executeStatementInPolypheny( query, nsId, DataModel.RELATIONAL );
                    }
                }
            }

        }
        //}

    }


    /**
     * Creates a "create collecton" query and executes it in polypheny for all collections in the wrappedCollections map
     * @param wrappedCollections map of collections to be inserted, where the key is the namespace id and the value is a list of wrapped collections that are in this namespace
     */
    private void insertCreateCollection( ImmutableMap<Long, List<BackupEntityWrapper<LogicalEntity>>> wrappedCollections ) {
        String query = new String();

        //FIXME(FF): collections are not wrapped yet!!
        // go through all collections per namespace and create and execute a query
        for ( Map.Entry<Long, List<BackupEntityWrapper<LogicalEntity>>> collectionsPerNs : wrappedCollections.entrySet() ) {
            Long nsIDOriginal = collectionsPerNs.getKey();
            String namespaceName = backupInformationObject.getWrappedNamespaces().get( nsIDOriginal ).getNameForQuery();
            long nsId = Catalog.snapshot().getNamespace( namespaceName ).orElseThrow().id;

            List<BackupEntityWrapper<LogicalEntity>> collectionsList = collectionsPerNs.getValue();

            // go through each collection in the list (of collections for one namespace)
            for ( BackupEntityWrapper<LogicalEntity> collection : collectionsList ) {
                // only create collections that should be inserted
                if ( collection.getToBeInserted() ) {
                    // only create tables that don't (exist by default in polypheny)
                    query = String.format( "db.createCollection(\"%s\")", collection.getNameForQuery() );
                    log.info( query );
                    executeStatementInPolypheny( query, nsId, DataModel.DOCUMENT );
                }
            }
        }
        //db.createCollection('users')
        //executeStatementInPolypheny( "db.createCollection(\"users\")", Catalog.defaultNamespaceId, DataModel.DOCUMENT );
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


    /**
     * Creates a "create table" query for one table and returns it, the query only contains the columns and the primary key
     * @param table wrapped table to be inserted
     * @param namespaceName name of the namespace of the table
     * @return the query to create the table
     */
    private String createTableQuery( BackupEntityWrapper<LogicalEntity> table, String namespaceName ) {
        String query = new String();
        String columnDefinitions = "";
        String pkConstraint = "";
        ImmutableMap<Long, List<LogicalColumn>> columns = backupInformationObject.getColumns();
        ImmutableMap<Long, List<LogicalPrimaryKey>> primaryKeys = backupInformationObject.getPrimaryKeysPerTable();
        LogicalTable logicalTable = table.getEntityObject().unwrap( LogicalTable.class ).orElseThrow();
        Long tableID = logicalTable.getId();
        List<LogicalColumn> colsPerTable = columns.get( tableID );
        List<LogicalPrimaryKey> pksPerTable = primaryKeys.get( tableID );

        // create the column defintion statement for the table
        for ( LogicalColumn col : colsPerTable ) {
            columnDefinitions = columnDefinitions + createColumnDefinition( col );
            //log.info( columnDefinitions );
        }
        if ( !columnDefinitions.isEmpty() ) {
            columnDefinitions = columnDefinitions.substring( 0, columnDefinitions.length() - 2 ); // remove last ", "
        }

        // create the primary key constraint statement for the table
        if ( !(pksPerTable.isEmpty()) ) {
            String listOfCols = new String();
            for ( long columnId : pksPerTable.get( 0 ).fieldIds ) {
                String colName =  colsPerTable.stream().filter( e -> e.getId() == columnId ).findFirst().get().getName();
                listOfCols = listOfCols + colName + ", ";
            }
            if ( !listOfCols.isEmpty() ) {
                listOfCols = listOfCols.substring( 0, listOfCols.length() - 2 ); // remove last ", "
                pkConstraint = ", PRIMARY KEY (" + listOfCols + ")";
            }
        }

        //query to create one table (from the list of tables, from the list of namespaces)
        //TODO(FF): ON STORE storename PARTITION BY partionionInfo
        //query = String.format( "CREATE TABLE %s.%s (%s, %s)", namespaceName, table.getNameForQuery(), columnDefinitions, pkConstraint );
        query = String.format( "CREATE TABLE %s.%s (%s%s)", namespaceName, table.getNameForQuery(), columnDefinitions, pkConstraint );
        log.info( query );

        return query;
    }


    /**
     * Creates the column definition part of a "create table" query
     * @param col logical column for which the column definition should be created
     * @return the column definition part of a "create table" query
     */
    private String createColumnDefinition( LogicalColumn col ) {
        String columnDefinitionString = new String();
        String colName = col.getName();
        String colDataType = col.getType().toString();
        String colNullable = "";
        String defaultValue = new String();
        if ( !(col.defaultValue == null) ) {

            //replace ' to '', in case there is a " in the default value
            String value = col.defaultValue.value.toJson();
            value = value.replaceAll( "'", "''" );

            if ( PolyType.CHAR_TYPES.contains( col.defaultValue.type ) || PolyType.DATETIME_TYPES.contains( col.defaultValue.type ) ) {
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
            // Remove the "_" from the collation enum standard
            String collation = col.collation.toString().replaceAll( "_", " " );
            caseSensitivity = String.format( "COLLATE %s", collation);
        }

        if ( col.nullable ) {
            colNullable = "";
        } else if ( !col.nullable ) {
            colNullable = " NOT NULL ";
        } else {
            throw new GenericRuntimeException( "During backup schema insertions not supported nullable value detected" + colNullable );
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
                if ( !(col.length == null) ) {
                    if ( !(col.scale == null) ) {
                        dataTypeString = String.format( "%s(%s, %s) ", colDataType, col.length.toString(), col.scale.toString() );
                    } else {
                        dataTypeString = String.format( "%s(%s) ", colDataType, col.length.toString() );
                    }
                } else {
                    dataTypeString = colDataType;
                }
                break;

            default:
                throw new GenericRuntimeException( "During backup schema insertions not supported datatype detected" + colDataType );
        }

        String arrayString = new String();

        if ( !(col.collectionsType == null) ) {
            String collectionsType = col.collectionsType.toString();

            switch ( collectionsType ) {
                case "ARRAY":
                    arrayString = String.format( " ARRAY (%s, %s) ", col.dimension.toString(), col.cardinality.toString() );
                    break;

                default:
                    throw new GenericRuntimeException( "During backup schema insertions not supported collectionstype detected" + collectionsType );
            }
        }

        columnDefinitionString = String.format( "%s %s%s%s%s %s, ", colName, dataTypeString, arrayString, colNullable, defaultValue, caseSensitivity );
        //log.info( columnDefinitionString );

        return columnDefinitionString;
    }


    //(implemented at other place, but may change there. This is kept for reference)
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


    //(implemented at other place, but may change there. This is kept for reference)
    /*
    private String nullableBoolToString (boolean nullable) {
        if (nullable) {
            return "NULL";
        } else {
            return "NOT NULL";
        }
    }
     */


    /**
     * Executes a query in polypheny
     * @param query query to be executed
     * @param namespaceId namespace id of where the query should be executed
     * @param dataModel data model of the query
     */
    private void executeStatementInPolypheny( String query, Long namespaceId, DataModel dataModel ) {
        log.info( "entered execution with query:" + query );
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
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery(
                            QueryContext.builder()
                                    .language( QueryLanguage.from( "sql" ) )
                                    .query( query )
                                    .origin( BACKUP_MANAGER )
                                    .transactionManager( transactionManager )
                                    .namespaceId( namespaceId )
                                    .statement( statement )
                                    .build()
                                    .addTransaction( transaction ) ).get( 0 );
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
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                            .anyQuery(
                                    QueryContext.builder()
                                            .language( QueryLanguage.from( "mql" ) )
                                            .query( query )
                                            .origin( BACKUP_MANAGER )
                                            .transactionManager( transactionManager )
                                            .namespaceId( namespaceId )
                                            .statement( statement )
                                            .build()
                                            .addTransaction( transaction ) ).get( 0 );
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
                    ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                            .anyQuery(
                                    QueryContext.builder().language( QueryLanguage.from( "cypher" ) )
                                            .query( query )
                                            .origin( BACKUP_MANAGER )
                                            .transactionManager( transactionManager )
                                            .namespaceId( namespaceId )
                                            .statement( statement )
                                            .build()
                                            .addTransaction( transaction ) ).get( 0 );
                    transaction.commit();

                } catch ( Exception e ) {
                    throw new GenericRuntimeException( "Error while starting transaction" + e.getMessage() );
                } catch ( TransactionException e ) {
                    throw new GenericRuntimeException( "Error while starting transaction" + e.getMessage() );
                }
                break;

            default:
                throw new RuntimeException( "Backup - InsertSchema: DataModel not supported" );
        }

        //just to keep it
        int i = 1;
        if ( i == 0 ) {
            try {
                // get a transaction and a statement
                transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Inserter" );
                statement = transaction.createStatement();
                ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                        .anyQuery(
                                QueryContext.builder()
                                        .language( QueryLanguage.from( "sql" ) )
                                        .query( query )
                                        .origin( BACKUP_MANAGER )
                                        .transactionManager( transactionManager )
                                        .namespaceId( Catalog.defaultNamespaceId )
                                        .statement( statement )
                                        .build()
                                        .addTransaction( transaction ) ).get( 0 );
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

    }

}
