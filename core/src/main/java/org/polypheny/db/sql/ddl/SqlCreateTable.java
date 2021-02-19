/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.ddl;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.catalog.NameGenerator;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownCollationException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlCreate;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
@Slf4j
public class SqlCreateTable extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    private final SqlNode query;
    private final SqlIdentifier store;
    private final SqlIdentifier partitionColumn;
    private final SqlIdentifier partitionType;
    private final int numPartitions;
    private final List<SqlIdentifier> partitionNamesList;

    private final List<List<SqlNode>> partitionQualifierList;


    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE TABLE", SqlKind.CREATE_TABLE );


    /**
     * Creates a SqlCreateTable.
     */
    SqlCreateTable(
            SqlParserPos pos,
            boolean replace,
            boolean ifNotExists,
            SqlIdentifier name,
            SqlNodeList columnList,
            SqlNode query,
            SqlIdentifier store,
            SqlIdentifier partitionType,
            SqlIdentifier partitionColumn,
            int numPartitions,
            List<SqlIdentifier> partitionNamesList,
            List<List<SqlNode>> partitionQualifierList ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
        this.store = store; // ON STORE [store name]; may be null
        this.partitionType = partitionType; // PARTITION BY (HASH | RANGE | LIST); may be null
        this.partitionColumn = partitionColumn; // may be null
        this.numPartitions = numPartitions; //May be null and can only be used in association with PARTITION BY
        this.partitionNamesList = partitionNamesList; //May be null and can only be used in association with PARTITION BY and PARTITIONS
        this.partitionQualifierList = partitionQualifierList;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        // TODO @HENNLO: The partition part is still incomplete
        /** There are several possible ways to unparse the partition section.
         The To Do is deferred until we have decided if parsing of partition functions will be
         self contained or not. If not than we need to unparse
         `WITH PARTITIONS 3`
         or something like
         `(
            PARTITION a892_233 VALUES(892, 233),
            PARTITION a1001_1002 VALUES(1001, 1002),
            PARTITION a8000_4003 VALUES(8000, 4003),
            PARTITION a900_999 VALUES(900, 999)
         )`*/

        writer.keyword( "CREATE" );
        writer.keyword( "TABLE" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
        if ( columnList != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columnList ) {
                writer.sep( "," );
                c.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        if ( query != null ) {
            writer.keyword( "AS" );
            writer.newlineAndIndent();
            query.unparse( writer, 0, 0 );
        }
        if ( store != null ) {
            writer.keyword( "ON STORE" );
            store.unparse( writer, 0, 0 );
        }
        if ( partitionType != null ) {
            writer.keyword( " PARTITION" );
            writer.keyword( " BY" );
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            partitionColumn.unparse( writer, 0, 0 );
            writer.endList( frame );
        }
    }


    @Override
    public void execute( Context context, Statement statement ) {
        if ( query != null ) {
            throw new RuntimeException( "Not supported yet" );
        }
        Catalog catalog = Catalog.getInstance();
        String tableName;
        long schemaId;
        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = catalog.getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                tableName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), name.names.get( 0 ) ).id;
                tableName = name.names.get( 1 );
            } else { // TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableName = name.names.get( 0 );
            }
        } catch ( UnknownDatabaseException e ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.databaseNotFound( name.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.schemaNotFound( name.toString() ) );
        }

        try {
            // Check if there is already a table with this name
            if ( catalog.checkIfExistsTable( schemaId, tableName ) ) {
                if ( ifNotExists ) {
                    // It is ok that there is already a table with this name because "IF NOT EXISTS" was specified
                    return;
                } else {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableExists( tableName ) );
                }
            }

            if ( this.columnList == null ) {
                // "CREATE TABLE t" is invalid; because there is no "AS query" we need a list of column names and types, "CREATE TABLE t (INT c)".
                throw SqlUtil.newContextException( SqlParserPos.ZERO, RESOURCE.createTableRequiresColumnList() );
            }

            List<DataStore> stores;
            if ( this.store != null ) {
                stores = ImmutableList.of( getDataStoreInstance( store ) );
            } else {
                // Ask router on which store(s) the table should be placed
                stores = statement.getRouter().createTable( schemaId, statement );
            }

            long tableId = catalog.addTable(
                    tableName,
                    schemaId,
                    context.getCurrentUserId(),
                    TableType.TABLE,
                    true,
                    null );

            List<SqlNode> columnList = this.columnList.getList();
            int position = 1;
            for ( Ord<SqlNode> c : Ord.zip( columnList ) ) {
                if ( c.e instanceof SqlColumnDeclaration ) {
                    final SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) c.e;
                    final PolyType dataType = PolyType.get( columnDeclaration.dataType.getTypeName().getSimple() );
                    final PolyType collectionsType = columnDeclaration.dataType.getCollectionsTypeName() == null ?
                            null : PolyType.get( columnDeclaration.dataType.getCollectionsTypeName().getSimple() );
                    Collation collation = null;
                    if ( dataType.getFamily() == PolyTypeFamily.CHARACTER ) {
                        if ( columnDeclaration.collation != null ) {
                            collation = Collation.parse( columnDeclaration.collation );
                        } else {
                            collation = Collation.getById( RuntimeConfig.DEFAULT_COLLATION.getInteger() ); // Set default collation
                        }
                    }
                    long addedColumnId = catalog.addColumn(
                            columnDeclaration.name.getSimple(),
                            tableId,
                            position++,
                            dataType,
                            collectionsType,
                            columnDeclaration.dataType.getPrecision() == -1 ? null : columnDeclaration.dataType.getPrecision(),
                            columnDeclaration.dataType.getScale() == -1 ? null : columnDeclaration.dataType.getScale(),
                            columnDeclaration.dataType.getDimension() == -1 ? null : columnDeclaration.dataType.getDimension(),
                            columnDeclaration.dataType.getCardinality() == -1 ? null : columnDeclaration.dataType.getCardinality(),
                            columnDeclaration.dataType.getNullable(),
                            collation
                    );

                    for ( DataStore s : stores ) {
                        catalog.addColumnPlacement(
                                s.getAdapterId(),
                                addedColumnId,
                                store == null ? PlacementType.AUTOMATIC : PlacementType.MANUAL,
                                null,
                                null,
                                null,
                                null );
                    }

                    // Add default value
                    if ( ((SqlColumnDeclaration) c.e).expression != null ) {
                        // TODO: String is only a temporal solution for default values
                        String v = ((SqlColumnDeclaration) c.e).expression.toString();
                        if ( v.startsWith( "'" ) ) {
                            v = v.substring( 1, v.length() - 1 );
                        }
                        catalog.setDefaultValue( addedColumnId, PolyType.VARCHAR, v );
                    }
                } else if ( c.e instanceof SqlKeyConstraint ) {
                    SqlKeyConstraint constraint = (SqlKeyConstraint) c.e;
                    List<Long> columnIds = new LinkedList<>();
                    for ( SqlNode node : constraint.getColumnList().getList() ) {
                        String columnName = node.toString();
                        CatalogColumn catalogColumn = catalog.getColumn( tableId, columnName );
                        columnIds.add( catalogColumn.id );
                    }
                    if ( constraint.getOperator() == SqlKeyConstraint.PRIMARY ) {
                        catalog.addPrimaryKey( tableId, columnIds );
                    } else if ( constraint.getOperator() == SqlKeyConstraint.UNIQUE ) {
                        String constraintName;
                        if ( constraint.getName() == null ) {
                            constraintName = NameGenerator.generateConstraintName();
                        } else {
                            constraintName = constraint.getName().getSimple();
                        }
                        catalog.addUniqueConstraint( tableId, constraintName, columnIds );
                    }
                } else {
                    throw new AssertionError( c.e.getClass() );
                }
            }

            CatalogTable catalogTable = catalog.getTable( tableId );
            for ( DataStore store : stores ) {
                store.createTable( context, catalogTable );
            }

            if ( partitionType != null ) {
                // Check if specified partitionColumn is even part of the table
                Catalog.PartitionType actualPartitionType = Catalog.PartitionType.getByName( partitionType.toString() );
                long partitionColumnID = catalog.getColumn( tableId, partitionColumn.toString() ).id;
                if ( log.isDebugEnabled() ) {
                    log.debug( "Creating partitions for table: {} with id {} on schema: {} on column: {}", catalogTable.name, catalogTable.id, catalogTable.getSchemaName(), partitionColumnID );
                }

                List<List<String>> partitionQualifierStringList = new ArrayList<>();
                for ( List<SqlNode> partitionValueList : partitionQualifierList ) {
                    partitionQualifierStringList.add( partitionValueList.stream().map( Object::toString ).collect( Collectors.toList() ) );
                }

                catalog.partitionTable(
                        tableId,
                        actualPartitionType,
                        partitionColumnID,
                        numPartitions,
                        partitionQualifierStringList,
                        partitionNamesList.stream().map( Object::toString ).collect( Collectors.toList() ) );
                if ( log.isDebugEnabled() ) {
                    log.debug( "Table: '{}' has been partitioned on columnId '{}' ", catalogTable.name, catalogTable.columnIds.get( catalogTable.columnIds.indexOf( partitionColumnID ) ) );
                }
            }
        } catch ( GenericCatalogException | UnknownColumnException | UnknownCollationException | UnknownPartitionTypeException e ) {
            throw new RuntimeException( e );
        }
    }

}
