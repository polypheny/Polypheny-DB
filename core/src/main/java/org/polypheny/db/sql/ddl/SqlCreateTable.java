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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnInformation;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.PartitionInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.partition.raw.RawPartitionInformation;
import org.polypheny.db.partition.raw.RawTemperaturePartitionInformation;
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
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Pair;


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
    private final int numPartitionGroups;
    private final int numPartitions;
    private final List<SqlIdentifier> partitionNamesList;
    private final RawPartitionInformation rawPartitionInfo;

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
            int numPartitionGroups,
            int numPartitions,
            List<SqlIdentifier> partitionNamesList,
            List<List<SqlNode>> partitionQualifierList,
            RawPartitionInformation rawPartitionInfo) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // May be null
        this.query = query; // for "CREATE TABLE ... AS query"; may be null
        this.store = store; // ON STORE [store name]; may be null
        this.partitionType = partitionType; // PARTITION BY (HASH | RANGE | LIST); may be null
        this.partitionColumn = partitionColumn; // May be null
        this.numPartitionGroups = numPartitionGroups; // May be null and can only be used in association with PARTITION BY
        this.numPartitions = numPartitions;
        this.partitionNamesList = partitionNamesList; // May be null and can only be used in association with PARTITION BY and PARTITIONS
        this.partitionQualifierList = partitionQualifierList;
        this.rawPartitionInfo = rawPartitionInfo;
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
            // cannot use getTable here, as table does not yet exist
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

        List<DataStore> stores = store != null ? ImmutableList.of( getDataStoreInstance( store ) ) : null;

        PlacementType placementType = store == null ? PlacementType.AUTOMATIC : PlacementType.MANUAL;

        List<ColumnInformation> columns = null;
        List<ConstraintInformation> constraints = null;

        if ( columnList != null ) {
            Pair<List<ColumnInformation>, List<ConstraintInformation>> columnsConstraints = separateColumnList();
            columns = columnsConstraints.left;
            constraints = columnsConstraints.right;
        }

        try {

            DdlManager.getInstance().createTable(
                    schemaId,
                    tableName,
                    columns,
                    constraints,
                    ifNotExists,
                    stores,
                    placementType,
                    statement );





            if ( partitionType != null ) {
                DdlManager.getInstance().addPartitioning( PartitionInformation.fromSqlLists(
                        getCatalogTable( context, new SqlIdentifier( tableName, SqlParserPos.ZERO ) ),
                        partitionType.getSimple(),
                        partitionColumn.getSimple(),
                        partitionNamesList,
                        numPartitionGroups,
                        numPartitions,
                        partitionQualifierList,
                        rawPartitionInfo),
                        stores,
                        statement);
            }

        } catch ( TableAlreadyExistsException e ) {
            throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.tableExists( tableName ) );
        } catch ( ColumnNotExistsException e ) {
            throw SqlUtil.newContextException( partitionColumn.getParserPosition(), RESOURCE.columnNotFoundInTable( e.columnName, e.tableName ) );
        } catch ( UnknownPartitionTypeException e ) {
            throw SqlUtil.newContextException( partitionType.getParserPosition(), RESOURCE.unknownPartitionType( partitionType.getSimple() ) );
        } catch ( PartitionGroupNamesNotUniqueException e ) {
            throw SqlUtil.newContextException( partitionColumn.getParserPosition(), RESOURCE.partitionNamesNotUnique() );
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            // we just added the table/column so it has to exist or we have a internal problem
            throw new RuntimeException( e );
        }
    }


    private Pair<List<ColumnInformation>, List<ConstraintInformation>> separateColumnList() {
        List<ColumnInformation> columnInformations = new ArrayList<>();
        List<ConstraintInformation> constraintInformations = new ArrayList<>();

        int position = 1;
        for ( Ord<SqlNode> c : Ord.zip( columnList ) ) {
            if ( c.e instanceof SqlColumnDeclaration ) {
                final SqlColumnDeclaration columnDeclaration = (SqlColumnDeclaration) c.e;

                String defaultValue = columnDeclaration.getExpression() == null ? null : columnDeclaration.getExpression().toString();

                columnInformations.add( new ColumnInformation( columnDeclaration.getName().getSimple(), ColumnTypeInformation.fromSqlDataTypeSpec( columnDeclaration.getDataType() ), columnDeclaration.getCollation(), defaultValue, position ) );

            } else if ( c.e instanceof SqlKeyConstraint ) {
                SqlKeyConstraint constraint = (SqlKeyConstraint) c.e;
                String constraintName = constraint.getName() != null ? constraint.getName().getSimple() : null;

                constraintInformations.add( new ConstraintInformation( constraintName, constraint.getConstraintType(), constraint.getColumnList().getList().stream().map( SqlNode::toString ).collect( Collectors.toList() ) ) );
            } else {
                throw new AssertionError( c.e.getClass() );
            }
            position++;
        }

        return new Pair<>( columnInformations, constraintInformations );

    }


}

