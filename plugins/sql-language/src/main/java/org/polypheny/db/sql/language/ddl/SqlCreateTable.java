/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.sql.language.ddl;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Ord;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.ddl.DdlManager.PartitionInformation;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.partition.raw.RawPartitionInformation;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlCreate;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlNodeList;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Pair;


/**
 * Parse tree for {@code CREATE TABLE} statement.
 */
@EqualsAndHashCode(callSuper = true)
@Slf4j
@Value
public class SqlCreateTable extends SqlCreate implements ExecutableStatement {

    SqlIdentifier name;
    SqlNodeList columns;

    @Nullable
    SqlNode query; // for "CREATE TABLE ... AS query"; may be null

    @Nullable
    SqlIdentifier store; // ON STORE [storeId name];

    @Nullable
    SqlIdentifier partitionColumn;

    @Nullable
    SqlIdentifier partitionType; // PARTITION BY (HASH | RANGE | LIST);

    int numPartitionGroups;
    int numPartitions;

    @Nullable
    List<SqlIdentifier> partitionGroupNamesList; // can only be used in association with PARTITION BY and PARTITIONS
    RawPartitionInformation rawPartitionInfo;

    List<List<SqlNode>> partitionQualifierList;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE TABLE", Kind.CREATE_TABLE );
    Snapshot snapshot = Catalog.getInstance().getSnapshot();


    /**
     * Creates a SqlCreateTable.
     */
    SqlCreateTable(
            ParserPos pos,
            boolean replace,
            boolean ifNotExists,
            SqlIdentifier name,
            @Nullable SqlNodeList columns,
            @Nullable SqlNode query,
            @Nullable SqlIdentifier store,
            @Nullable SqlIdentifier partitionType,
            @Nullable SqlIdentifier partitionColumn,
            int numPartitionGroups,
            int numPartitions,
            @Nullable List<SqlIdentifier> partitionGroupNamesList,
            List<List<SqlNode>> partitionQualifierList,
            RawPartitionInformation rawPartitionInfo ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
        this.columns = columns;
        this.query = query;
        this.store = store;
        this.partitionType = partitionType;
        this.partitionColumn = partitionColumn;
        this.numPartitionGroups = numPartitionGroups;
        this.numPartitions = numPartitions;
        this.partitionGroupNamesList = partitionGroupNamesList;
        this.partitionQualifierList = partitionQualifierList;
        this.rawPartitionInfo = rawPartitionInfo;
    }


    @Override
    public List<Node> getOperandList() {
        return ImmutableNullableList.of( name, columns, query );
    }


    @Override
    public List<SqlNode> getSqlOperandList() {
        return ImmutableNullableList.of( name, columns, query );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        writer.keyword( "TABLE" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
        if ( columns != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columns.getSqlList() ) {
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

            switch ( partitionType.getSimple() ) {
                case "HASH":
                    writer.keyword( "WITH" );
                    frame = writer.startList( "(", ")" );
                    for ( SqlIdentifier name : partitionGroupNamesList ) {
                        writer.sep( "," );
                        name.unparse( writer, 0, 0 );
                    }
                    break;
                case "RANGE":
                case "LIST":
                    writer.keyword( "(" );
                    for ( int i = 0; i < partitionGroupNamesList.size(); i++ ) {
                        writer.keyword( "PARTITION" );
                        partitionGroupNamesList.get( i ).unparse( writer, 0, 0 );
                        writer.keyword( "VALUES" );
                        writer.keyword( "(" );
                        partitionQualifierList.get( i ).get( 0 ).unparse( writer, 0, 0 );
                        writer.sep( "," );
                        partitionQualifierList.get( i ).get( 1 ).unparse( writer, 0, 0 );
                        writer.keyword( ")" );

                        if ( i + 1 < partitionGroupNamesList.size() ) {
                            writer.sep( "," );
                        }
                    }
                    writer.keyword( ")" );
                    break;
            }
            writer.endList( frame );
        }
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        if ( query != null ) {
            throw new GenericRuntimeException( "Not yet supported" );
        }

        String tableName;
        long namespaceId;

        // Cannot use getTable() here since table does not yet exist
        if ( name.names.size() == 2 ) { // NamespaceName.TableName
            namespaceId = snapshot.getNamespace( name.names.get( 0 ) ).orElseThrow().id;
            tableName = name.names.get( 1 );
        } else if ( name.names.size() == 1 ) { // TableName
            namespaceId = snapshot.getNamespace( context.getDefaultNamespaceName() ).orElseThrow().id;
            tableName = name.names.get( 0 );
        } else {
            throw new GenericRuntimeException( "Invalid table name: " + name );
        }

        List<DataStore<?>> stores = store != null ? ImmutableList.of( getDataStoreInstance( store ) ) : null;

        PlacementType placementType = store == null ? PlacementType.AUTOMATIC : PlacementType.MANUAL;

        List<FieldInformation> columns = null;
        List<ConstraintInformation> constraints = null;

        if ( this.columns != null ) {
            Pair<List<FieldInformation>, List<ConstraintInformation>> columnsConstraints = separateColumnList();
            columns = columnsConstraints.left;
            constraints = columnsConstraints.right;
        }

        try {
            DdlManager.getInstance().createTable(
                    namespaceId,
                    tableName,
                    columns,
                    constraints,
                    ifNotExists,
                    stores,
                    placementType,
                    statement );

            if ( partitionType != null ) {
                context.updateSnapshot();
                DdlManager.getInstance().createTablePartition(
                        PartitionInformation.fromNodeLists(
                                getTableFailOnEmpty( context, new SqlIdentifier( tableName, ParserPos.ZERO ) ),
                                partitionType.getSimple(),
                                partitionColumn.getSimple(),
                                partitionGroupNamesList.stream().map( n -> (Identifier) n ).collect( Collectors.toList() ),
                                numPartitionGroups,
                                numPartitions,
                                partitionQualifierList.stream().map( l -> l.stream().map( e -> (Node) e ).toList() ).toList(),
                                rawPartitionInfo ),
                        stores,
                        statement );
            }

        } catch ( TransactionException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    private Pair<List<FieldInformation>, List<ConstraintInformation>> separateColumnList() {
        List<FieldInformation> fieldInformation = new ArrayList<>();
        List<ConstraintInformation> constraintInformation = new ArrayList<>();

        int position = 1;
        for ( Ord<SqlNode> c : Ord.zip( columns.getSqlList() ) ) {
            if ( c.e instanceof SqlColumnDeclaration columnDeclaration ) {

                PolyValue defaultValue = columnDeclaration.getExpression() == null ? null : SqlLiteral.toPoly( columnDeclaration.getExpression() );

                fieldInformation.add(
                        new FieldInformation(
                                columnDeclaration.getName().getSimple(),
                                ColumnTypeInformation.fromDataTypeSpec( columnDeclaration.getDataType() ),
                                columnDeclaration.getCollation(),
                                defaultValue,
                                position ) );

            } else if ( c.e instanceof SqlKeyConstraint constraint ) {
                String constraintName = constraint.getName() != null ? constraint.getName().getSimple() : null;

                constraintInformation.add( getConstraintInformation( constraintName, constraint ) );
            } else {
                throw new AssertionError( c.e.getClass() );
            }
            position++;
        }

        return new Pair<>( fieldInformation, constraintInformation );

    }


    @NotNull
    private ConstraintInformation getConstraintInformation( String constraintName, SqlKeyConstraint constraint ) {
        return switch ( constraint.getConstraintType() ) {
            case PRIMARY, UNIQUE -> new ConstraintInformation(
                    constraintName,
                    constraint.getConstraintType(),
                    constraint.getFields().getSqlList().stream()
                            .map( SqlNode::toString )
                            .collect( Collectors.toList() ) );
            case FOREIGN -> new ConstraintInformation(
                    constraintName,
                    constraint.getConstraintType(),
                    constraint.getFields().getSqlList().stream()
                            .map( SqlNode::toString )
                            .collect( Collectors.toList() ),
                    ((SqlForeignKeyConstraint) constraint).getReferencedEntity().toString(),
                    ((SqlForeignKeyConstraint) constraint).getReferencedField().toString() );
        };
    }


}
