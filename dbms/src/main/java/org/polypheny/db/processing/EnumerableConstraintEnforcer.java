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
 */

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.ConditionalExecute.Condition;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.exceptions.ConstraintViolationException;
import org.polypheny.db.rel.logical.LogicalConditionalExecute;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.fun.SqlCountAggFunction;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;


@Slf4j
public class EnumerableConstraintEnforcer implements ConstraintEnforcer {

    @Override
    public RelRoot enforce( RelRoot logicalRoot, Statement statement ) {
        if ( !logicalRoot.kind.belongsTo( SqlKind.DML ) ) {
            return logicalRoot;
        }
        if ( !(logicalRoot.rel instanceof TableModify) ) {
            return logicalRoot;
        }
        final TableModify root = (TableModify) logicalRoot.rel;

        final Catalog catalog = Catalog.getInstance();
        final CatalogSchema schema = statement.getTransaction().getDefaultSchema();
        final CatalogTable table;
        final CatalogPrimaryKey primaryKey;
        final List<CatalogConstraint> constraints;
        final List<CatalogForeignKey> foreignKeys;
        final List<CatalogForeignKey> exportedKeys;
        try {
            String tableName;
            if ( root.getTable().getQualifiedName().size() == 1 ) { // tableName
                tableName = root.getTable().getQualifiedName().get( 0 );
            } else if ( root.getTable().getQualifiedName().size() == 2 ) { // schemaName.tableName
                if ( !schema.name.equalsIgnoreCase( root.getTable().getQualifiedName().get( 0 ) ) ) {
                    throw new RuntimeException( "Schema name does not match expected schema name: " + root.getTable().getQualifiedName().get( 0 ) );
                }
                tableName = root.getTable().getQualifiedName().get( 1 );
            } else {
                throw new RuntimeException( "Invalid table name: " + root.getTable().getQualifiedName() );
            }
            table = catalog.getTable( schema.id, tableName );
            primaryKey = catalog.getPrimaryKey( table.primaryKey );
            constraints = new ArrayList<>( Catalog.getInstance().getConstraints( table.id ) );
            foreignKeys = Catalog.getInstance().getForeignKeys( table.id );
            exportedKeys = Catalog.getInstance().getExportedKeys( table.id );
            // Turn primary key into an artificial unique constraint
            CatalogPrimaryKey pk = Catalog.getInstance().getPrimaryKey( table.primaryKey );
            final CatalogConstraint pkc = new CatalogConstraint( 0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
            constraints.add( pkc );
        } catch ( UnknownTableException e ) {
            log.error( "Caught exception", e );
            return logicalRoot;
        }

        RelNode lceRoot = root;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        if ( root.isInsert() && RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RelNode input = root.getInput().accept( new DeepCopyShuttle() );
            final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
            for ( final CatalogConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: {}", constraint.type );
                    continue;
                }
                // Enforce uniqueness between the already existing values and the new values
                final RelNode scan = LogicalTableScan.create( root.getCluster(), root.getTable() );
                RexNode joinCondition = rexBuilder.makeLiteral( true );
                //
                // TODO: Here we get issues with batch queries
                //
                builder.push( input );
                builder.project( constraint.key.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                builder.push( scan );
                builder.project( constraint.key.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                for ( final String column : constraint.key.getColumnNames() ) {
                    RexNode joinComparison = rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            builder.field( 2, 1, column ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( SqlStdOperatorTable.AND, joinCondition, joinComparison );
                }
                //
                // TODO MV: Changed JOIN Type from LEFT to INNER to fix issues row types in index based query simplification.
                //  Make sure this is ok!
                //
                final RelNode join = builder.join( JoinRelType.INNER, joinCondition ).build();
                final RelNode check = LogicalFilter.create( join, rexBuilder.makeCall( SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef( join, join.getRowType().getFieldCount() - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO,
                        ConstraintViolationException.class,
                        String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lce.setCheckDescription( String.format( "Enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lceRoot = lce;
                // Enforce uniqueness within the values to insert
                if ( input instanceof LogicalValues && ((LogicalValues) input).getTuples().size() <= 1 ) {
                    // no need to check, only one tuple in set
                } else if ( input instanceof LogicalProject && input.getInput( 0 ) instanceof LogicalValues && (input.getInput( 0 )).getRowType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                    //noinspection StatementWithEmptyBody
                    if ( statement.getDataContext().getParameterValues().size() > 0 ) {
                        LogicalProject project = (LogicalProject) input;
                        List<Map<Long, Object>> parameterValues = statement.getDataContext().getParameterValues();
                        final Set<List<Object>> uniqueSet = new HashSet<>( parameterValues.get( 0 ).size() );
                        final Map<String, Integer> columnMap = new HashMap<>( constraint.key.columnIds.size() );
                        for ( final String columnName : constraint.key.getColumnNames() ) {
                            int i = project.getRowType().getField( columnName, true, false ).getIndex();
                            columnMap.put( columnName, i );
                        }
                        for ( Integer index : columnMap.values() ) {
                            for ( Map<Long, Object> entry : parameterValues ) {
                                List<Object> list = new LinkedList<>();
                                if ( project.getProjects().get( index ) instanceof RexDynamicParam ) {
                                    list.add( entry.get( ((RexDynamicParam) project.getProjects().get( index )).getIndex() ) );
                                } else {
                                    throw new RuntimeException( "Unexpected node type" );
                                }
                                uniqueSet.add( list );
                            }
                        }
                        if ( uniqueSet.size() != parameterValues.size() ) {
                            throw new ConstraintViolationException( String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                        }
                    } else {
                        // no need to check, only one tuple in set
                    }
                } else if ( input instanceof Values ) {
                    // If the input is a Values node, check uniqueness right away, as not all stores can implement this check
                    // (And anyway, pushing this down to stores seems rather inefficient)
                    final Values values = (Values) input;
                    final List<? extends List<RexLiteral>> tuples = values.getTuples();
                    final Set<List<RexLiteral>> uniqueSet = new HashSet<>( tuples.size() );
                    final Map<String, Integer> columnMap = new HashMap<>( constraint.key.columnIds.size() );
                    for ( final String columnName : constraint.key.getColumnNames() ) {
                        int i = values.getRowType().getField( columnName, true, false ).getIndex();
                        columnMap.put( columnName, i );
                    }
                    for ( final List<RexLiteral> tuple : tuples ) {
                        List<RexLiteral> projection = new ArrayList<>( constraint.key.columnIds.size() );
                        for ( final String columnName : constraint.key.getColumnNames() ) {
                            projection.add( tuple.get( columnMap.get( columnName ) ) );
                        }
                        uniqueSet.add( projection );
                    }
                    if ( uniqueSet.size() != tuples.size() ) {
                        throw new ConstraintViolationException( String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                    }
                } else {
                    builder.clear();
                    builder.push( input );
                    builder.aggregate( builder.groupKey( constraint.key.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) ), builder.aggregateCall( new SqlCountAggFunction( "count" ) ).as( "count" ) );
                    builder.filter( builder.call( SqlStdOperatorTable.GREATER_THAN, builder.field( "count" ), builder.literal( 1 ) ) );
                    final RelNode innerCheck = builder.build();
                    final LogicalConditionalExecute ilce = LogicalConditionalExecute.create( innerCheck, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                            String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                    ilce.setCheckDescription( String.format( "Source-internal enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                    lceRoot = ilce;
                }
            }
        }

        //
        //  Enforce FOREIGN KEY constraints in INSERT operations
        //
        if ( root.isInsert() && RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RelNode input = root.getInput().accept( new DeepCopyShuttle() );
            final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
            for ( final CatalogForeignKey foreignKey : foreignKeys ) {
                final RelOptSchema relOptSchema = root.getCatalogReader();
                final RelOptTable relOptTable = relOptSchema.getTableForMember( Collections.singletonList( foreignKey.getReferencedKeyTableName() ) );
                final LogicalTableScan scan = LogicalTableScan.create( root.getCluster(), relOptTable );
                RexNode joinCondition = rexBuilder.makeLiteral( true );
                builder.push( input );
                builder.project( foreignKey.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                builder.push( scan );
                builder.project( foreignKey.getReferencedKeyColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                for ( int i = 0; i < foreignKey.getColumnNames().size(); ++i ) {
                    final String column = foreignKey.getColumnNames().get( i );
                    final String referencedColumn = foreignKey.getReferencedKeyColumnNames().get( i );
                    RexNode joinComparison = rexBuilder.makeCall(
                            SqlStdOperatorTable.EQUALS,
                            builder.field( 2, 1, referencedColumn ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( SqlStdOperatorTable.AND, joinCondition, joinComparison );
                }

                final RelNode join = builder.join( JoinRelType.LEFT, joinCondition ).build();
                final RelNode check = LogicalFilter.create( join, rexBuilder.makeCall( SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef( join, join.getRowType().getFieldCount() - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Insert violates foreign key constraint `%s`.`%s`", table.name, foreignKey.name ) );
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", table.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        //
        //  Enforce UNIQUE constraints in UPDATE operations
        //
        if ( (root.isUpdate() || root.isMerge()) && RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            RelBuilder builder = RelBuilder.create( statement );
            RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final CatalogConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: {}", constraint.type );
                    continue;
                }
                // Check if update affects this constraint
                boolean affected = false;
                for ( final String c : root.getUpdateColumnList() ) {
                    if ( constraint.key.getColumnNames().contains( c ) ) {
                        affected = true;
                        break;
                    }
                }
                if ( !affected ) {
                    continue;
                }
                RelNode input = root.getInput().accept( new DeepCopyShuttle() );
                Map<String, Integer> nameMap = new HashMap<>();
                for ( int i = 0; i < root.getUpdateColumnList().size(); ++i ) {
                    nameMap.put( root.getUpdateColumnList().get( i ), i );
                }
                // Enforce uniqueness between updated records and already present records
                builder.clear();
                builder.push( input );
                List<RexNode> projects = new ArrayList<>();
                List<String> names = new ArrayList<>();
                for ( final String column : primaryKey.getColumnNames() ) {
                    projects.add( builder.field( column ) );
                    names.add( column );
                }
                for ( final String column : constraint.key.getColumnNames() ) {
                    if ( root.getUpdateColumnList().contains( column ) ) {
                        projects.add( root.getSourceExpressionList().get( nameMap.get( column ) ) );
                    } else {
                        // TODO(s3lph): For now, let's assume that all columns are actually present.
                        //  Otherwise this would require either some black magic project rewrites or joining against another table scan
                        projects.add( builder.field( column ) );
                    }
                    names.add( "$projected$." + column );
                }
                builder.project( projects );
                builder.scan( table.name );
                builder.join( JoinRelType.INNER, builder.literal( true ) );

                List<RexNode> conditionList1 = primaryKey.getColumnNames().stream().map( c ->
                        builder.call( SqlStdOperatorTable.EQUALS,
                                builder.field( names.indexOf( c ) ),
                                builder.field( names.size() + table.getColumnNames().indexOf( c ) )
                        )
                ).collect( Collectors.toList() );

                List<RexNode> conditionList2 = constraint.key.getColumnNames().stream().map( c ->
                        builder.call( SqlStdOperatorTable.EQUALS,
                                builder.field( names.indexOf( "$projected$." + c ) ),
                                builder.field( names.size() + table.getColumnNames().indexOf( c ) )
                        )
                ).collect( Collectors.toList() );

                RexNode condition =
                        rexBuilder.makeCall( SqlStdOperatorTable.AND,
                                rexBuilder.makeCall( SqlStdOperatorTable.NOT,
                                        conditionList1.size() > 1 ?
                                                rexBuilder.makeCall( SqlStdOperatorTable.AND, conditionList1 ) :
                                                conditionList1.get( 0 )
                                ),
                                conditionList2.size() > 1 ?
                                        rexBuilder.makeCall( SqlStdOperatorTable.AND, conditionList2 ) :
                                        conditionList2.get( 0 )
                        );
                condition = RexUtil.flatten( rexBuilder, condition );
                RelNode check = builder.build();
                check = new LogicalFilter( check.getCluster(), check.getTraitSet(), check, condition, ImmutableSet.of() );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Update violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lce.setCheckDescription( String.format( "Enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lceRoot = lce;
                // Enforce uniqueness within the values to insert
                builder.clear();
                builder.push( input );
                projects = new ArrayList<>();
                for ( final String column : constraint.key.getColumnNames() ) {
                    if ( root.getUpdateColumnList().contains( column ) ) {
                        projects.add( root.getSourceExpressionList().get( nameMap.get( column ) ) );
                    } else {
                        // TODO(s3lph): For now, let's assume that all columns are actually present.
                        //  Otherwise this would require either some black magic project rewrites or joining against another table scan
                        projects.add( builder.field( column ) );
                    }
                }
                builder.project( projects );
                builder.aggregate(
                        builder.groupKey( IntStream.range( 0, projects.size() ).mapToObj( builder::field ).collect( Collectors.toList() ) ),
                        builder.aggregateCall( new SqlCountAggFunction( "count" ) ).as( "count" )
                );
                builder.filter( builder.call( SqlStdOperatorTable.GREATER_THAN, builder.field( "count" ), builder.literal( 1 ) ) );
                final RelNode innerCheck = builder.build();
                final LogicalConditionalExecute ilce = LogicalConditionalExecute.create( innerCheck, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Update violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                ilce.setCheckDescription( String.format( "Source-internal enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lceRoot = ilce;
            }
        }

        //
        //  Enforce FOREIGN KEY constraints in UPDATE operations
        //
        if ( (root.isUpdate() || root.isMerge()) && RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final CatalogForeignKey foreignKey : foreignKeys ) {
                final String constraintRule = "ON UPDATE " + foreignKey.updateRule;
                RelNode input = root.getInput().accept( new DeepCopyShuttle() );
                final List<RexNode> projects = new ArrayList<>( foreignKey.columnIds.size() );
                final List<RexNode> foreignProjects = new ArrayList<>( foreignKey.columnIds.size() );
                final CatalogTable foreignTable = Catalog.getInstance().getTable( foreignKey.referencedKeyTableId );
                builder.push( input );
                for ( int i = 0; i < foreignKey.columnIds.size(); ++i ) {
                    final String columnName = foreignKey.getColumnNames().get( i );
                    final String foreignColumnName = foreignKey.getReferencedKeyColumnNames().get( i );
                    final CatalogColumn foreignColumn;
                    try {
                        foreignColumn = Catalog.getInstance().getColumn( foreignTable.id, foreignColumnName );
                    } catch ( UnknownColumnException e ) {
                        throw new RuntimeException( e );
                    }
                    RexNode newValue;
                    int targetIndex;
                    if ( root.isUpdate() ) {
                        targetIndex = root.getUpdateColumnList().indexOf( columnName );
                        newValue = root.getSourceExpressionList().get( targetIndex );
                        newValue = new RexShuttle() {
                            @Override
                            public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
                                return rexBuilder.makeInputRef( input, input.getRowType().getField( fieldAccess.getField().getName(), true, false ).getIndex() );
                            }
                        }.apply( newValue );
                    } else {
                        targetIndex = input.getRowType().getField( columnName, true, false ).getIndex();
                        newValue = rexBuilder.makeInputRef( input, targetIndex );
                    }
                    RexNode foreignValue = rexBuilder.makeInputRef( foreignColumn.getRelDataType( rexBuilder.getTypeFactory() ), targetIndex );
                    projects.add( newValue );
                    foreignProjects.add( foreignValue );
                }
                builder
                        .project( projects )
                        .scan( foreignKey.getReferencedKeyTableName() )
                        .project( foreignProjects );
                RexNode condition = rexBuilder.makeLiteral( true );
                for ( int i = 0; i < projects.size(); ++i ) {
                    condition = builder.and(
                            condition,
                            builder.equals(
                                    builder.field( 2, 0, i ),
                                    builder.field( 2, 1, i )
                            )
                    );
                }
                final RelNode join = builder.join( JoinRelType.LEFT, condition ).build();
                final RelNode check = LogicalFilter.create( join, rexBuilder.makeCall( SqlStdOperatorTable.IS_NULL, rexBuilder.makeInputRef( join, projects.size() * 2 - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Update violates foreign key constraint `%s` (`%s` %s -> `%s` %s, %s)",
                                foreignKey.name, table.name, foreignKey.getColumnNames(), foreignTable.name, foreignKey.getReferencedKeyColumnNames(), constraintRule ) );
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", table.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        //
        //  Enforce reverse FOREIGN KEY constraints in UPDATE and DELETE operations
        //
        if ( (root.isDelete() || root.isUpdate() || root.isMerge()) && RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            RelBuilder builder = RelBuilder.create( statement );
            final RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final CatalogForeignKey foreignKey : exportedKeys ) {
                final String constraintRule = root.isDelete() ? "ON DELETE " + foreignKey.deleteRule : "ON UPDATE " + foreignKey.updateRule;
                switch ( root.isDelete() ? foreignKey.deleteRule : foreignKey.updateRule ) {
                    case RESTRICT:
                        break;
                    default:
                        throw new NotImplementedException( String.format( "The foreign key option %s is not yet implemented.", constraintRule ) );
                }
                RelNode pInput;
                if ( root.getInput() instanceof Project ) {
                    pInput = ((LogicalProject) root.getInput()).getInput().accept( new DeepCopyShuttle() );
                } else {
                    pInput = root.getInput().accept( new DeepCopyShuttle() );
                }
                final List<RexNode> projects = new ArrayList<>( foreignKey.columnIds.size() );
                final List<RexNode> foreignProjects = new ArrayList<>( foreignKey.columnIds.size() );
                final CatalogTable foreignTable = Catalog.getInstance().getTable( foreignKey.tableId );
                for ( int i = 0; i < foreignKey.columnIds.size(); ++i ) {
                    final String columnName = foreignKey.getReferencedKeyColumnNames().get( i );
                    final String foreignColumnName = foreignKey.getColumnNames().get( i );
                    final CatalogColumn column, foreignColumn;
                    try {
                        column = Catalog.getInstance().getColumn( table.id, columnName );
                        foreignColumn = Catalog.getInstance().getColumn( foreignTable.id, foreignColumnName );
                    } catch ( UnknownColumnException e ) {
                        throw new RuntimeException( e );
                    }
                    final RexNode inputRef = new RexInputRef( column.position - 1, rexBuilder.getTypeFactory().createPolyType( column.type ) );
                    final RexNode foreignInputRef = new RexInputRef( foreignColumn.position - 1, rexBuilder.getTypeFactory().createPolyType( foreignColumn.type ) );
                    projects.add( inputRef );
                    foreignProjects.add( foreignInputRef );
                }
                builder
                        .push( pInput )
                        .project( projects )
                        .scan( foreignKey.getTableName() )
                        .project( foreignProjects );
                RexNode condition = rexBuilder.makeLiteral( true );
                for ( int i = 0; i < projects.size(); ++i ) {
                    condition = builder.and(
                            condition,
                            builder.equals(
                                    builder.field( 2, 0, i ),
                                    builder.field( 2, 1, i )
                            )
                    );
                }
                final RelNode join = builder.join( JoinRelType.INNER, condition ).build();
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( join, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "%s violates foreign key constraint `%s` (`%s` %s -> `%s` %s, %s)",
                                root.isUpdate() ? "Update" : "Delete",
                                foreignKey.name, foreignTable.name, foreignKey.getColumnNames(), table.name, foreignKey.getReferencedKeyColumnNames(), constraintRule ) );
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", foreignTable.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        RelRoot enforcementRoot = new RelRoot( lceRoot, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
        // Send the generated tree with all unoptimized constraint enforcement checks to the UI
        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Constraint Enforcement Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Constraint Enforcement Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Constraint Enforcement Plan", enforcementRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
        return enforcementRoot;
    }

}
