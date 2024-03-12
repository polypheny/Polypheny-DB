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

package org.polypheny.db.processing;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.common.ConditionalExecute.Condition;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer.EnforcementInformation;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer.ModifyExtractor;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalKey.EnforcementTime;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.config.Config;
import org.polypheny.db.config.Config.ConfigListener;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class ConstraintEnforceAttacher {


    /**
     * Inserts a dedicated {@link LogicalConstraintEnforcer} node at the top of the provided tree.
     * This node is later used during execution to execute queries, which check if no constraints where violated.
     *
     * <code><pre>
     *
     *                                   ConstraintEnforcer
     *     TableModify                          |
     *          |              ->           TableModify
     *       Values                             |
     *                                       Values
     * </code></pre>
     *
     * @param root the un-constraint algebraic tree
     * @param statement the used statement
     * @return the tree with an inserted {@link LogicalConstraintEnforcer}
     */
    public static AlgRoot attachOnQueryConstraints( AlgRoot root, Statement statement ) {
        return AlgRoot.of(
                LogicalConstraintEnforcer.create( root.alg, statement ),
                root.kind );
    }


    /**
     * This method marks the entity in the transaction as {@code ON_COMMIT}
     *
     * @param node the full algebraic tree
     * @param statement the used statement
     */
    public static void attachOnCommitConstraints( AlgNode node, Statement statement ) {
        ModifyExtractor extractor = new ModifyExtractor();
        node.accept( extractor );
        RelModify<?> modify = extractor.getModify();

        if ( modify == null ) {
            throw new GenericRuntimeException( "The tree did no conform, while generating the constraint enforcement query!" );
        }

        statement.getTransaction().getLogicalTables().add( modify.entity.unwrap( LogicalTable.class ).orElseThrow() );
    }


    public static List<EnforcementInformation> getConstraintAlg( Set<LogicalTable> tables, Statement statement, EnforcementTime enforcementTime ) {
        return tables
                .stream()
                .map( t -> LogicalConstraintEnforcer.getControl( t, statement, enforcementTime ) )
                .filter( i -> i.control() != null )
                .toList();
    }


    /**
     * Depending on the used enforcement strategy this method potentially attaches different
     * algebraic nodes to the {@link AlgRoot}.
     *
     * @param constraintsRoot the initial un-constraint {@link AlgRoot}
     * @param statement the used statement
     * @return the constraint tree
     */
    public static AlgRoot handleConstraints( AlgRoot constraintsRoot, Statement statement ) {
        Enum<?> strategy = RuntimeConfig.CONSTRAINT_ENFORCEMENT_STRATEGY.getEnum();
        if ( strategy == ConstraintStrategy.AFTER_QUERY_EXECUTION ) {
            attachOnCommitConstraints( constraintsRoot.alg, statement );
            return attachOnQueryConstraints( constraintsRoot, statement );
        } else if ( strategy == ConstraintStrategy.BEFORE_QUERY_EXECUTION ) {
            return enforceConstraintBeforeQuery( constraintsRoot, statement );
        } else {
            throw new GenericRuntimeException( "Constraint enforcement strategy is unknown." );
        }

    }


    /**
     * This method inserts a {@link LogicalConditionalExecute} on top of the initial DML query.
     * Additionally, it tries to build a DQL query as a left child of the {@link LogicalConditionalExecute},
     * which emulates the result of the DML query on the right and checks if all constraints are valid.
     *
     * <code><pre>
     *
     *                                   ConditionalExecute
     *     TableModify                    /             \
     *          |              ->     Filter           TableModify
     *       Values                     |                  |
     *                                Scan               Values
     * </code></pre>
     *
     * <br>
     * <i>Attention: This works great in theory, but gets complexer with increasing DML complexity and tends to fail, use with care.</i>
     *
     * @param logicalRoot the un-constraint algebraic tree
     * @param statement the used statement
     * @return the constraint algebraic tree
     */
    public static AlgRoot enforceConstraintBeforeQuery( AlgRoot logicalRoot, Statement statement ) {
        if ( !logicalRoot.kind.belongsTo( Kind.DML ) ) {
            return logicalRoot;
        }
        if ( !(logicalRoot.alg instanceof RelModify<?> root) ) {
            return logicalRoot;
        }

        final LogicalTable table = root.getEntity().unwrap( LogicalTable.class ).orElseThrow();
        LogicalRelSnapshot snapshot = statement.getTransaction().getSnapshot().rel();
        final LogicalPrimaryKey primaryKey = snapshot.getPrimaryKey( table.primaryKey ).orElseThrow();
        final List<LogicalConstraint> constraints = new ArrayList<>( snapshot.getConstraints( table.id ) );
        final List<LogicalForeignKey> foreignKeys = snapshot.getForeignKeys( table.id );
        final List<LogicalForeignKey> exportedKeys = snapshot.getExportedKeys( table.id );
        // Turn primary key into an artificial unique constraint
        LogicalPrimaryKey pk = snapshot.getPrimaryKey( table.primaryKey ).orElseThrow();
        final LogicalConstraint pkc = new LogicalConstraint( 0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
        constraints.add( pkc );

        AlgNode lceRoot = root;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        if ( root.isInsert() && RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            AlgBuilder builder = AlgBuilder.create( statement );
            final AlgNode input = root.getInput().accept( new DeepCopyShuttle() );
            final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
            for ( final LogicalConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: {}", constraint.type );
                    continue;
                }
                // Enforce uniqueness between the already existing values and the new values
                final AlgNode scan = LogicalRelScan.create( root.getCluster(), root.getEntity() );
                RexNode joinCondition = rexBuilder.makeLiteral( true );
                //
                // TODO: Here we get issues with batch queries
                //
                builder.push( input );
                builder.project( constraint.key.getFieldNames().stream().map( builder::field ).toList() );
                builder.push( scan );
                builder.project( constraint.key.getFieldNames().stream().map( builder::field ).toList() );
                for ( final String column : constraint.key.getFieldNames() ) {
                    RexNode joinComparison = rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            builder.field( 2, 1, column ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), joinCondition, joinComparison );
                }
                //
                // TODO MV: Changed JOIN Type from LEFT to INNER to fix issues row types in index based query simplification.
                //  Make sure this is ok!
                //
                final AlgNode join = builder.join( JoinAlgType.INNER, joinCondition ).build();
                final AlgNode check = LogicalRelFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), rexBuilder.makeInputRef( join, join.getTupleType().getFieldCount() - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO,
                        ConstraintViolationException.class,
                        String.format( "Insert violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lce.setCheckDescription( String.format( "Enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lceRoot = lce;
                // Enforce uniqueness within the values to insert
                if ( input instanceof LogicalRelValues && ((LogicalRelValues) input).getTuples().size() <= 1 ) {
                    // no need to check, only one tuple in set
                } else if ( input instanceof LogicalRelProject && input.getInput( 0 ) instanceof LogicalRelValues && (input.getInput( 0 )).getTupleType().toString().equals( "RecordType(INTEGER ZERO)" ) ) {
                    //noinspection StatementWithEmptyBody
                    if ( !statement.getDataContext().getParameterValues().isEmpty() ) {
                        LogicalRelProject project = (LogicalRelProject) input;
                        List<Map<Long, PolyValue>> parameterValues = statement.getDataContext().getParameterValues();
                        final Set<List<PolyValue>> uniqueSet = new HashSet<>( parameterValues.get( 0 ).size() );
                        final Map<String, Integer> columnMap = new HashMap<>( constraint.key.fieldIds.size() );
                        for ( final String columnName : constraint.key.getFieldNames() ) {
                            int i = project.getTupleType().getField( columnName, true, false ).getIndex();
                            columnMap.put( columnName, i );
                        }
                        for ( Integer index : columnMap.values() ) {
                            for ( Map<Long, PolyValue> entry : parameterValues ) {
                                List<PolyValue> list = new LinkedList<>();
                                if ( project.getProjects().get( index ) instanceof RexDynamicParam ) {
                                    list.add( entry.get( ((RexDynamicParam) project.getProjects().get( index )).getIndex() ) );
                                } else {
                                    throw new GenericRuntimeException( "Unexpected node type" );
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
                } else if ( input instanceof Values values ) {
                    // If the input is a Values node, check uniqueness right away, as not all stores can implement this check
                    // (And anyway, pushing this down to stores seems rather inefficient)
                    final List<? extends List<RexLiteral>> tuples = values.getTuples();
                    final Set<List<RexLiteral>> uniqueSet = new HashSet<>( tuples.size() );
                    final Map<String, Integer> columnMap = new HashMap<>( constraint.key.fieldIds.size() );
                    for ( final String columnName : constraint.key.getFieldNames() ) {
                        int i = values.getTupleType().getField( columnName, true, false ).getIndex();
                        columnMap.put( columnName, i );
                    }
                    for ( final List<RexLiteral> tuple : tuples ) {
                        List<RexLiteral> projection = new ArrayList<>( constraint.key.fieldIds.size() );
                        for ( final String columnName : constraint.key.getFieldNames() ) {
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
                    builder.aggregate( builder.groupKey( constraint.key.getFieldNames().stream().map( builder::field ).toList() ), builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.COUNT ) ).as( "count" ) );
                    builder.filter( builder.call( OperatorRegistry.get( OperatorName.GREATER_THAN ), builder.field( "count" ), builder.literal( 1 ) ) );
                    final AlgNode innerCheck = builder.build();
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
            AlgBuilder builder = AlgBuilder.create( statement );
            final AlgNode input = root.getInput().accept( new DeepCopyShuttle() );
            final RexBuilder rexBuilder = root.getCluster().getRexBuilder();
            for ( final LogicalForeignKey foreignKey : foreignKeys ) {

                final LogicalTable entity = statement.getDataContext().getSnapshot().rel().getTable( foreignKey.referencedKeyEntityId ).orElseThrow();
                final LogicalRelScan scan = LogicalRelScan.create( root.getCluster(), entity );
                RexNode joinCondition = rexBuilder.makeLiteral( true );
                builder.push( input );
                builder.project( foreignKey.getFieldNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                builder.push( scan );
                builder.project( foreignKey.getReferencedKeyFieldNames().stream().map( builder::field ).collect( Collectors.toList() ) );
                for ( int i = 0; i < foreignKey.getFieldNames().size(); ++i ) {
                    final String column = foreignKey.getFieldNames().get( i );
                    final String referencedColumn = foreignKey.getReferencedKeyFieldNames().get( i );
                    RexNode joinComparison = rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            builder.field( 2, 1, referencedColumn ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), joinCondition, joinComparison );
                }

                final AlgNode join = builder.join( JoinAlgType.LEFT, joinCondition ).build();
                final AlgNode check = LogicalRelFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( join, join.getTupleType().getFieldCount() - 1 ) ) );
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
            AlgBuilder builder = AlgBuilder.create( statement );
            RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final LogicalConstraint constraint : constraints ) {
                if ( constraint.type != ConstraintType.UNIQUE ) {
                    log.warn( "Unknown constraint type: {}", constraint.type );
                    continue;
                }
                // Check if update affects this constraint
                boolean affected = false;
                for ( final String c : root.getUpdateColumns() ) {
                    if ( constraint.key.getFieldNames().contains( c ) ) {
                        affected = true;
                        break;
                    }
                }
                if ( !affected ) {
                    continue;
                }
                AlgNode input = root.getInput().accept( new DeepCopyShuttle() );
                Map<String, Integer> nameMap = new HashMap<>();
                for ( int i = 0; i < root.getUpdateColumns().size(); ++i ) {
                    nameMap.put( root.getUpdateColumns().get( i ), i );
                }
                // Enforce uniqueness between updated records and already present records
                builder.clear();
                builder.push( input );
                List<RexNode> projects = new ArrayList<>();
                List<String> names = new ArrayList<>();
                for ( final String column : primaryKey.getFieldNames() ) {
                    projects.add( builder.field( column ) );
                    names.add( column );
                }
                for ( final String column : constraint.key.getFieldNames() ) {
                    if ( root.getUpdateColumns().contains( column ) ) {
                        projects.add( root.getSourceExpressions().get( nameMap.get( column ) ) );
                    } else {
                        // TODO(s3lph): For now, let's assume that all columns are actually present.
                        //  Otherwise this would require either some black magic project rewrites or joining against another table relScan
                        projects.add( builder.field( column ) );
                    }
                    names.add( "$projected$." + column );
                }
                builder.project( projects );
                builder.relScan( table.name );
                builder.join( JoinAlgType.INNER, builder.literal( true ) );

                List<LogicalColumn> columns = snapshot.getColumns( table.id );
                List<String> columNames = columns.stream().map( c -> c.name ).toList();

                List<RexNode> conditionList1 = primaryKey.getFieldNames().stream().map( c ->
                        builder.call(
                                OperatorRegistry.get( OperatorName.EQUALS ),
                                builder.field( names.indexOf( c ) ),
                                builder.field( names.size() + columNames.indexOf( c ) )
                        ) ).collect( Collectors.toList() );

                List<RexNode> conditionList2 = constraint.key.getFieldNames().stream().map( c ->
                        builder.call(
                                OperatorRegistry.get( OperatorName.EQUALS ),
                                builder.field( names.indexOf( "$projected$." + c ) ),
                                builder.field( names.size() + columNames.indexOf( c ) )
                        ) ).collect( Collectors.toList() );

                RexNode condition =
                        rexBuilder.makeCall(
                                OperatorRegistry.get( OperatorName.AND ),
                                rexBuilder.makeCall(
                                        OperatorRegistry.get( OperatorName.NOT ),
                                        conditionList1.size() > 1 ?
                                                rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), conditionList1 ) :
                                                conditionList1.get( 0 )
                                ),
                                conditionList2.size() > 1 ?
                                        rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), conditionList2 ) :
                                        conditionList2.get( 0 )
                        );
                condition = RexUtil.flatten( rexBuilder, condition );
                AlgNode check = builder.build();
                check = new LogicalRelFilter( check.getCluster(), check.getTraitSet(), check, condition, ImmutableSet.of() );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Update violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lce.setCheckDescription( String.format( "Enforcement of unique constraint `%s`.`%s`", table.name, constraint.name ) );
                lceRoot = lce;
                // Enforce uniqueness within the values to insert
                builder.clear();
                builder.push( input );
                projects = new ArrayList<>();
                for ( final String column : constraint.key.getFieldNames() ) {
                    if ( root.getUpdateColumns().contains( column ) ) {
                        projects.add( root.getSourceExpressions().get( nameMap.get( column ) ) );
                    } else {
                        // TODO(s3lph): For now, let's assume that all columns are actually present.
                        //  Otherwise this would require either some black magic project rewrites or joining against another table relScan
                        projects.add( builder.field( column ) );
                    }
                }
                builder.project( projects );
                builder.aggregate(
                        builder.groupKey( IntStream.range( 0, projects.size() ).mapToObj( builder::field ).collect( Collectors.toList() ) ),
                        builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.COUNT ) ).as( "count" )
                );
                builder.filter( builder.call( OperatorRegistry.get( OperatorName.GREATER_THAN ), builder.field( "count" ), builder.literal( 1 ) ) );
                final AlgNode innerCheck = builder.build();
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
            AlgBuilder builder = AlgBuilder.create( statement );
            final RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final LogicalForeignKey foreignKey : foreignKeys ) {
                final String constraintRule = "ON UPDATE " + foreignKey.updateRule;
                AlgNode input = root.getInput().accept( new DeepCopyShuttle() );
                final List<RexNode> projects = new ArrayList<>( foreignKey.fieldIds.size() );
                final List<RexNode> foreignProjects = new ArrayList<>( foreignKey.fieldIds.size() );
                final LogicalTable foreignTable = snapshot.getTable( foreignKey.referencedKeyEntityId ).orElseThrow();
                builder.push( input );
                for ( int i = 0; i < foreignKey.fieldIds.size(); ++i ) {
                    final String columnName = foreignKey.getFieldNames().get( i );
                    final String foreignColumnName = foreignKey.getReferencedKeyFieldNames().get( i );
                    final LogicalColumn foreignColumn = snapshot.getColumn( foreignTable.id, foreignColumnName ).orElseThrow();
                    RexNode newValue;
                    int targetIndex;
                    if ( root.isUpdate() ) {
                        targetIndex = root.getUpdateColumns().indexOf( columnName );
                        newValue = root.getSourceExpressions().get( targetIndex );
                        newValue = new RexShuttle() {
                            @Override
                            public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
                                return rexBuilder.makeInputRef( input, input.getTupleType().getField( fieldAccess.getField().getName(), true, false ).getIndex() );
                            }
                        }.apply( newValue );
                    } else {
                        targetIndex = input.getTupleType().getField( columnName, true, false ).getIndex();
                        newValue = rexBuilder.makeInputRef( input, targetIndex );
                    }
                    RexNode foreignValue = rexBuilder.makeInputRef( foreignColumn.getAlgDataType( rexBuilder.getTypeFactory() ), targetIndex );
                    projects.add( newValue );
                    foreignProjects.add( foreignValue );
                }
                builder
                        .project( projects )
                        .relScan( foreignKey.getReferencedKeyEntityName() )
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
                final AlgNode join = builder.join( JoinAlgType.LEFT, condition ).build();
                final AlgNode check = LogicalRelFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( join, projects.size() * 2 - 1 ) ) );
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( check, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "Update violates foreign key constraint `%s` (`%s` %s -> `%s` %s, %s)",
                                foreignKey.name, table.name, foreignKey.getFieldNames(), foreignTable.name, foreignKey.getReferencedKeyFieldNames(), constraintRule ) );
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", table.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        //
        //  Enforce reverse FOREIGN KEY constraints in UPDATE and DELETE operations
        //
        if ( (root.isDelete() || root.isUpdate() || root.isMerge()) && RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            AlgBuilder builder = AlgBuilder.create( statement );
            final RexBuilder rexBuilder = builder.getRexBuilder();
            for ( final LogicalForeignKey foreignKey : exportedKeys ) {
                final String constraintRule = root.isDelete() ? "ON DELETE " + foreignKey.deleteRule : "ON UPDATE " + foreignKey.updateRule;
                switch ( root.isDelete() ? foreignKey.deleteRule : foreignKey.updateRule ) {
                    case RESTRICT:
                        break;
                    default:
                        throw new NotImplementedException( String.format( "The foreign key option %s is not yet implemented.", constraintRule ) );
                }
                AlgNode pInput;
                if ( root.getInput() instanceof Project ) {
                    pInput = ((LogicalRelProject) root.getInput()).getInput().accept( new DeepCopyShuttle() );
                } else {
                    pInput = root.getInput().accept( new DeepCopyShuttle() );
                }
                final List<RexNode> projects = new ArrayList<>( foreignKey.fieldIds.size() );
                final List<RexNode> foreignProjects = new ArrayList<>( foreignKey.fieldIds.size() );
                final LogicalTable foreignTable = snapshot.getTable( foreignKey.entityId ).orElseThrow();
                for ( int i = 0; i < foreignKey.fieldIds.size(); ++i ) {
                    final String columnName = foreignKey.getReferencedKeyFieldNames().get( i );
                    final String foreignColumnName = foreignKey.getFieldNames().get( i );
                    final LogicalColumn column = snapshot.getColumn( table.id, columnName ).orElseThrow();
                    final LogicalColumn foreignColumn = snapshot.getColumn( foreignTable.id, foreignColumnName ).orElseThrow();
                    final RexNode inputRef = new RexIndexRef( column.position - 1, rexBuilder.getTypeFactory().createPolyType( column.type ) );
                    final RexNode foreignInputRef = new RexIndexRef( foreignColumn.position - 1, rexBuilder.getTypeFactory().createPolyType( foreignColumn.type ) );
                    projects.add( inputRef );
                    foreignProjects.add( foreignInputRef );
                }
                builder
                        .push( pInput )
                        .project( projects )
                        .relScan( foreignKey.getTableName() )
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
                final AlgNode join = builder.join( JoinAlgType.INNER, condition ).build();
                final LogicalConditionalExecute lce = LogicalConditionalExecute.create( join, lceRoot, Condition.EQUAL_TO_ZERO, ConstraintViolationException.class,
                        String.format( "%s violates foreign key constraint `%s` (`%s` %s -> `%s` %s, %s)",
                                root.isUpdate() ? "Update" : "Delete",
                                foreignKey.name, foreignTable.name, foreignKey.getFieldNames(), table.name, foreignKey.getReferencedKeyFieldNames(), constraintRule ) );
                lce.setCheckDescription( String.format( "Enforcement of foreign key `%s`.`%s`", foreignTable.name, foreignKey.name ) );
                lceRoot = lce;
            }
        }

        AlgRoot enforcementRoot = new AlgRoot( lceRoot, logicalRoot.validatedRowType, logicalRoot.kind, logicalRoot.fields, logicalRoot.collation );
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
                    AlgOptUtil.dumpPlan( "Constraint Enforcement Plan", enforcementRoot.alg, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }
        return enforcementRoot;
    }


    /**
     * {@link ConstraintTracker} tests if constraint enforcement can be enabled and no constraint is already violated.
     * This is used to ensure constraint enforcement during runtime and allows to for example to disable the enforcement before INSERT
     * and later toggle it back on with automatic re-evaluation of the constraint entities.
     */
    static public class ConstraintTracker implements ConfigListener {

        private final TransactionManager manager;


        public ConstraintTracker( TransactionManager manager ) {
            this.manager = manager;
        }


        @Override
        public void onConfigChange( Config c ) {
            if ( !testConstraintsValid() ) {
                c.setBoolean( !c.getBoolean() );
                throw new GenericRuntimeException( "Could not change the constraints." );
            }
        }


        @Override
        public void restart( Config c ) {
            if ( !testConstraintsValid() ) {
                c.setBoolean( !c.getBoolean() );
                throw new GenericRuntimeException( "After restart the constraints where not longer enforceable." );
            }
        }


        private boolean testConstraintsValid() {
            if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() || RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
                try {
                    List<LogicalTable> tables = Catalog
                            .getInstance()
                            .getSnapshot()
                            .getNamespaces( null )
                            .stream()
                            .flatMap( n -> Catalog.getInstance().getSnapshot().rel().getTables( n.id, null ).stream() )
                            .filter( t -> t.entityType == EntityType.ENTITY && t.getDataModel() == DataModel.RELATIONAL )
                            .toList();
                    Transaction transaction = this.manager.startTransaction( Catalog.defaultUserId, false, "ConstraintEnforcement" );
                    Statement statement = transaction.createStatement();
                    QueryProcessor processor = statement.getQueryProcessor();
                    List<EnforcementInformation> infos = ConstraintEnforceAttacher
                            .getConstraintAlg( new TreeSet<>( tables ), statement, EnforcementTime.ON_QUERY );
                    List<PolyImplementation> results = infos
                            .stream()
                            .map( s -> processor.prepareQuery( AlgRoot.of( s.control(), Kind.SELECT ), false ) )
                            .toList();
                    List<List<List<PolyValue>>> rows = results.stream()
                            .map( r -> r.execute( statement, -1 ).getAllRowsAndClose() )
                            .filter( r -> !r.isEmpty() )
                            .toList();

                    if ( !rows.isEmpty() ) {
                        int index = rows.get( 0 ).get( 0 ).get( 1 ).asNumber().intValue();
                        throw new TransactionException( infos.get( 0 ).errorMessages().get( index ) + "\nThere are violated constraints, the transaction was rolled back!" );
                    }
                    try {
                        statement.getTransaction().commit();
                    } catch ( TransactionException e ) {
                        throw new GenericRuntimeException( "Error while committing constraint enforcement check." );
                    }


                } catch ( TransactionException e ) {
                    return false;
                }
            }
            return true;
        }

    }


}
