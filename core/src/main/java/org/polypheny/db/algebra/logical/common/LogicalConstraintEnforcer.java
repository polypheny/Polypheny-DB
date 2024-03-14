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

package org.polypheny.db.algebra.logical.common;


import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.common.ConstraintEnforcer;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.LogicalConstraint;
import org.polypheny.db.catalog.entity.logical.LogicalForeignKey;
import org.polypheny.db.catalog.entity.logical.LogicalKey.EnforcementTime;
import org.polypheny.db.catalog.entity.logical.LogicalPrimaryKey;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.ConstraintType;
import org.polypheny.db.catalog.snapshot.LogicalRelSnapshot;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;


@Slf4j
public class LogicalConstraintEnforcer extends ConstraintEnforcer {

    final static String REF_POSTFIX = "$ref";


    /**
     * This class checks if after a DML operation the constraints on the involved
     * entities still are valid.
     *
     * @param modify is the initial dml query, which modifies the entity
     * @param control is the control query, which tests if still all conditions are correct
     */
    public LogicalConstraintEnforcer( AlgCluster cluster, AlgTraitSet traitSet, AlgNode modify, AlgNode control, List<Class<? extends Exception>> exceptionClasses, List<String> exceptionMessages ) {
        super(
                cluster,
                traitSet,
                modify,
                control,
                exceptionClasses,
                exceptionMessages );
    }


    private static EnforcementInformation getControl( AlgNode node, Statement statement ) {
        ModifyExtractor extractor = new ModifyExtractor();
        node.accept( extractor );
        RelModify<?> modify = extractor.getModify();

        if ( modify == null ) {
            throw new GenericRuntimeException( "The tree did no conform, while generating the constraint enforcement query!" );
        }

        final LogicalTable table = modify.entity.unwrap( LogicalTable.class ).orElseThrow();

        AlgBuilder builder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = modify.getCluster().getRexBuilder();

        LogicalRelSnapshot snapshot = Catalog.getInstance().getSnapshot().rel();

        EnforcementTime enforcementTime = EnforcementTime.ON_QUERY;
        final List<LogicalConstraint> constraints = new ArrayList<>( snapshot.getConstraints( table.id ) )
                .stream()
                .filter( f -> f.key.enforcementTime == enforcementTime )
                .collect( Collectors.toCollection( ArrayList::new ) );
        final List<LogicalForeignKey> foreignKeys = snapshot
                .getForeignKeys( table.id )
                .stream()
                .filter( f -> f.enforcementTime == enforcementTime )
                .toList();
        final List<LogicalForeignKey> exportedKeys = snapshot
                .getExportedKeys( table.id )
                .stream()
                .filter( f -> f.enforcementTime == enforcementTime )
                .toList();

        // Turn primary key into an artificial unique constraint
        LogicalPrimaryKey pk = snapshot.getPrimaryKey( table.primaryKey ).orElseThrow();
        if ( pk.enforcementTime == enforcementTime ) {
            final LogicalConstraint pkc = new LogicalConstraint( 0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
            constraints.add( pkc );
        }

        AlgNode constrainedNode;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        Queue<AlgNode> filters = new LinkedList<>();
        int pos = 0;
        List<String> errorMessages = new ArrayList<>();
        List<Class<? extends Exception>> errorClasses = new ArrayList<>();
        if ( (modify.isInsert() || modify.isMerge() || modify.isUpdate()) && RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            //builder.relScan( table.getNamespaceName(), table.name );
            for ( LogicalConstraint constraint : constraints ) {
                builder.clear();
                final AlgNode scan = LogicalRelScan.create( modify.getCluster(), modify.getEntity() );
                builder.push( scan );
                // Enforce uniqueness between the already existing values and the new values
                List<RexIndexRef> keys = constraint.key
                        .getFieldNames()
                        .stream()
                        .map( builder::field )
                        .collect( Collectors.toList() );
                builder.project( keys );

                builder.aggregate( builder.groupKey( builder.fields() ), builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.COUNT ) ).as( "count$" + pos ) );

                builder.project( builder.field( "count$" + pos ) );

                builder.filter( builder.call( OperatorRegistry.get( OperatorName.GREATER_THAN ), builder.field( "count$" + pos ), builder.literal( 1 ) ) );
                // we attach constant to later retrieve the corresponding constraint, which was violated
                builder.projectPlus( builder.literal( pos ) );
                filters.add( builder.build() );
                String type = modify.isInsert() ? "Insert" : modify.isUpdate() ? "Update" : modify.isMerge() ? "Merge" : null;
                errorMessages.add( String.format( "%s violates unique constraint `%s`.`%s`", type, table.name, constraint.name ) );
                errorClasses.add( ConstraintViolationException.class );
                pos++;
            }
        }

        //
        //  Enforce FOREIGN KEY constraints in INSERT operations
        //
        if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            for ( final LogicalForeignKey foreignKey : Stream.concat( foreignKeys.stream(), exportedKeys.stream() ).toList() ) {
                builder.clear();
                final LogicalTable scanOptTable = snapshot.getTable( foreignKey.entityId ).orElseThrow();
                final LogicalTable refOptTable = snapshot.getTable( foreignKey.referencedKeyEntityId ).orElseThrow();
                final AlgNode scan = LogicalRelScan.create( modify.getCluster(), scanOptTable );
                final LogicalRelScan ref = LogicalRelScan.create( modify.getCluster(), refOptTable );

                builder.push( scan );
                builder.project( foreignKey.getFieldNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                builder.push( ref );
                builder.project( foreignKey.getReferencedKeyFieldNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                RexNode joinCondition = rexBuilder.makeLiteral( true );

                for ( int i = 0; i < foreignKey.getFieldNames().size(); i++ ) {
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
                //builder.project( builder.fields() );
                builder.push( LogicalRelFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( join, join.getTupleType().getFieldCount() - 1 ) ) ) );
                builder.project( builder.field( foreignKey.getFieldNames().get( 0 ) ) );
                builder.rename( Collections.singletonList( "count$" + pos ) );
                builder.projectPlus( builder.literal( pos ) );

                filters.add( builder.build() );
                String type = modify.isInsert() ? "Insert" : modify.isUpdate() ? "Update" : modify.isMerge() ? "Merge" : modify.isDelete() ? "Delete" : null;
                errorMessages.add( String.format( "%s violates foreign key constraint `%s`.`%s`", type, table.name, foreignKey.name ) );
                errorClasses.add( ConstraintViolationException.class );
                pos++;
            }
        }

        if ( filters.isEmpty() ) {
            constrainedNode = null;
        } else if ( filters.size() == 1 ) {
            constrainedNode = filters.poll();
        } else if ( filters.size() == 2 ) {
            filters.forEach( builder::push );
            builder.union( true );
            constrainedNode = builder.build();
        } else {
            builder.clear();
            constrainedNode = mergeFilter( filters, builder );
        }

        return new EnforcementInformation( constrainedNode, errorClasses, errorMessages );
    }


    public static EnforcementInformation getControl( LogicalTable table, Statement statement, EnforcementTime enforcementTime ) {

        AlgBuilder builder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = builder.getRexBuilder();
        LogicalRelSnapshot snapshot = Catalog.getInstance().getSnapshot().rel();

        final List<LogicalConstraint> constraints = snapshot
                .getConstraints( table.id )
                .stream()
                .filter( c -> c.key.enforcementTime == enforcementTime )
                .collect( Collectors.toCollection( ArrayList::new ) );
        final List<LogicalForeignKey> foreignKeys = snapshot.getForeignKeys( table.id )
                .stream()
                .filter( c -> c.enforcementTime == enforcementTime )
                .collect( Collectors.toCollection( ArrayList::new ) );
        final List<LogicalForeignKey> exportedKeys = snapshot.getExportedKeys( table.id )
                .stream()
                .filter( c -> c.enforcementTime == enforcementTime )
                .collect( Collectors.toCollection( ArrayList::new ) );

        // Turn primary key into an artificial unique constraint
        LogicalPrimaryKey pk = snapshot.getPrimaryKey( table.primaryKey ).orElseThrow();
        if ( pk.enforcementTime == enforcementTime ) {
            final LogicalConstraint pkc = new LogicalConstraint( 0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
            constraints.add( pkc );
        }

        AlgNode constrainedNode;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        Queue<AlgNode> filters = new LinkedList<>();
        int pos = 0;
        List<String> errorMessages = new ArrayList<>();
        List<Class<? extends Exception>> errorClasses = new ArrayList<>();
        if ( RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            //builder.relScan( table.getNamespaceName(), table.name );
            for ( LogicalConstraint constraint : constraints ) {
                builder.clear();
                builder.relScan( table );//LogicalTableScan.create( modify.getCluster(), modify.getTable() );
                // Enforce uniqueness between the already existing values and the new values
                List<RexIndexRef> keys = constraint.key
                        .getFieldNames()
                        .stream()
                        .map( builder::field )
                        .collect( Collectors.toList() );
                builder.project( keys );

                builder.aggregate( builder.groupKey( builder.fields() ), builder.aggregateCall( OperatorRegistry.getAgg( OperatorName.COUNT ) ).as( "count$" + pos ) );

                builder.project( builder.field( "count$" + pos ) );

                builder.filter( builder.call( OperatorRegistry.get( OperatorName.GREATER_THAN ), builder.field( "count$" + pos ), builder.literal( 1 ) ) );
                // we attach constant to later retrieve the corresponding constraint, which was violated
                builder.projectPlus( builder.literal( pos ) );
                filters.add( builder.build() );
                errorMessages.add( String.format( "Transaction violates unique constraint `%s`.`%s`", table.name, constraint.name ) );
                errorClasses.add( ConstraintViolationException.class );
                pos++;
            }
        }

        //
        //  Enforce FOREIGN KEY constraints in INSERT operations
        //
        if ( RuntimeConfig.FOREIGN_KEY_ENFORCEMENT.getBoolean() ) {
            for ( final LogicalForeignKey foreignKey : Stream.concat( foreignKeys.stream(), exportedKeys.stream() ).toList() ) {
                builder.clear();

                final AlgNode scan = builder.relScan( foreignKey.getSchemaName(), foreignKey.getTableName() ).build();
                final AlgNode ref = builder.relScan( foreignKey.getSchemaName(), foreignKey.getReferencedKeyEntityName() ).build();

                builder.push( scan );
                builder.project( foreignKey.getFieldNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                builder.push( ref );
                builder.project( foreignKey.getReferencedKeyFieldNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                RexNode joinCondition = rexBuilder.makeLiteral( true );

                for ( int i = 0; i < foreignKey.getFieldNames().size(); i++ ) {
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
                //builder.project( builder.fields() );
                builder.push( LogicalRelFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( join, join.getTupleType().getFieldCount() - 1 ) ) ) );
                builder.project( builder.field( foreignKey.getFieldNames().get( 0 ) ) );
                builder.rename( Collections.singletonList( "count$" + pos ) );
                builder.projectPlus( builder.literal( pos ) );

                filters.add( builder.build() );
                errorMessages.add( String.format( "Transaction violates foreign key constraint `%s`.`%s`", table.name, foreignKey.name ) );
                errorClasses.add( ConstraintViolationException.class );
                pos++;
            }
        }

        if ( filters.isEmpty() ) {
            constrainedNode = null;
        } else if ( filters.size() == 1 ) {
            constrainedNode = filters.poll();
        } else {
            builder.clear();
            filters.forEach( builder::push );
            builder.union( true, filters.size() );
            constrainedNode = builder.build();
        }

        return new EnforcementInformation( constrainedNode, errorClasses, errorMessages );
    }


    private static AlgNode mergeFilter( Queue<AlgNode> filters, AlgBuilder builder ) {
        if ( filters.size() >= 2 ) {
            builder.push( filters.poll() );
            builder.push( filters.poll() );
            filters.add( builder.union( true ).build() );

            return mergeFilter( filters, builder );
        } else if ( filters.size() == 1 ) {
            return filters.poll();
        } else {
            throw new RuntimeException( "Merging the Constraint was not possible." );
        }
    }


    public static LogicalConstraintEnforcer create( AlgNode modify, AlgNode control, List<Class<? extends Exception>> exceptionClasses, List<String> exceptionMessages ) {
        return new LogicalConstraintEnforcer(
                modify.getCluster(),
                modify.getTraitSet(),
                modify,
                control,
                exceptionClasses,
                exceptionMessages
        );
    }


    public static AlgNode create( AlgNode node, Statement statement ) {
        EnforcementInformation information = getControl( node, statement );
        if ( information.control() == null ) {
            // there is no constraint, which is enforced {@code ON QUERY} so we return the original
            return node;
        } else {
            return new LogicalConstraintEnforcer( node.getCluster(), node.getTraitSet(), node, information.control(), information.errorClasses(), information.errorMessages() );
        }
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalConstraintEnforcer(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ),
                inputs.get( 1 ),
                this.getExceptionClasses(),
                this.getExceptionMessages() );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }


    public record EnforcementInformation(AlgNode control, List<Class<? extends Exception>> errorClasses, List<String> errorMessages) {

        /**
         * {@link EnforcementInformation} holds all needed information regarding a constraint.
         *
         * @param control the control query, which is either execute during execution for {@code ON_QUERY}
         * or during the commit for {@code ON_COMMIT}.
         * @param errorClasses Class used to throw if constraint is violated
         * @param errorMessages messages, which describes validated constraint in case of validation
         */
        public EnforcementInformation {
        }

    }


    @Getter
    public static class ModifyExtractor extends AlgShuttleImpl {

        private LogicalRelModify modify;


        @Override
        public AlgNode visit( LogicalRelModify modify ) {
            this.modify = modify;
            return modify;
        }

    }

}
