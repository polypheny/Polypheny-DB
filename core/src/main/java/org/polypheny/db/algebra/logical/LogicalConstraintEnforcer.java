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

package org.polypheny.db.algebra.logical;

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
import org.polypheny.db.algebra.core.BatchIterator;
import org.polypheny.db.algebra.core.ConstraintEnforcer;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.exceptions.ConstraintViolationException;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogKey.EnforcementTime;
import org.polypheny.db.catalog.entity.CatalogPrimaryKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.LogicalTable;
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
     * @param exceptionClasses
     * @param exceptionMessages
     */
    public LogicalConstraintEnforcer( AlgOptCluster cluster, AlgTraitSet traitSet, AlgNode modify, AlgNode control, List<Class<? extends Exception>> exceptionClasses, List<String> exceptionMessages ) {
        super(
                cluster,
                traitSet,
                modify,
                control,
                exceptionClasses,
                exceptionMessages );
    }


    private static EnforcementInformation getControl( AlgNode node, Statement statement ) {
        TableModify modify;
        // todo maybe use shuttle?
        if ( node instanceof TableModify ) {
            modify = (TableModify) node;
        } else if ( node instanceof BatchIterator ) {
            if ( node.getInput( 0 ) instanceof TableModify ) {
                modify = (TableModify) node.getInput( 0 );
            } else {
                throw new RuntimeException( "The tree did no conform, while generating the constraint enforcement query!" );
            }
        } else {
            throw new RuntimeException( "The tree did no conform, while generating the constraint enforcement query!" );
        }

        final CatalogTable table = getCatalogTable( modify );

        AlgBuilder builder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = modify.getCluster().getRexBuilder();

        final List<CatalogConstraint> constraints = new ArrayList<>( Catalog.getInstance().getConstraints( table.id ) );
        final List<CatalogForeignKey> foreignKeys = Catalog.getInstance()
                .getForeignKeys( table.id )
                .stream()
                .filter( f -> f.enforcementTime == EnforcementTime.ON_QUERY )
                .collect( Collectors.toList() );
        final List<CatalogForeignKey> exportedKeys = Catalog.getInstance()
                .getExportedKeys( table.id )
                .stream()
                .filter( f -> f.enforcementTime == EnforcementTime.ON_QUERY )
                .collect( Collectors.toList() );

        // Turn primary key into an artificial unique constraint
        CatalogPrimaryKey pk = Catalog.getInstance().getPrimaryKey( table.primaryKey );
        final CatalogConstraint pkc = new CatalogConstraint( 0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
        constraints.add( pkc );

        AlgNode constrainedNode;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        Queue<AlgNode> filters = new LinkedList<>();
        int pos = 0;
        List<String> errorMessages = new ArrayList<>();
        List<Class<? extends Exception>> errorClasses = new ArrayList<>();
        if ( (modify.isInsert() || modify.isMerge() || modify.isUpdate()) && RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            //builder.scan( table.getSchemaName(), table.name );
            for ( CatalogConstraint constraint : constraints ) {
                builder.clear();
                final AlgNode scan = LogicalTableScan.create( modify.getCluster(), modify.getTable() );
                builder.push( scan );
                // Enforce uniqueness between the already existing values and the new values
                List<RexInputRef> keys = constraint.key
                        .getColumnNames()
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
            for ( final CatalogForeignKey foreignKey : Stream.concat( foreignKeys.stream(), exportedKeys.stream() ).collect( Collectors.toList() ) ) {
                builder.clear();
                final AlgOptSchema algOptSchema = modify.getCatalogReader();
                final AlgOptTable scanOptTable = algOptSchema.getTableForMember( Collections.singletonList( foreignKey.getTableName() ) );
                final AlgOptTable refOptTable = algOptSchema.getTableForMember( Collections.singletonList( foreignKey.getReferencedKeyTableName() ) );
                final AlgNode scan = LogicalTableScan.create( modify.getCluster(), scanOptTable );
                final LogicalTableScan ref = LogicalTableScan.create( modify.getCluster(), refOptTable );

                builder.push( scan );
                builder.project( foreignKey.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                builder.push( ref );
                builder.project( foreignKey.getReferencedKeyColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                RexNode joinCondition = rexBuilder.makeLiteral( true );

                for ( int i = 0; i < foreignKey.getColumnNames().size(); i++ ) {
                    final String column = foreignKey.getColumnNames().get( i );
                    final String referencedColumn = foreignKey.getReferencedKeyColumnNames().get( i );
                    RexNode joinComparison = rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            builder.field( 2, 1, referencedColumn ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), joinCondition, joinComparison );
                }

                final AlgNode join = builder.join( JoinAlgType.LEFT, joinCondition ).build();
                //builder.project( builder.fields() );
                builder.push( LogicalFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( join, join.getRowType().getFieldCount() - 1 ) ) ) );
                builder.project( builder.field( foreignKey.getColumnNames().get( 0 ) ) );
                builder.rename( Collections.singletonList( "count$" + pos ) );
                builder.projectPlus( builder.literal( pos ) );

                filters.add( builder.build() );
                String type = modify.isInsert() ? "Insert" : modify.isUpdate() ? "Update" : modify.isMerge() ? "Merge" : modify.isDelete() ? "Delete" : null;
                errorMessages.add( String.format( "%s violates foreign key constraint `%s`.`%s`", type, table.name, foreignKey.name ) );
                errorClasses.add( ConstraintViolationException.class );
                pos++;
            }
        }

        if ( filters.size() == 0 ) {
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

        // todo dl add missing tree ui
        return new EnforcementInformation( constrainedNode, errorClasses, errorMessages );
    }


    public static EnforcementInformation getControl( CatalogTable table, Statement statement ) {

        AlgBuilder builder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = builder.getRexBuilder();

        final List<CatalogConstraint> constraints = new ArrayList<>( Catalog.getInstance().getConstraints( table.id ) );
        final List<CatalogForeignKey> foreignKeys = Catalog.getInstance().getForeignKeys( table.id );
        final List<CatalogForeignKey> exportedKeys = Catalog.getInstance().getExportedKeys( table.id );

        // Turn primary key into an artificial unique constraint
        CatalogPrimaryKey pk = Catalog.getInstance().getPrimaryKey( table.primaryKey );
        final CatalogConstraint pkc = new CatalogConstraint( 0L, pk.id, ConstraintType.UNIQUE, "PRIMARY KEY", pk );
        constraints.add( pkc );

        AlgNode constrainedNode;

        //
        //  Enforce UNIQUE constraints in INSERT operations
        //
        Queue<AlgNode> filters = new LinkedList<>();
        int pos = 0;
        List<String> errorMessages = new ArrayList<>();
        List<Class<? extends Exception>> errorClasses = new ArrayList<>();
        if ( RuntimeConfig.UNIQUE_CONSTRAINT_ENFORCEMENT.getBoolean() ) {
            //builder.scan( table.getSchemaName(), table.name );
            for ( CatalogConstraint constraint : constraints ) {
                builder.clear();
                builder.scan( table.getSchemaName(), table.name );//LogicalTableScan.create( modify.getCluster(), modify.getTable() );
                // Enforce uniqueness between the already existing values and the new values
                List<RexInputRef> keys = constraint.key
                        .getColumnNames()
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
            for ( final CatalogForeignKey foreignKey : Stream.concat( foreignKeys.stream(), exportedKeys.stream() ).collect( Collectors.toList() ) ) {
                builder.clear();
                //final AlgOptSchema algOptSchema = modify.getCatalogReader();
                //final AlgOptTable scanOptTable = algOptSchema.getTableForMember( Collections.singletonList( foreignKey.getTableName() ) );
                //final AlgOptTable refOptTable = algOptSchema.getTableForMember( Collections.singletonList( foreignKey.getReferencedKeyTableName() ) );
                final AlgNode scan = builder.scan( foreignKey.getSchemaName(), foreignKey.getTableName() ).build();//LogicalTableScan.create( modify.getCluster(), scanOptTable );
                final AlgNode ref = builder.scan( foreignKey.getSchemaName(), foreignKey.getReferencedKeyTableName() ).build();

                builder.push( scan );
                builder.project( foreignKey.getColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                builder.push( ref );
                builder.project( foreignKey.getReferencedKeyColumnNames().stream().map( builder::field ).collect( Collectors.toList() ) );

                RexNode joinCondition = rexBuilder.makeLiteral( true );

                for ( int i = 0; i < foreignKey.getColumnNames().size(); i++ ) {
                    final String column = foreignKey.getColumnNames().get( i );
                    final String referencedColumn = foreignKey.getReferencedKeyColumnNames().get( i );
                    RexNode joinComparison = rexBuilder.makeCall(
                            OperatorRegistry.get( OperatorName.EQUALS ),
                            builder.field( 2, 1, referencedColumn ),
                            builder.field( 2, 0, column )
                    );
                    joinCondition = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.AND ), joinCondition, joinComparison );
                }

                final AlgNode join = builder.join( JoinAlgType.LEFT, joinCondition ).build();
                //builder.project( builder.fields() );
                builder.push( LogicalFilter.create( join, rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NULL ), rexBuilder.makeInputRef( join, join.getRowType().getFieldCount() - 1 ) ) ) );
                builder.project( builder.field( foreignKey.getColumnNames().get( 0 ) ) );
                builder.rename( Collections.singletonList( "count$" + pos ) );
                builder.projectPlus( builder.literal( pos ) );

                filters.add( builder.build() );
                errorMessages.add( String.format( "Transaction violates foreign key constraint `%s`.`%s`", table.name, foreignKey.name ) );
                errorClasses.add( ConstraintViolationException.class );
                pos++;
            }
        }

        if ( filters.size() == 0 ) {
            constrainedNode = null;
        } else if ( filters.size() == 1 ) {
            constrainedNode = filters.poll();
        } else {
            builder.clear();
            filters.forEach( builder::push );
            builder.union( true, filters.size() );
            constrainedNode = builder.build();
        }

        // todo dl add missing tree ui
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
        if ( information.getControl() == null ) {
            // there is no constraint, which is enforced {@code ON QUERY} so we return the original
            return node;
        } else {
            return new LogicalConstraintEnforcer( node.getCluster(), node.getTraitSet(), node, information.getControl(), information.getErrorClasses(), information.getErrorMessages() );
        }
    }

    /*public static AlgNode create( List<CatalogTable>, Statement statement ) {
        EnforcementInformation information = getControl( modify, statement );
        return new LogicalConstraintEnforcer( modify.getCluster(), modify.getTraitSet(), modify, information.getControl(), information.getErrorClasses(), information.getErrorMessages() );
    }*/


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


    public static CatalogTable getCatalogTable( TableModify modify ) {
        Catalog catalog = Catalog.getInstance();
        String schemaName;
        if ( modify.getTable().getTable() instanceof LogicalTable ) {
            schemaName = ((LogicalTable) modify.getTable().getTable()).getLogicalSchemaName();
        } else if ( modify.getTable().getQualifiedName().size() == 2 ) {
            schemaName = modify.getTable().getQualifiedName().get( 0 );
        } else if ( modify.getTable().getQualifiedName().size() == 3 ) {
            schemaName = modify.getTable().getQualifiedName().get( 1 );
        } else {
            throw new RuntimeException( "The schema was not provided correctly!" );
        }
        final CatalogSchema schema;
        try {
            schema = catalog.getSchema( Catalog.defaultDatabaseId, schemaName );
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "The schema was not provided correctly!" );
        }

        try {
            String tableName;
            if ( modify.getTable().getQualifiedName().size() == 1 ) { // tableName
                tableName = modify.getTable().getQualifiedName().get( 0 );
            } else if ( modify.getTable().getQualifiedName().size() == 2 ) { // schemaName.tableName
                if ( !schema.name.equalsIgnoreCase( modify.getTable().getQualifiedName().get( 0 ) ) ) {
                    throw new RuntimeException( "Schema name does not match expected schema name: " + modify.getTable().getQualifiedName().get( 0 ) );
                }
                tableName = modify.getTable().getQualifiedName().get( 1 );
            } else {
                throw new RuntimeException( "Invalid table name: " + modify.getTable().getQualifiedName() );
            }
            return catalog.getTable( schema.id, tableName );

        } catch ( UnknownTableException e ) {
            log.error( "Caught exception", e );
            throw new RuntimeException( "The table was not found in the catalog!" );
        }
    }


    @Getter
    public static class EnforcementInformation {

        private final AlgNode control;
        private final List<Class<? extends Exception>> errorClasses;
        private final List<String> errorMessages;


        public EnforcementInformation( AlgNode control, List<Class<? extends Exception>> errorClasses, List<String> errorMessages ) {
            this.control = control;
            this.errorClasses = errorClasses;
            this.errorMessages = errorMessages;
        }

    }

}
