/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.rndschema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptimizer.AdaptMathUtils;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.exceptions.RandomSchemaException;
import org.polypheny.db.adaptimizer.rnddata.RndDataException;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.ForeignKeyOption;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.exceptions.*;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

@Slf4j
@Getter(AccessLevel.PRIVATE)
public class RandomSchemaGenerator {

    private final Catalog catalog;
    private final TransactionManager transactionManager;
    private final SchemaTemplate template;
    private final DdlManager ddlManager;
    private final List<DataStore> stores;

    private CatalogSchema catalogSchema;
    private ArrayList<TableNode> tableNodes;

    public RandomSchemaGenerator( SchemaTemplate template, List<DataStore> stores ) {
        this.stores = stores;
        this.template = template;
        this.catalog = AdaptiveOptimizerImpl.getCatalog();
        this.ddlManager = AdaptiveOptimizerImpl.getDdlManager();
        this.transactionManager = AdaptiveOptimizerImpl.getTransactionManager();
    }

    public void generate() {
        String schema = template.nextSchema();
        generateSchema( schema );
        int nextNrOfTables = template.nextNrOfTables();
        try {
            generateTables( catalog.getSchema( Catalog.defaultDatabaseId, schema ).id, nextNrOfTables );
        } catch ( UnknownTableException | UnknownSchemaException e ) {
            e.printStackTrace();
        }
    }

    private void generateTables( long schema, int tables ) throws UnknownTableException {
        Transaction transaction = getTransaction();

        // Generate a tables with columns
        tableNodes = new ArrayList<>();
        for ( int idx = 0; idx < tables; idx++ ) {
            tableNodes.add( generateTable( schema, transaction ) );
        }

        List<ColumnNode> columns = tableNodes.stream().map( TableNode::getColumnNodes ).flatMap( Collection::stream ).collect( Collectors.toList() );

        // Generate Random References between columns
        Collection<Pair<ColumnNode, ColumnNode>> foreignKeys = generateReferences( tableNodes,  columns );

        // Generate Attributes for columns
        generateColumnAttributes( columns );

        // Generate Primary Keys
        tableNodes.forEach( table -> {
            if ( table.primaryKey == null ) {
                table.primaryKey = new LinkedList<>();
                table.primaryKey.add( template.getNextPrimaryKey( table ) );
            }
            table.tableBuilder.addConstraint( template.nextConstraint( table ), ConstraintType.PRIMARY, List.of( table.primaryKey.get( 0 ).columnName ) );
        } );

        tableNodes.stream().map( TableNode::getTableBuilder ).forEach( builder -> builder.build( ddlManager ) );

        // extract table ids
        for ( TableNode tableNode : tableNodes ) {
            tableNode.tableId = catalog.getTable( schema, tableNode.tableName ).id;
        }

        // Add foreign key constraints
        foreignKeys.forEach( this::addForeignKeyConstraint );

        // Commit the transaction
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            log.error( "Could not commit transaction", e );
        }
    }

    private void addForeignKeyConstraint( Pair<ColumnNode, ColumnNode> foreignKey ) {
        try {
            ddlManager.addForeignKey(
                    catalog.getTable( foreignKey.left.tableNode.tableId ),
                    catalog.getTable( foreignKey.right.tableNode.tableId ),
                    List.of( foreignKey.left.columnName ), List.of( foreignKey.right.columnName ),
                    template.nextConstraint( foreignKey ),
                    ForeignKeyOption.NONE,
                    ForeignKeyOption.NONE
            );
        } catch (UnknownColumnException | GenericCatalogException e ) {
            e.printStackTrace();
        }
    }

    private TableNode generateTable( long schema, Transaction transaction ) {
        // generate a new table
        final TableNode tableNode = new TableNode();
        tableNode.tableName = template.nextTable();
        tableNode.columnNodes = new LinkedList<>();
        tableNode.tableBuilder = new TableBuilder(
                schema,
                tableNode.tableName,
                stores,
                PlacementType.MANUAL,
                transaction.createStatement()
        );

        // add random numbers of columns to the tables
        int attr = template.nextNrOfAttributes();
        for ( int jdx = 0; jdx < attr; jdx++ ) {
            ColumnNode columnNode = new ColumnNode();
            columnNode.columnName = template.nextColumnName();
            columnNode.tableNode = tableNode;
            tableNode.columnNodes.add( columnNode );
        }

        return tableNode;
    }

    private void generateColumnAttributes( List<ColumnNode> columns ) {
        configureColumns( columns );
    }

    private void overwrite( ColumnNode columnNode, ColumnNode cNode ) {
        cNode.polyType = columnNode.polyType;
        cNode.collectionType = columnNode.collectionType;
        cNode.precision = columnNode.precision;
        cNode.scale = columnNode.scale;
        cNode.dimension = columnNode.dimension;
        cNode.cardinality = columnNode.cardinality;
        cNode.nullable = columnNode.nullable;
        cNode.collation = columnNode.collation;
        cNode.defaultValue = columnNode.defaultValue;
    }

    private void configureColumns( Collection<ColumnNode> columnNodes ) {
        columnNodes.forEach( this::configurePolyType );
        columnNodes.forEach( this::configureTableBuilderForColumnAttributes );
    }

    private void configureTableBuilderForColumnAttributes( ColumnNode columnNode ) {
        columnNode.tableNode.tableBuilder.addColumn(
                columnNode.columnName,
                columnNode.polyType,
                columnNode.collectionType,
                columnNode.precision,
                columnNode.scale,
                columnNode.dimension,
                columnNode.cardinality,
                columnNode.nullable,
                columnNode.collation,
                columnNode.defaultValue
        );
    }

    private void configurePolyType( ColumnNode columnNode ) {
        if ( columnNode.references != null ) {
            return;
        }
        if ( columnNode.referencedBy == null ) {
            columnNode.polyType = template.nextIsolatedPolyType();
            configureForPolyType( columnNode );
        } else {
            columnNode.polyType = template.nextPolyType();
            configureForPolyType( columnNode );
            columnNode.referencedBy.forEach( this::cascadeColumnPolyTypes );
        }
    }

    private void cascadeColumnPolyTypes( ColumnNode columnNode ) {
        overwrite( columnNode.references, columnNode );
        if ( columnNode.referencedBy != null ) {
            columnNode.referencedBy.forEach( this::cascadeColumnPolyTypes );
        }
    }

    private void configureForPolyType( ColumnNode columnNode ) {
        switch ( columnNode.polyType ) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                columnNode.nullable = columnNode.referencedBy == null;
                return;
            case VARCHAR:
                columnNode.precision = 24;
                columnNode.nullable = columnNode.referencedBy == null;
                columnNode.collation = Collation.CASE_INSENSITIVE;
                break;
            case BIGINT:
            case DECIMAL:
            case REAL:
            case CHAR:
            case BINARY:
            default:
                throw new RandomSchemaException( "No such PolyType implemented...", new IllegalArgumentException( columnNode.polyType.getName() ) );
        }

    }

    private void generateSchema( String schema ) {
        try {
            ddlManager.createNamespace( schema, Catalog.defaultDatabaseId, Catalog.NamespaceType.RELATIONAL, Catalog.defaultUserId, true, false );
            this.catalogSchema = catalog.getSchema( Catalog.defaultDatabaseId, schema );
        } catch (NamespaceAlreadyExistsException | UnknownSchemaException e ) {
            throw new RandomSchemaException( "Could not create Schema", e );
        }
    }

    private Transaction getTransaction() {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction(
                    Catalog.defaultUserId,
                    Catalog.defaultDatabaseId,
                    false,
                    "AdaptSchemaGenerator"
            );
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            e.printStackTrace();
            throw new RndDataException( "Could not start transaction", e );
        }
        return transaction;
    }

    private Collection<Pair<ColumnNode, ColumnNode>> generateReferences( List<TableNode> tables, List<ColumnNode> columns ) {

        List<Pair<ColumnNode, ColumnNode>> foreignKeys = new LinkedList<>();

        // With the tables in a random order
        template.shuffle( columns );

        float perColumn = (float)(columns.size() / (double) AdaptMathUtils.nCr( columns.size(), 2 ));

        // For each all possible column pairs c, d
        for ( ColumnNode outer : columns ) {
            for ( ColumnNode inner : columns ) {
                if ( Objects.equals( outer.columnName, inner.columnName ) ) {
                    continue;
                }
                if ( Objects.equals( outer.tableNode.tableName, inner.tableNode.tableName ) ) {
                    continue;
                }
                // Where d !r-> c
                if ( foreignKeys.stream().anyMatch( p -> p.left == inner && p.right == outer ) ) {
                    continue;
                }
                // If ? add a reference c r->d
                if ( template.nextReference( perColumn ) ) {
                    foreignKeys.add( new Pair<>( outer, inner ) );
                }
            }
        }

        cutCycle( foreignKeys );

        // ------------------------------------------------------------------------------------------
        // HUGE WORKAROUND: Right now only single primary columns are possible, to allow for
        // Multi-Column primary keys the generation needs to be reworked
        // Todo implement multi-columns foreign and primary keys
        List<Pair<Pair<ColumnNode, ColumnNode>,Pair<ColumnNode, ColumnNode>>> ys = new LinkedList<>();

        foreignKeys.forEach( foreignKey -> {
            if ( foreignKey.right.tableNode.primaryKey == null ) {
                foreignKey.right.tableNode.primaryKey = new LinkedList<>();
                foreignKey.right.tableNode.primaryKey.add( foreignKey.right );
            } else {
                ColumnNode newReference = foreignKey.right.tableNode.primaryKey.get( 0 );
                ys.add( new Pair<>( foreignKey, new Pair<>( foreignKey.left, newReference ) ) );
            }
        } );

        ys.forEach( p -> {
            foreignKeys.remove( p.left );
            foreignKeys.add( p.right );
        } );

        foreignKeys.forEach( foreignKey -> {
            foreignKey.left.references = foreignKey.right;
            if ( foreignKey.right.referencedBy == null ) {
                foreignKey.right.referencedBy = new HashSet<>();
            }
            foreignKey.right.referencedBy.add( foreignKey.left );
        } );

        // ------------------------------------------------------------------------------------------

        return foreignKeys;
    }

    private void cutCycle( List<Pair<ColumnNode, ColumnNode>> foreignKeys ) {
        Queue<Pair<ColumnNode, ColumnNode>> queue = new ArrayDeque<>( foreignKeys );
        while ( ! queue.isEmpty() ) {
            Pair<ColumnNode, ColumnNode> node = queue.remove();
            if ( cutCycleAux( foreignKeys, node, node, 0 ) ) {
                foreignKeys.remove( node );
            }
        }
    }

    private boolean cutCycleAux(  List<Pair<ColumnNode, ColumnNode>> foreignKeys, Pair<ColumnNode, ColumnNode> cut, Pair<ColumnNode, ColumnNode> curr, int depth ) {
        if ( depth > foreignKeys.size() ) {
            return false;
        }
        List<Pair<ColumnNode, ColumnNode>> xs = foreignKeys.stream().filter( p -> p.left ==  curr.right ).collect( Collectors.toList() );
        if ( xs.isEmpty() ) {
            return false;
        }
        if ( xs.stream().anyMatch( p -> p.right == cut.left ) ) {
            return true;
        }
        return xs.stream().anyMatch( p -> cutCycleAux( foreignKeys, cut, p, depth + 1 ) );
    }

}
