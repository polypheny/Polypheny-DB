/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.router;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.logical.LogicalTableModify;
import org.polypheny.db.rel.logical.LogicalTableScan;
import org.polypheny.db.rel.logical.LogicalValues;
import org.polypheny.db.schema.LogicalTable;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Transaction;

public class SimpleRouter extends AbstractRouter {

    @Override
    public RelRoot route( RelRoot logicalRoot, Transaction transaction ) {
        RelBuilder builder = RelBuilder.create( transaction, logicalRoot.rel.getCluster() );
        builder = build( logicalRoot.rel, builder, transaction );
        return new RelRoot(
                builder.build(),
                logicalRoot.validatedRowType,
                logicalRoot.kind,
                logicalRoot.fields,
                logicalRoot.collation );
    }


    private RelBuilder build( RelNode node, RelBuilder builder, Transaction transaction ) {
        for ( int i = 0; i < node.getInputs().size(); i++ ) {
            build( node.getInput( i ), builder, transaction );
        }
        if ( node instanceof LogicalTableScan && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                // Get placements of this table
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: This assumes there are only full table placements !!!!!!!!!!!!!!!!!!
                List<CatalogColumnPlacement> placements;
                try {
                    placements = transaction.getCatalog().getColumnPlacements( t.getColumnIds().get( 0 ) );
                } catch ( GenericCatalogException e ) {
                    throw new RuntimeException( e );
                }
                // Take first
                String storeName = placements.get( 0 ).storeUniqueName;
                return builder.scan( ImmutableList.of( storeName, ((LogicalTable) table.getTable()).getLogicalTableName() ) );
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalTableModify && node.getTable() != null ) {
            RelOptTableImpl table = (RelOptTableImpl) node.getTable();
            if ( table.getTable() instanceof LogicalTable ) {
                LogicalTable t = ((LogicalTable) table.getTable());
                // Get placements of this table
                // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                // TODO: This assumes there are only full table placements !!!!!!!!!!!!!!!!!!
                List<CatalogColumnPlacement> placements;
                try {
                    placements = transaction.getCatalog().getColumnPlacements( t.getColumnIds().get( 0 ) );
                } catch ( GenericCatalogException e ) {
                    throw new RuntimeException( e );
                }

                // Execute on all placements
                for ( CatalogColumnPlacement placement : placements ) {
                    CatalogReader catalogReader = transaction.getCatalogReader();
                    List<String> tableNames = ImmutableList.of( placement.storeUniqueName, t.getLogicalTableName() );
                    RelOptTable physical = catalogReader.getTableForMember( tableNames );
                    ModifiableTable modifiableTable = physical.unwrap( ModifiableTable.class );
                    TableModify modify;
                    if ( modifiableTable != null && modifiableTable == physical.unwrap( Table.class ) ) {
                        modify = modifiableTable.toModificationRel(
                                builder.peek().getCluster(),
                                physical,
                                catalogReader,
                                builder.peek(),
                                ((LogicalTableModify) node).getOperation(),
                                ((LogicalTableModify) node).getUpdateColumnList(),
                                ((LogicalTableModify) node).getSourceExpressionList(),
                                ((LogicalTableModify) node).isFlattened()
                        );
                    } else {
                        modify = LogicalTableModify.create(
                                physical,
                                catalogReader,
                                builder.peek(),
                                ((LogicalTableModify) node).getOperation(),
                                ((LogicalTableModify) node).getUpdateColumnList(),
                                ((LogicalTableModify) node).getSourceExpressionList(),
                                ((LogicalTableModify) node).isFlattened()
                        );
                    }
                    builder.replaceTop( modify );
                }
                return builder;
            } else {
                throw new RuntimeException( "Unexpected table. Only logical tables expected here!" );
            }
        } else if ( node instanceof LogicalValues ) {
            return builder.values( ((LogicalValues) node).tuples, node.getRowType() );
        } else {
            if ( node.getInputs().size() == 1 ) {
                builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 0 ) ) ) );
            } else if ( node.getInputs().size() == 2 ) { // Joins, SetOperations
                builder.replaceTop( node.copy( node.getTraitSet(), ImmutableList.of( builder.peek( 1 ), builder.peek( 0 ) ) ) );
            } else {
                throw new RuntimeException( "Unexpected number of input elements: " + node.getInputs().size() );
            }
            return builder;
        }
    }

}
