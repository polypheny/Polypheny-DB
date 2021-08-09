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

package org.polypheny.cql;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.polypheny.cql.exception.InvalidMethodInvocation;
import org.polypheny.cql.exception.InvalidModifierException;
import org.polypheny.cql.exception.UnexpectedTypeException;
import org.polypheny.cql.utils.Tree;
import org.polypheny.cql.utils.Tree.NodeType;
import org.polypheny.cql.utils.Tree.TraversalType;

public class CqlResource {

    public final Tree<Combiner, Index> queryRelation;
    public final QueryNode filters;
    public final Map<String, Index> indexMapping;
    public final List<SortSpecification> sortSpecifications;

    /*
        TODO: sorting : Sort Specifications.
     */

    private CqlResource( final Tree<Combiner, Index> queryRelation,
            final QueryNode filters, final Map<String, Index> indexMapping, List<SortSpecification> sortSpecs ) {
        this.queryRelation = queryRelation;
        this.filters = filters;
        this.indexMapping = indexMapping;
        this.sortSpecifications = sortSpecs;
    }


    public static CqlResource createCqlResource( CqlQuery cqlQuery ) {

        Tree<Combiner, Index> queryRelation =
                generateQueryRelation( cqlQuery.tableOperations, cqlQuery.filters, cqlQuery.indexMapping );

        return new CqlResource( queryRelation, cqlQuery.filters, cqlQuery.indexMapping, cqlQuery.sortSpecs );
    }


    private static Tree<Combiner, Index> generateQueryRelation( QueryNode tableOperations, QueryNode filters,
            HashMap<String, Index> indexMapping ) {

        AtomicReference<Tree<Combiner, Index>> queryRelation = new AtomicReference<>();
        AtomicReference<BooleanGroup> booleanGroup = new AtomicReference<>();
        AtomicReference<Index> lastIndex = new AtomicReference<>();

        if ( tableOperations != null ) {
            tableOperations.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
                try {
                    if ( nodeType == NodeType.DESTINATION_NODE ) {
                        if ( treeNode.isLeaf() ) {
                            SearchClause searchClause = treeNode.getExternalNode();
                            Index index = indexMapping.get( searchClause.searchTerm );
                            Tree<Combiner, Index> node = new Tree<>( index );
                            if ( queryRelation.get() == null ) {
                                queryRelation.set( node );
                            } else {
                                queryRelation.set(
                                        new Tree<>( queryRelation.get(),
                                                Combiner.createCombiner( booleanGroup.get(), lastIndex.get(), index ),
                                                node )
                                );
                            }

                            lastIndex.set( index );
                        } else {
                            booleanGroup.set(
                                    treeNode.getInternalNode()
                            );
                        }
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been made before"
                            + " calling the getExternalNode and getInternalNode methods." );
                } catch ( InvalidMethodInvocation e ) {
                    // TODO
                } catch ( InvalidModifierException e ) {
                    // TODO
                }

                return true;
            } );
        } else {
            HashSet<String> tablesSeen = new HashSet<>();

            filters.traverse( TraversalType.INORDER, ( treeNode, nodeType, direction, frame ) -> {
                try {
                    if ( nodeType == NodeType.DESTINATION_NODE ) {
                        if ( treeNode.isLeaf() ) {
                            SearchClause searchClause = treeNode.getExternalNode();
                            if ( !tablesSeen.contains( searchClause.indexStr ) ) {
                                Index index = indexMapping.get( searchClause.indexStr );
                                index = indexMapping.get( index.schemaName + "." + index.tableName );
                                Tree<Combiner, Index> node = new Tree<>( index );
                                if ( queryRelation.get() == null ) {
                                    queryRelation.set( node );
                                } else {
                                    BooleanGroup currentBooleanGroup = new BooleanGroup( BooleanOperator.AND );
                                    queryRelation.set(
                                            new Tree<>( queryRelation.get(),
                                                    Combiner.createCombiner( currentBooleanGroup, lastIndex.get(), index ),
                                                    node )
                                    );
                                }
                                tablesSeen.add( searchClause.indexStr );
                                lastIndex.set( index );
                            }

                            if ( !tablesSeen.contains( searchClause.searchTerm ) ) {
                                Index index = indexMapping.get( searchClause.searchTerm );
                                if ( index != null ) {
                                    index = indexMapping.get( index.schemaName + "." + index.tableName );
                                    Tree<Combiner, Index> node = new Tree<>( index );
                                    BooleanGroup currentBooleanGroup = new BooleanGroup( BooleanOperator.AND );
                                    queryRelation.set(
                                            new Tree<>( queryRelation.get(),
                                                    Combiner.createCombiner( currentBooleanGroup, lastIndex.get(), index ),
                                                    node )
                                    );
                                    tablesSeen.add( searchClause.searchTerm );
                                    lastIndex.set( index );
                                }
                            }
                        }
                    }
                } catch ( UnexpectedTypeException e ) {
                    throw new RuntimeException( "This exception will never be thrown since checks have been made before"
                            + " calling the getExternalNode method." );
                } catch ( InvalidMethodInvocation e ) {
                    // TODO
                } catch ( InvalidModifierException e ) {
                    // TODO
                }

                return true;
            } );
        }

        return queryRelation.get();
    }
}
