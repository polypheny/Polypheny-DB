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

package org.polypheny.db.adaptimizer.randomtrees;


import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.http.model.SortState;
import org.polypheny.db.util.Pair;


/**
 * Model for a {@link AlgNode} coming from the RelAlg-Builder in the AdaptiveOptimizer context.
 */
public class AdaptiveNode {

    public AdaptiveNode() {}

    public AdaptiveNode( int depth ) {
        this.depth = depth;
    }

    /**
     * Type of the AlgNode, e.g. TableScan
     */
    public String type;

    /**
     * Type of Table, e.g. Table, View
     */
    public String tableType;

    /**
     * Children of this node in the tree
     */
    public AdaptiveNode[] children;

    /**
     * Number of inputs of a node.
     * Required by the AlgBuilder
     */
    public int inputCount;

    //tableScan
    public String tableName;

    //join
    public JoinAlgType join;
    //join condition
    public String operator;
    public String col1;
    public String col2;

    //filter
    //(String operator)
    public String field;
    public String filter;

    //project
    public String[] fields;

    //aggregate
    public String groupBy;
    public String aggregation;


    public String alias;
    //(String field)

    //sort
    public SortState[] sortColumns;

    //union, minus
    public boolean all;

    @Getter
    private AdaptiveTableRecord adaptiveTableRecord;

    @Getter
    @Setter
    private int depth;


    public void setAdaptiveTableRecord( AdaptiveTableRecord adaptiveTableRecord ) {
        this.adaptiveTableRecord = adaptiveTableRecord;
    }

    public void resetAdaptiveTableRecord() {
        this.adaptiveTableRecord = AdaptiveTableRecord.from( children[ 0 ].getAdaptiveTableRecord() );
    }

    public static void insertProjection( AdaptiveNode node ) {
        insertProjection( node, 0 );
    }

    public static void insertProjections( AdaptiveNode node ) {
        insertProjection( node, 0 );
        insertProjection( node, 1 );
    }

    private static void insertProjection( AdaptiveNode node, int i ) {
        AdaptiveNode tmp = node.children[ i ];

        AdaptiveNode projectionNode = new AdaptiveNode();
        projectionNode.children = new AdaptiveNode[] { tmp, null };
        projectionNode.inputCount = 1;
        projectionNode.type = "Project";
        projectionNode.setAdaptiveTableRecord( AdaptiveTableRecord.from( tmp.getAdaptiveTableRecord() ) );

        projectionNode.setDepth( node.getDepth() );
        incrementDepth( projectionNode );

        node.children[ i ] = projectionNode;
    }

    private static void incrementDepth( AdaptiveNode node ) {
        node.setDepth( node.getDepth() + 1 );
        if ( node.children == null ) {
            return;
        }
        incrementDepth( node.children[ 0 ] );
        if ( node.children[ 1 ] != null ) {
            incrementDepth( node.children[ 1 ] );
        }
    }

}