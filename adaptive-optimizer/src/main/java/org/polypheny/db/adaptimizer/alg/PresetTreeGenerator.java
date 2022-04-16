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

package org.polypheny.db.adaptimizer.alg;

import java.util.ArrayList;
import org.apache.commons.lang.math.NumberUtils;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.http.model.SortDirection;
import org.polypheny.db.http.model.SortState;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Util;

/**
 * Based on a predefined Schema.
 */
public class PresetTreeGenerator implements RelTreeGenerator {
    private static final String[] tables = new String[]{ "a", "b", "c", "d", "ab", "cd", "abcd" };

    private final int maxHeight;
    private final TreeGenRandom treeGenRandom;

    public PresetTreeGenerator( int maxHeight, TreeGenRandom treeGenRandom ) {
        this.maxHeight = maxHeight;
        this.treeGenRandom = treeGenRandom;
    }

    public AlgNode generate( Statement statement ) {
        return adaptivelyGenerate( statement );
    }

    private AlgNode adaptivelyGenerate( Statement statement ) {

        AlgBuilder algBuilder = AlgBuilder.create( statement );
        final AdaptiveNode topNode = new AdaptiveNode();
        topNode.type = this.treeGenRandom.nextOperatorType();

        this.adaptivelyGenerateTree( algBuilder,  topNode, 0 );

        return algBuilder.build();
    }


    private void binaryCase( AlgBuilder algBuilder, final AdaptiveNode node, int depth ) {
        AdaptiveNode middle = new AdaptiveNode();

        middle.type = this.treeGenRandom.nextOperatorType(); // Generate a random type for its child.

        node.children = new AdaptiveNode[]{ middle, null };
        node.inputCount = 1;

        this.adaptivelyGenerateTree( algBuilder, middle, depth + 1 );
    }


    private void unaryCase( AlgBuilder algBuilder, final AdaptiveNode node, int depth ) {
        AdaptiveNode left = new AdaptiveNode();
        AdaptiveNode right = new AdaptiveNode();

        left.type = this.treeGenRandom.nextOperatorType();
        right.type = this.treeGenRandom.nextOperatorType();

        node.children = new AdaptiveNode[]{ left, right };
        node.inputCount = 2;

        this.adaptivelyGenerateTree( algBuilder, left, depth + 1 );
        this.adaptivelyGenerateTree( algBuilder, right, depth + 1 );
    }


    /**
     * This function is a hack-draft for the best case scenario, where the AdaptiveOptimizer could
     * work on the schema as provided by the user. It works recursively in both directions. In the
     * first part the tree grows down and node-types as well as expressions are chosen randomly or
     * predefined. In the second part the recursion copies the behavior of the function
     * {@see org.polypheny.db.QueryPlanBuilder #buildStep()}.
     */
    private AlgBuilder adaptivelyGenerateTree( AlgBuilder algBuilder, final AdaptiveNode node, int depth ) {
        if ( depth < maxHeight - 1 ) {
            // growing down we generate random types of operators and choose their expressions..

            switch ( node.type ) {
                case "Join":
                    node.join = JoinAlgType.LEFT;
                    binaryCase( algBuilder, node, depth );
                    break;
                case "Union":
                    binaryCase( algBuilder, node, depth );
                    break;
                case "Minus":
                    binaryCase( algBuilder, node, depth );
                    break;
                case "Intersect":
                    binaryCase( algBuilder, node, depth );
                    break;
                case "Filter":
                    unaryCase( algBuilder, node, depth );
                    break;
                case "Project":
                    unaryCase( algBuilder, node, depth );
                    break;
                case "Sort":
                    SortState sortState = new SortState( SortDirection.DESC );
                    node.sortColumns = new SortState[]{ sortState };
                    unaryCase( algBuilder, node, depth );
                    break;
                case "Aggregate":
                    unaryCase( algBuilder, node, depth );
                    break;
                default:
                    throw new IllegalArgumentException( "AdaptiveNode of type '" + node.type + "' is not supported yet." );
            }

        } else {
            node.type = "TableScan";

            node.tableName = this.treeGenRandom.nextTable();

            node.setAdaptiveTableRecord( new AdaptiveTableRecord( node.tableName ) );
        }

        if ( depth == 0 ) {
            /*
            At this point we have a complete Tree of AdaptiveNodes, alternatively we could serialize this tree here (?)
            Instead of continuing the function, we could completely rely on the methods from QueryPlanBuilder and parse
            the jsonString. This would allow us to store the generated queries as Strings. Maybe in order to execute
            buffered sets of queries multiple times on stores multiple times, while using DML to extend row counts and
            account for different table sizes...
             */
            // Todo: question this

            /*
            String jsonString = gson.toJson( node );
            QueryPlanBuilder.buildStep( algbuilder, node )
             */
        }

        /*
        copied from QueryPlanBuilder
         */
        String[] field1 = null;
        String[] field2 = null;
        if ( node.col1 != null ) {
            field1 = node.col1.split( "\\." );
        }
        if ( node.col2 != null ) {
            field2 = node.col2.split( "\\." );
        }
        switch ( node.type ) {
            case "TableScan":
                return algBuilder.scan( Util.tokenize( node.tableName, "." ) ).as( node.tableName );
            case "Join":
                return algBuilder.join( node.join, algBuilder.call( getOperator( node.operator ), algBuilder.field( node.inputCount, field1[0], field1[1] ), algBuilder.field( node.inputCount, field2[0], field2[1] ) ) );
            case "Filter":
                String[] field = node.field.split( "\\." );
                if ( NumberUtils.isNumber( node.filter ) ) {
                    Number filter;
                    Double dbl = Double.parseDouble( node.filter );
                    filter = dbl;
                    if ( dbl % 1 == 0 ) {
                        filter = Integer.parseInt( node.filter );
                    }
                    return algBuilder.filter( algBuilder.call( getOperator( node.operator ), algBuilder.field( node.inputCount, field[0], field[1] ), algBuilder.literal( filter ) ) );
                } else {
                    return algBuilder.filter( algBuilder.call( getOperator( node.operator ), algBuilder.field( node.inputCount, field[0], field[1] ), algBuilder.literal( node.filter ) ) );
                }
            case "Project":
                ArrayList<RexNode> fields = getFields( node.fields, node.inputCount, algBuilder );
                algBuilder.project( fields );
                return algBuilder;
            case "Aggregate":
                AlgBuilder.AggCall aggregation;
                String[] aggFields = node.field.split( "\\." );
                switch ( node.aggregation ) {
                    case "SUM":
                        aggregation = algBuilder.sum( false, node.alias, algBuilder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "COUNT":
                        aggregation = algBuilder.count( false, node.alias, algBuilder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "AVG":
                        aggregation = algBuilder.avg( false, node.alias, algBuilder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "MAX":
                        aggregation = algBuilder.max( node.alias, algBuilder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "MIN":
                        aggregation = algBuilder.min( node.alias, algBuilder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    default:
                        throw new IllegalArgumentException( "unknown aggregate type" );
                }
                if ( node.groupBy == null || node.groupBy.equals( "" ) ) {
                    return algBuilder.aggregate( algBuilder.groupKey(), aggregation );
                } else {
                    return algBuilder.aggregate( algBuilder.groupKey( node.groupBy ), aggregation );
                }
            case "Sort":
                ArrayList<RexNode> columns = new ArrayList<>();
                for ( SortState s : node.sortColumns ) {
                    String[] sortField = s.column.split( "\\." );
                    if ( s.direction == SortDirection.DESC ) {
                        columns.add( algBuilder.desc( algBuilder.field( node.inputCount, sortField[0], sortField[1] ) ) );
                    } else {
                        columns.add( algBuilder.field( node.inputCount, sortField[0], sortField[1] ) );
                    }
                }
                return algBuilder.sort( columns );
            case "Union":
                return algBuilder.union( node.all, node.inputCount );
            case "Minus":
                return algBuilder.minus( node.all );
            case "Intersect":
                return algBuilder.intersect( node.all, node.inputCount );
            default:
                throw new IllegalArgumentException( "PlanBuilder node of type '" + node.type + "' is not supported yet." );
        }

    }

    /**
     * Parse an operator and return it as SqlOperator. Copied From QueryPlanBuilder.java
     *
     * @param operator operator for a filter condition
     * @return parsed operator as SqlOperator
     */
    private static Operator getOperator( final String operator ) {
        switch ( operator ) {
            case "=":
                return OperatorRegistry.get( OperatorName.EQUALS );
            case "!=":
            case "<>":
                return OperatorRegistry.get( OperatorName.NOT_EQUALS );
            case "<":
                return OperatorRegistry.get( OperatorName.LESS_THAN );
            case "<=":
                return OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
            case ">":
                return OperatorRegistry.get( OperatorName.GREATER_THAN );
            case ">=":
                return OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
            default:
                throw new IllegalArgumentException( "Operator '" + operator + "' is not supported." );
        }
    }


    /**
     *  Copied From QueryPlanBuilder.java
     */
    private static ArrayList<RexNode> getFields( String[] fields, int inputCount, AlgBuilder builder ) {
        ArrayList<RexNode> nodes = new ArrayList<>();
        for ( String f : fields ) {
            if ( f.equals( "" ) ) {
                continue;
            }
            String[] field = f.split( "\\." );
            nodes.add( builder.field( inputCount, field[0], field[1] ) );
        }
        return nodes;
    }


}
