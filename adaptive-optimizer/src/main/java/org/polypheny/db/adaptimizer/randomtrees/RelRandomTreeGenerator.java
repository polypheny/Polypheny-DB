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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import lombok.extern.slf4j.Slf4j;
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
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;

/**
 * Based on a predefined Schema.
 */
@Slf4j
public class RelRandomTreeGenerator implements RelTreeGenerator {

    private final RelRandomTreeTemplate treeGenRandom;

    public RelRandomTreeGenerator( RelRandomTreeTemplate treeGenRandom ) {
        this.treeGenRandom = treeGenRandom;
    }

    public AlgNode generate( Statement statement ) {
        return adaptivelyGenerate( statement );
    }

    private AlgNode adaptivelyGenerate( Statement statement ) {

        AlgBuilder algBuilder = AlgBuilder.create( statement );
        final AdaptiveNode topNode = new AdaptiveNode();
        topNode.type = this.treeGenRandom.nextOperator();

        this.adaptivelyGenerateTree( algBuilder,  topNode, 0 );

        return algBuilder.build();
    }


    private void unaryCase( AlgBuilder algBuilder, final AdaptiveNode node, int depth ) {
        AdaptiveNode middle = new AdaptiveNode();

        middle.type = this.treeGenRandom.nextOperator(); // Generate a random type for its child.

        node.children = new AdaptiveNode[]{ middle, null };
        node.inputCount = 1;

        this.adaptivelyGenerateTree( algBuilder, middle, depth + 1 );
    }


    private void binaryCase( AlgBuilder algBuilder, final AdaptiveNode node, int depth ) {
        AdaptiveNode left = new AdaptiveNode();
        AdaptiveNode right = new AdaptiveNode();

        left.type = this.treeGenRandom.nextOperator();
        right.type = this.treeGenRandom.nextOperator();

        node.children = new AdaptiveNode[]{ left, right };
        node.inputCount = 2;

        this.adaptivelyGenerateTree( algBuilder, left, depth + 1 );
        this.adaptivelyGenerateTree( algBuilder, right, depth + 1 );
    }

    private void setModeForBinaryOperatorNode(final AdaptiveNode node) {
        AdaptiveTableRecord left = node.children[ 0 ].getAdaptiveTableRecord();
        AdaptiveTableRecord right = node.children[ 1 ].getAdaptiveTableRecord();

        boolean retroactiveOperatorSwitch = false;
        switch ( node.type ) {
            case "Join":
                Pair<String, String> pair = this.treeGenRandom.nextJoinColumns( left, right );

                if ( pair == null ) {
                    // No possible joins found between the tables... Only option, switch to other binary operator...
                    node.type = new String[]{"Union", "Intersect", "Minus"}[ treeGenRandom.nextInt( 3 ) ];
                    retroactiveOperatorSwitch = true;
                    break;
                }

                node.setAdaptiveTableRecord( AdaptiveTableRecord.join( left, right, pair.left, pair.right ) );

                node.join = JoinAlgType.LEFT; // For now
                node.col1 = pair.left;
                node.col2 = pair.right;
                node.operator = "="; // For now
                break;
            case "Union":
            case "Intersect":
            case "Minus":
                node.all = false; // For now
                node.setAdaptiveTableRecord( AdaptiveTableRecord.from( left ) );
                break;
            default:
                throw new IllegalArgumentException( "Binary AdaptiveNode of type '" + node.type + "' is not supported yet." );
        }

        if ( retroactiveOperatorSwitch ) {
            setModeForBinaryOperatorNode( node );
        }
    }

    private void setModeForUnaryOperatorNode(final AdaptiveNode node) {
        AdaptiveTableRecord adaptiveTableRecord = AdaptiveTableRecord.from( node.children[ 0 ].getAdaptiveTableRecord() );
        Pair<String, PolyType> pair;
        switch ( node.type ) {
            case "Filter":
                pair = this.treeGenRandom.nextColumn( adaptiveTableRecord );
                node.setAdaptiveTableRecord( adaptiveTableRecord );
                node.field = pair.left;
                node.operator = "<>";
                switch ( pair.right ) {
                    case BOOLEAN:
                        node.filter = "true";
                        break;
                    case TINYINT:
                    case SMALLINT:
                    case INTEGER:
                    case BIGINT:
                    case DECIMAL:
                    case FLOAT:
                    case REAL:
                    case DOUBLE:
                        node.filter = "0";
                        break;
                    case DATE:
                        node.filter = "date '2000-01-01'";
                        break;
                    case TIME:
                        node.filter = "time '00:00:00'";
                        break;
                    case TIME_WITH_LOCAL_TIME_ZONE:
                        node.filter = "time '00:00:00 GMT'";
                        break;
                    case TIMESTAMP:
                        node.filter = "timestamp '2000-01-01 00:00:00'";
                        break;
                    case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                        node.filter = "timestamp '2000-01-01 00:00:00 GMT'";
                        break;
                    case CHAR:
                        node.filter = "$";
                        break;
                    case VARCHAR:
                        node.filter = "$varchar";
                        break;
                }
                break;
            case "Project":
                node.fields = adaptiveTableRecord.getColumns().toArray( String[]::new );
                node.setAdaptiveTableRecord( adaptiveTableRecord );
                break;
            case "Sort":
                pair = this.treeGenRandom.nextColumn( adaptiveTableRecord );
                SortState sortState = new SortState();
                sortState.sorting = true;
                sortState.column = pair.left;
                node.sortColumns = new SortState[] { sortState };
                node.setAdaptiveTableRecord( adaptiveTableRecord );
                break;
            default:
                throw new IllegalArgumentException( "Binary AdaptiveNode of type '" + node.type + "' is not supported yet." );
        }

    }


    /**
     * This function is a hack-draft for the best case scenario, where the AdaptiveOptimizer could
     * work on the schema as provided by the user. It works recursively in both directions. In the
     * first part the tree grows down and node-types as well as expressions are chosen randomly or
     * predefined. In the second part the recursion copies the behavior of the function
     * {@see org.polypheny.db.QueryPlanBuilder #buildStep()}.
     */
    private AlgBuilder adaptivelyGenerateTree( AlgBuilder algBuilder, final AdaptiveNode node, int depth ) {

        if ( depth < this.treeGenRandom.getHeight() - 1 ) {
            // growing down we generate random types of operators and choose their expressions..

            switch ( node.type ) {
                case "Join":
                case "Union":
                case "Intersect":
                case "Minus":
                    binaryCase( algBuilder, node, depth );
                    break;
                case "Filter":
                case "Project":
                case "Sort":
                    unaryCase( algBuilder, node, depth );
                    break;
 //                case "Aggregate":
//                    unaryCase( algBuilder, node, depth );
//                    break;
                default:
                    throw new IllegalArgumentException( "AdaptiveNode of type '" + node.type + "' is not supported yet." );
            }

        } else {
            Pair<String, TableRecord> pair = this.treeGenRandom.nextTable();
            node.type = "TableScan";
            node.setAdaptiveTableRecord( ( AdaptiveTableRecord ) pair.right );
            node.tableName = pair.left + "." + pair.right.getTableName();
        }


        if ( node.inputCount == 2) {
            setModeForBinaryOperatorNode( node );
        } else if ( ! Objects.equals( node.type, "TableScan" ) ) {
            setModeForUnaryOperatorNode( node );
        }

        if ( log.isDebugEnabled() ) {
            StringJoiner infoJoiner = new StringJoiner( ", ", "(", ")" );
            switch ( node.type ) {
                case "Join":
                    infoJoiner
                            .add( node.join.name() )
                            .add( node.col1 )
                            .add( node.col2 );
                    break;
                case "Union":
                    break;
                case "Intersect":
                    break;
                case "Minus":
                    break;
                case "Filter":
                    infoJoiner
                            .add( node.field )
                            .add( node.operator )
                            .add( node.filter );
                    break;
                case "Project":
                    infoJoiner
                            .add( Arrays.toString( node.fields ) );
                    break;
                case "Sort":
                    infoJoiner
                            .add( node.sortColumns[0].column )
                            .add( node.sortColumns[0].direction.name() );
                    break;
                case "TableScan":
                    infoJoiner.add( node.tableName );
                    break;
            }
            log.debug( "{} | {} - {}", "\t".repeat( depth ), node.type, infoJoiner );
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
                    double dbl = Double.parseDouble( node.filter );
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
