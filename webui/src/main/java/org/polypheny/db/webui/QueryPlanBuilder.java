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

package org.polypheny.db.webui;


import com.google.gson.Gson;
import java.util.ArrayList;
import org.apache.commons.lang.math.NumberUtils;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Util;
import org.polypheny.db.webui.models.SortDirection;
import org.polypheny.db.webui.models.SortState;
import org.polypheny.db.webui.models.UIAlgNode;


public class QueryPlanBuilder {

    private QueryPlanBuilder() {
        // This is a utility class
    }


    private static AlgBuilder createRelBuilder( final Statement statement ) {
        /*final SchemaPlus rootSchema = transaction.getSchema().plus();
        FrameworkConfig config = Frameworks.newConfigBuilder()
                .parserConfig( SqlParserConfig.DEFAULT )
                .defaultSchema( rootSchema.getSubSchema( transaction.getDefaultSchema().name ) )
                .traitDefs( (List<RelTraitDef>) null )
                .programs( Programs.heuristicJoinOrder( Programs.RULE_SET, true, 2 ) )
                .prepareContext( new ContextImpl(
                        PolyphenyDbSchema.from( rootSchema ),
                        new SlimDataContext() {
                            @Override
                            public JavaTypeFactory getTypeFactory() {
                                return new JavaTypeFactoryImpl();
                            }
                        },
                        "",
                        0,
                        0,
                        transaction ) ).build();
        return AlgBuilder.create( config );
                         */
        return AlgBuilder.create( statement );
    }


    /**
     * Build a tree using the AlgBuilder
     *
     * @param topNode top node from the tree from the user interface, with its children
     * @param statement transaction
     */
    public static AlgNode buildFromTree( final UIAlgNode topNode, final Statement statement ) {
        AlgBuilder b = createRelBuilder( statement );
        buildStep( b, topNode );
        return b.build();
    }


    public static AlgNode buildFromJsonRel( Statement statement, String json ) {
        Gson gson = new Gson();
        AlgBuilder b = createRelBuilder( statement );
        return buildFromTree( gson.fromJson( json, UIAlgNode.class ), statement );
    }


    /**
     * Set up the{@link AlgBuilder}  recursively
     */
    private static AlgBuilder buildStep( AlgBuilder builder, final UIAlgNode node ) {
        if ( node.children != null ) {
            for ( UIAlgNode n : node.children ) {
                builder = buildStep( builder, n );
            }
        }

        String[] field1 = null;
        String[] field2 = null;
        if ( node.col1 != null ) {
            field1 = node.col1.split( "\\." );
        }
        if ( node.col2 != null ) {
            field2 = node.col2.split( "\\." );
        }
        switch ( node.type ) {
            case "Scan":
                return builder.scan( Util.tokenize( node.tableName, "." ) ).as( node.tableName );
            case "Join":
                return builder.join( node.join, builder.call( getOperator( node.operator ), builder.field( node.inputCount, field1[0], field1[1] ), builder.field( node.inputCount, field2[0], field2[1] ) ) );
            case "Filter":
                String[] field = node.field.split( "\\." );
                if ( NumberUtils.isNumber( node.filter ) ) {
                    Number filter;
                    Double dbl = Double.parseDouble( node.filter );
                    filter = dbl;
                    if ( dbl % 1 == 0 ) {
                        filter = Integer.parseInt( node.filter );
                    }
                    return builder.filter( builder.call( getOperator( node.operator ), builder.field( node.inputCount, field[0], field[1] ), builder.literal( filter ) ) );
                } else {
                    return builder.filter( builder.call( getOperator( node.operator ), builder.field( node.inputCount, field[0], field[1] ), builder.literal( node.filter ) ) );
                }
            case "Project":
                ArrayList<RexNode> fields = getFields( node.fields, node.inputCount, builder );
                builder.project( fields );
                return builder;
            case "Aggregate":
                AlgBuilder.AggCall aggregation;
                String[] aggFields = node.field.split( "\\." );
                switch ( node.aggregation ) {
                    case "SUM":
                        aggregation = builder.sum( false, node.alias, builder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "COUNT":
                        aggregation = builder.count( false, node.alias, builder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "AVG":
                        aggregation = builder.avg( false, node.alias, builder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "MAX":
                        aggregation = builder.max( node.alias, builder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    case "MIN":
                        aggregation = builder.min( node.alias, builder.field( node.inputCount, aggFields[0], aggFields[1] ) );
                        break;
                    default:
                        throw new IllegalArgumentException( "unknown aggregate type" );
                }
                if ( node.groupBy == null || node.groupBy.equals( "" ) ) {
                    return builder.aggregate( builder.groupKey(), aggregation );
                } else {
                    return builder.aggregate( builder.groupKey( node.groupBy ), aggregation );
                }
            case "Sort":
                ArrayList<RexNode> columns = new ArrayList<>();
                for ( SortState s : node.sortColumns ) {
                    String[] sortField = s.column.split( "\\." );
                    if ( s.direction == SortDirection.DESC ) {
                        columns.add( builder.desc( builder.field( node.inputCount, sortField[0], sortField[1] ) ) );
                    } else {
                        columns.add( builder.field( node.inputCount, sortField[0], sortField[1] ) );
                    }
                }
                return builder.sort( columns );
            case "Union":
                return builder.union( node.all, node.inputCount );
            case "Minus":
                return builder.minus( node.all );
            case "Intersect":
                return builder.intersect( node.all, node.inputCount );
            default:
                throw new IllegalArgumentException( "PlanBuilder node of type '" + node.type + "' is not supported yet." );
        }
    }


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


    /**
     * Parse an operator and return it as SqlOperator
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

}
