/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.webui.models.UIRelNode;
import java.util.ArrayList;
import org.apache.commons.lang.math.NumberUtils;


public class QueryPlanBuilder {

    private QueryPlanBuilder() {
        // This is a utility class
    }


    private static RelBuilder createRelBuilder( final Transaction transaction ) {
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
        return RelBuilder.create( config );
                         */
        return RelBuilder.create( transaction );
    }


    /**
     * Build a tree using the RelBuilder
     *
     * @param topNode top node from the tree from the user interface, with its children
     * @param transaction transaction
     */
    public static RelNode buildFromTree( final UIRelNode topNode, final Transaction transaction ) {
        RelBuilder b = createRelBuilder( transaction );
        b = buildStep( b, topNode );
        return b.build();
    }


    /**
     * Set up the RelBuilder recursively
     */
    private static RelBuilder buildStep( RelBuilder builder, final UIRelNode node ) {
        if ( node.children != null ) {
            for ( UIRelNode n : node.children ) {
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
            case "TableScan":
                return builder.scan( node.table ).as( node.table );
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
                String[] cols = node.fields.split( "[\\s]*,[\\s]*" );
                ArrayList<RexNode> fields = new ArrayList<>();
                for ( String c : cols ) {
                    String[] projectField = c.split( "\\." );
                    fields.add( builder.field( node.inputCount, projectField[0], projectField[1] ) );
                }
                builder.project( fields );
                return builder;
            default:
                throw new IllegalArgumentException( "Node of type " + node.type + " is not supported yet." );
        }
    }


    /**
     * Parse an operator and return it as SqlOperator
     *
     * @param operator operator for a filter condition
     * @return parsed operator as SqlOperator
     */
    private static SqlOperator getOperator( final String operator ) {
        switch ( operator ) {
            case "=":
                return SqlStdOperatorTable.EQUALS;
            case "!=":
            case "<>":
                return SqlStdOperatorTable.NOT_EQUALS;
            case "<":
                return SqlStdOperatorTable.LESS_THAN;
            case "<=":
                return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case ">":
                return SqlStdOperatorTable.GREATER_THAN;
            case ">=":
                return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            default:
                throw new IllegalArgumentException( "Operator '" + operator + "' is not supported." );
        }
    }

}
