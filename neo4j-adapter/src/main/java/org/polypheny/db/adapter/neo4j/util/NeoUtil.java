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

package org.polypheny.db.adapter.neo4j.util;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.edge_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.linq4j.function.Function1;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.DateString;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.TimeString;
import org.polypheny.db.util.TimestampString;

public interface NeoUtil {

    static Function1<Value, Object> getTypeFunction( PolyType type, PolyType componentType ) {
        Function1<Value, Object> getter = getUnnullableTypeFunction( type, componentType );
        return o -> {
            if ( o.isNull() ) {
                return null;
            }
            if ( getter == null ) {
                return null;
            }
            return getter.apply( o );
        };
    }

    static Function1<Value, Object> getUnnullableTypeFunction( PolyType type, PolyType componentType ) {

        switch ( type ) {
            case NULL:
                return o -> null;
            case BOOLEAN:
                return Value::asBoolean;
            case TINYINT:
                return v -> (byte) v.asInt();
            case SMALLINT:
                return v -> (short) v.asInt();
            case INTEGER:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return Value::asInt;
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case BIGINT:
                return Value::asLong;
            case DECIMAL:
                return e -> BigDecimal.valueOf( e.asDouble() );
            case FLOAT:
            case REAL:
                return e -> (float) e.asDouble();
            case DOUBLE:
                return Value::asDouble;
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                break;
            case CHAR:
            case VARCHAR:
                return ( o ) -> {
                    if ( o instanceof StringValue ) {
                        return o.asString();
                    }

                    return o.toString();
                };
            case BINARY:
            case VARBINARY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return Value::asByteArray;
            case ANY:
            case SYMBOL:
                return Value::asObject;
            case ARRAY:
                Function1<Value, Object> componentFunc = getTypeFunction( componentType, componentType );
                return el -> el.asList( componentFunc::apply );
            case MAP:
            case DOCUMENT:
            case JSON:
                return Value::asString;
            case GRAPH:
                return null;
            case NODE:
                return o -> asPolyNode( o.asNode() );
            case EDGE:
                return o -> asPolyEdge( o.asRelationship() );
            case PATH:
                return o -> asPolyPath( o.asPath() );
        }

        throw new RuntimeException( String.format( "Object of type %s was not transformable.", type ) );
    }

    static PolyPath asPolyPath( Path path ) {
        Iterator<Node> nodeIter = path.nodes().iterator();
        Iterator<Relationship> edgeIter = path.relationships().iterator();
        List<PolyNode> nodes = new ArrayList<>();
        List<PolyEdge> edges = new ArrayList<>();
        while ( nodeIter.hasNext() ) {
            nodes.add( asPolyNode( nodeIter.next() ) );
            if ( nodeIter.hasNext() ) {
                edges.add( asPolyEdge( edgeIter.next() ) );
            }
        }

        return PolyPath.create(
                nodes.stream().map( n -> Pair.of( (String) null, n ) ).collect( Collectors.toList() ),
                edges.stream().map( e -> Pair.of( (String) null, e ) ).collect( Collectors.toList() ) );
    }

    static PolyNode asPolyNode( Node node ) {
        Map<String, Comparable<?>> map = new HashMap<>( node.asMap( NeoUtil::getComparableOrString ) );
        String id = map.remove( "_id" ).toString();
        List<String> labels = new ArrayList<>();
        node.labels().forEach( e -> {
            if ( !e.startsWith( "__namespace" ) && !e.endsWith( "__" ) ) {
                labels.add( e );
            }
        } );
        return new PolyNode( id, new PolyDictionary( map ), labels, null );
    }


    static PolyEdge asPolyEdge( Relationship relationship ) {
        Map<String, Comparable<?>> map = new HashMap<>( relationship.asMap( NeoUtil::getComparableOrString ) );
        String id = map.remove( "_id" ).toString();
        String sourceId = map.remove( "__sourceId__" ).toString();
        String targetId = map.remove( "__targetId__" ).toString();
        return new PolyEdge( id, new PolyDictionary( map ), List.of( relationship.type() ), sourceId, targetId, EdgeDirection.LEFT_TO_RIGHT, null );
    }

    static Comparable<?> getComparableOrString( Value e ) {
        if ( e.isNull() ) {
            return null;
        }
        Object obj = e.asObject();
        if ( obj instanceof List<?> ) {
            return new PolyList<>( (List<Comparable<?>>) obj );
        }

        if ( obj instanceof Comparable<?> ) {
            return (Comparable<?>) obj;
        }

        return e.toString();
    }

    static Function1<Record, Object> getTypesFunction( List<PolyType> types, List<PolyType> componentTypes ) {
        int i = 0;
        List<Function1<Value, Object>> functions = new ArrayList<>();
        for ( PolyType type : types ) {
            functions.add( getTypeFunction( type, componentTypes.get( i ) ) );
            i++;
        }
        if ( functions.size() == 1 ) {
            // SCALAR
            return o -> functions.get( 0 ).apply( o.get( 0 ) );
        }

        // ARRAY
        return o -> Pair.zip( o.fields(), functions ).stream()
                .map( e -> e.right.apply( e.left.value() ) )
                .collect( Collectors.toList() )
                .toArray();
    }

    static String asParameter( long key, boolean withDollar ) {
        if ( withDollar ) {
            return String.format( "$p%s", key );
        } else {
            return String.format( "p%s", key );
        }
    }

    static String fixParameter( String name ) {
        if ( name.charAt( 0 ) == '$' ) {
            return "_" + name;
        }
        if ( name.contains( "." ) ) {
            // [namespace].[entity].[field] adjustment
            String[] splits = name.split( "\\." );
            return splits[splits.length - 1];
        }
        return name;
    }


    static PolyType getComponentTypeOrParent( AlgDataType type ) {
        if ( type.getPolyType() == PolyType.ARRAY ) {
            return type.getComponentType().getPolyType();
        }
        return type.getPolyType();
    }

    static String rexAsString( RexLiteral literal, String mappingLabel, boolean isLiteral ) {
        Object ob = literal.getValueForQueryParameterizer();
        if ( ob == null ) {
            return null;
        }
        switch ( literal.getTypeName() ) {
            case BOOLEAN:
                return literal.getValueAs( Boolean.class ).toString();
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case DATE:
            case TIME:
            case TIME_WITH_LOCAL_TIME_ZONE:
                return literal.getValueAs( Integer.class ).toString();
            case BIGINT:
            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return literal.getValueAs( Long.class ).toString();
            case DECIMAL:
            case FLOAT:
            case REAL:
            case DOUBLE:
                return literal.getValueAs( Double.class ).toString();
            case CHAR:
            case VARCHAR:
                if ( isLiteral ) {
                    return "'" + literal.getValueAs( String.class ) + "'";
                }
                return literal.getValueAs( String.class );
            case MAP:
            case DOCUMENT:
            case ARRAY:
                return literal.getValueAs( String.class );
            case BINARY:
            case VARBINARY:
            case FILE:
            case IMAGE:
            case VIDEO:
            case SOUND:
                return Arrays.toString( literal.getValueAs( byte[].class ) );
            case NULL:
                return null;
            case GRAPH:
                break;
            case NODE:
                PolyNode node = literal.getValueAs( PolyNode.class );
                if ( node.isVariable() ) {
                    return node_( node, null, false ).build();
                }
                return node_( node, mappingLabel, isLiteral ).build();
            case EDGE:
                return edge_( literal.getValueAs( PolyEdge.class ), isLiteral ).build();
            case PATH:
                break;
            case DISTINCT:
                break;
            case STRUCTURED:
                break;
            case ROW:
                break;
            case OTHER:
                break;
            case CURSOR:
                break;
            case COLUMN_LIST:
                break;
            case DYNAMIC_STAR:
                break;
            case GEOMETRY:
                break;
            case JSON:
                break;
            case SYMBOL:
                return literal.getValue().toString();
        }
        throw new UnsupportedOperationException( "Type is not supported by the Neo4j adapter." );
    }

    static Function1<List<String>, String> getOpAsNeo( OperatorName operatorName, List<RexNode> operands, AlgDataType returnType ) {
        switch ( operatorName ) {
            case AND:
                return o -> o.stream().map( e -> String.format( "(%s)", e ) ).collect( Collectors.joining( " AND " ) );
            case DIVIDE:
                return handleDivide( operatorName, operands, returnType );
            case DIVIDE_INTEGER:
                return o -> String.format( "%s / %s", o.get( 0 ), o.get( 1 ) );
            case EQUALS:
            case IS_DISTINCT_FROM:
                return o -> String.format( "%s = %s", o.get( 0 ), o.get( 1 ) );
            case GREATER_THAN:
                return o -> String.format( "%s > %s", o.get( 0 ), o.get( 1 ) );
            case GREATER_THAN_OR_EQUAL:
                return o -> String.format( "%s >= %s", o.get( 0 ), o.get( 1 ) );
            case IN:
                return o -> String.format( "%s IN %s", o.get( 0 ), o.get( 1 ) );
            case NOT_IN:
                return o -> String.format( "%s NOT IN %s", o.get( 0 ), o.get( 1 ) );
            case LESS_THAN:
                return o -> String.format( "%s < %s", o.get( 0 ), o.get( 1 ) );
            case LESS_THAN_OR_EQUAL:
                return o -> String.format( "%s <= %s", o.get( 0 ), o.get( 1 ) );
            case MINUS:
                return o -> String.format( "%s - %s", o.get( 0 ), o.get( 1 ) );
            case MULTIPLY:
                return o -> String.format( "%s * %s", o.get( 0 ), o.get( 1 ) );
            case NOT_EQUALS:
            case IS_NOT_DISTINCT_FROM:
                return o -> String.format( "%s <> %s", o.get( 0 ), o.get( 1 ) );
            case OR:
                return o -> o.stream().map( e -> String.format( "(%s)", e ) ).collect( Collectors.joining( " OR " ) );
            case PLUS:
                return o -> String.format( "%s + %s", o.get( 0 ), o.get( 1 ) );
            case DATETIME_PLUS:
                return null;
            case IS_NOT_NULL:
                return o -> String.format( "%s IS NOT NULL ", o.get( 0 ) );
            case IS_NULL:
                return o -> String.format( "%s IS NULL", o.get( 0 ) );
            case IS_NOT_TRUE:
            case IS_FALSE:
                return o -> String.format( "NOT %s", o.get( 0 ) );
            case IS_TRUE:
            case IS_NOT_FALSE:
                return o -> String.format( "%s", o.get( 0 ) );
            case IS_EMPTY:
                return o -> String.format( "%s = []", o.get( 0 ) );
            case IS_NOT_EMPTY:
                return o -> String.format( "%s <> []", o.get( 0 ) );
            case EXISTS:
                return o -> String.format( "exists(%s)", o.get( 0 ) );
            case NOT:
                return o -> String.format( "NOT %s", o.get( 0 ) );
            case UNARY_MINUS:
                return o -> String.format( "-%s", o.get( 0 ) );
            case UNARY_PLUS:
                return o -> o.get( 0 );
            case COALESCE:
                return o -> "coalesce(" + String.join( ", ", o ) + ")";
            case NOT_LIKE:
                return handleLike( operands, true );
            case LIKE:
                return handleLike( operands, false );
            case POWER:
                return o -> String.format( "%s^%s", o.get( 0 ), o.get( 1 ) );
            case SQRT:
                return o -> String.format( "sqrt(%s)", o.get( 0 ) );
            case MOD:
                return o -> o.get( 0 ) + " % " + o.get( 1 );
            case LOG10:
                return o -> String.format( "log10(toFloat(%s))", o.get( 0 ) );
            case ABS:
                return o -> String.format( "abs(toFloat(%s))", o.get( 0 ) );
            case ACOS:
                return o -> String.format( "acos(toFloat(%s))", o.get( 0 ) );
            case ASIN:
                return o -> String.format( "asin(toFloat(%s))", o.get( 0 ) );
            case ATAN:
                return o -> String.format( "atan(toFloat(%s))", o.get( 0 ) );
            case ATAN2:
                return o -> String.format( "atan2(toFloat(%s), toFloat(%s))", o.get( 0 ), o.get( 1 ) );
            case COS:
                return o -> String.format( "cos(toFloat(%s))", o.get( 0 ) );
            case COT:
                return o -> String.format( "cot(toFloat(%s))", o.get( 0 ) );
            case RADIANS:
                return o -> String.format( "radians(toFloat(%s))", o.get( 0 ) );
            case ROUND:
                if ( operands.size() == 1 ) {
                    return o -> String.format( "round(toFloat(%s))", o.get( 0 ) );
                } else {
                    return o -> String.format( "round(toFloat(%s), toInteger(%s))", o.get( 0 ), o.get( 1 ) );
                }
            case SIGN:
                return o -> String.format( "sign(toFloat(%s))", o.get( 0 ) );
            case SIN:
                return o -> String.format( "sin(toFloat(%s))", o.get( 0 ) );
            case TAN:
                return o -> String.format( "tan(toFloat(%s))", o.get( 0 ) );
            case CAST:
                return handleCast( operands );
            case ITEM:
                return o -> String.format( "%s[%s - 1]", o.get( 0 ), o.get( 1 ) );
            case ARRAY_VALUE_CONSTRUCTOR:
                return o -> "[" + String.join( ", ", o ) + "]";
            case CYPHER_EXTRACT_FROM_PATH:
                return o -> o.get( 0 );
            case CYPHER_ADJUST_EDGE:
                return o -> String.format( "%s%s%s", o.get( 1 ), o.get( 0 ), o.get( 2 ) );
            case CYPHER_REMOVE_LABELS:
                return Object::toString;
            case CYPHER_SET_LABELS:
                return o -> {
                    String name = o.get( 0 );
                    for ( int i = 1; i < o.size(); i++ ) {
                        name += ":" + o.get( i );
                    }
                    return name;
                };
            case CYPHER_SET_PROPERTIES:
                throw new RuntimeException( "No values should land here" );
            case CYPHER_SET_PROPERTY:
                return o -> String.format( "%s.%s = %s", o.get( 0 ), o.get( 1 ), o.get( 2 ) );
            case CYPHER_EXTRACT_PROPERTY:
                return o -> {
                    String name = o.get( 0 );
                    if ( name.contains( "." ) ) {
                        name = name.split( "\\." )[0];
                    }
                    return String.format( "%s.%s", name, o.get( 1 ) );
                };
            case CYPHER_HAS_PROPERTY:
                return o -> String.format( " EXISTS(%s.%s) ", o.get( 0 ), o.get( 1 ) );
            case COUNT:
                return o -> String.format( "count(%s)", String.join( ",", o ) );
            default:
                return null;
        }

    }

    private static Function1<List<String>, String> handleCast( List<RexNode> operands ) {
        if ( operands.get( 0 ).getType().getPolyType() == PolyType.DATE ) {
            return null;
        }
        return o -> o.get( 0 );
    }

    static Function1<List<String>, String> handleDivide( OperatorName operatorName, List<RexNode> operands, AlgDataType returnType ) {
        if ( PolyType.APPROX_TYPES.contains( returnType.getPolyType() ) ) {
            return o -> String.format( "toFloat(%s) / %s", o.get( 0 ), o.get( 1 ) );
        }
        return o -> String.format( "toInteger(toFloat(%s) / %s)", o.get( 0 ), o.get( 1 ) );
    }

    static Function1<List<String>, String> handleLike( List<RexNode> operands, boolean negate ) {
        if ( operands.get( 1 ).isA( Kind.DYNAMIC_PARAM ) ) {
            return null;
        }

        Function1<List<String>, String> func = o -> String.format( "%s =~ '%s'", o.get( 0 ), getAsRegex( o.get( 1 ) ) );
        if ( operands.get( 1 ).isA( Kind.INPUT_REF ) ) {
            func = o -> String.format( "%s = %s", o.get( 0 ), o.get( 1 ) );
        }
        if ( negate ) {
            Function1<List<String>, String> finalFunc = func;
            func = o -> String.format( " NOT (%s)", finalFunc.apply( o ) );
        }
        return func;
    }

    static String getAsRegex( String like ) {
        String adjusted = like.replace( "\\.", "." );
        //adjusted = adjusted.replace( ".", "_dot_" );
        adjusted = adjusted.replace( "\\*", "*" );
        adjusted = adjusted.replace( "*", "_star_" );
        adjusted = adjusted.replace( "%", ".*" );
        //adjusted = adjusted.replace( ".", "_" );
        adjusted = adjusted.replace( "_dot_", "." );
        adjusted = adjusted.replace( "_star_", "*" );
        return adjusted;
    }

    static boolean supports( Filter r ) {
        if ( r.getTraitSet().contains( ModelTrait.GRAPH ) ) {
            return false;
        }
        NeoSupportVisitor visitor = new NeoSupportVisitor();
        r.getCondition().accept( visitor );
        return visitor.supports;
    }

    static boolean supports( Project r ) {
        if ( r.getTraitSet().contains( ModelTrait.GRAPH ) ) {
            return false;
        }
        NeoSupportVisitor visitor = new NeoSupportVisitor();
        for ( RexNode project : r.getProjects() ) {
            project.accept( visitor );
        }
        return visitor.supports;
    }

    static Object fixParameterValue( Object value, Pair<PolyType, PolyType> type ) {
        if ( value instanceof BigDecimal ) {
            return ((BigDecimal) value).doubleValue();
        }
        if ( value instanceof List ) {
            return new PolyList<>( ((List<Object>) value).stream().map( v -> (Comparable<?>) fixParameterValue( v, Pair.of( type.right, type.right ) ) ).collect( Collectors.toList() ) );
        }

        if ( type == null ) {
            return value;
        }

        if ( value == null ) {
            return null;
        }
        switch ( type.left ) {
            case DATE:
                if ( value instanceof Date ) {
                    return ((Date) value).toLocalDate().toEpochDay();
                } else if ( value instanceof Integer ) {
                    return value;
                }
                return ((DateString) value).getDaysSinceEpoch();
            case TIME:
                return handleFixTime( value );
            case TIMESTAMP:
                if ( value instanceof Long ) {
                    return value;
                } else if ( value instanceof java.sql.Timestamp ) {
                    int offset = Calendar.getInstance().getTimeZone().getRawOffset();
                    return ((Timestamp) value).getTime() + offset;
                }
                return ((TimestampString) value).getMillisSinceEpoch();
            case DOCUMENT:
                return value.toString();
        }
        return value;
    }

    private static long handleFixTime( Object value ) {
        if ( value instanceof Integer ) {
            return Long.valueOf( (Integer) value );
        } else if ( value instanceof TimeString ) {
            return ((TimeString) value).getMillisOfDay();
        } else if ( value instanceof Time ) {
            return ((Time) value).toLocalTime().toNanoOfDay() / 1000000;
        } else if ( value instanceof GregorianCalendar ) {
            return ((GregorianCalendar) value).toZonedDateTime().toEpochSecond();
        }
        throw new UnsupportedOperationException( "Cannot handle Time format." );
    }

    @Getter
    class NeoSupportVisitor extends RexVisitorImpl<Void> {

        private boolean supports;


        protected NeoSupportVisitor() {
            super( true );
            this.supports = true;
        }


        @Override
        public Void visitCall( RexCall call ) {
            if ( NeoUtil.getOpAsNeo( call.op.getOperatorName(), call.operands, call.type ) == null ) {
                supports = false;
            }
            return super.visitCall( call );
        }

    }

}
