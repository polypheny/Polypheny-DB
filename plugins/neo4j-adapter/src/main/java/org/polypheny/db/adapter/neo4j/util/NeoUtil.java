/*
 * Copyright 2019-2024 The Polypheny Project
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.commons.lang3.NotImplementedException;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.internal.value.FloatValue;
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.ListValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Path;
import org.neo4j.driver.types.Relationship;
import org.polypheny.db.adapter.neo4j.types.NestedPolyType;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.core.Project;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolySymbol;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.UnsupportedRexCallVisitor;

public interface NeoUtil {

    static Function1<Value, PolyValue> getTypeFunction( NestedPolyType type, boolean isNested ) {
        Function1<Value, PolyValue> getter = getUnnullableTypeFunction( type, isNested );
        return o -> {
            if ( o.isNull() ) {
                return PolyNull.NULL;
            }
            return getter.apply( o );
        };
    }

    static Function1<Value, PolyValue> getUnnullableTypeFunction( NestedPolyType type, boolean isNested ) {

        return switch ( type.getType() ) {
            case NULL -> o -> PolyNull.NULL;
            case BOOLEAN -> value -> PolyBoolean.of( value.asBoolean() );
            case TINYINT, SMALLINT, INTEGER -> v -> PolyInteger.of( v.asNumber() );
            case DATE -> v -> PolyDate.of( v.asNumber() );
            case TIME -> v -> PolyTime.of( v.asNumber() );
            case TIMESTAMP -> v -> PolyTimestamp.of( v.asNumber() );
            case BIGINT -> v -> PolyBigDecimal.of( v.asLong() );
            case DECIMAL -> v -> v instanceof StringValue ? PolyBigDecimal.of( v.asString() ) : PolyBigDecimal.of( v.asDouble() );
            case FLOAT, REAL -> v -> v instanceof StringValue ? PolyFloat.of( Float.valueOf( v.asString() ) ) : PolyFloat.of( v.asNumber() );
            case DOUBLE -> v -> v instanceof StringValue ? PolyDouble.of( Double.valueOf( v.asString() ) ) : PolyDouble.of( v.asNumber() );
            case ANY -> v -> PolyString.of( v.asObject().toString() );
            case CHAR, VARCHAR, TEXT -> v -> PolyString.of( v.asString() );
            case BINARY, VARBINARY -> v -> PolyBinary.of( v.asByteArray() );
            case FILE, IMAGE, VIDEO, AUDIO -> v -> PolyBlob.of( v.asByteArray() );
            case SYMBOL -> v -> PolySymbol.of( v.asObject() );
            case ARRAY -> {
                if ( isNested ) {
                    yield v -> PolyValue.fromTypedJson( v.asString(), PolyList.class );
                }
                Function1<Value, PolyValue> componentFunc = getTypeFunction( type.asList().types.get( 0 ), true );
                yield el -> PolyList.of( el.asList( componentFunc::apply ) );
            }
            case MAP -> value -> PolyString.of( value.asString() );
            case DOCUMENT, JSON -> value -> PolyDocument.deserialize( value.asString() );
            case GRAPH -> o -> (PolyValue) o;
            case NODE -> o -> asPolyNode( o.asNode() );
            case EDGE -> o -> asPolyEdge( o.asRelationship() );
            case PATH -> o -> asPolyPath( o.asPath() );
            case GEOMETRY -> o -> PolyGeometry.of( o.asString() );
            default -> throw new GenericRuntimeException( String.format( "Object of type %s was not transformable.", type ) );
        };


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
                nodes.stream().map( n -> Pair.of( (PolyString) null, n ) ).toList(),
                edges.stream().map( e -> Pair.of( (PolyString) null, e ) ).toList() );
    }

    static PolyNode asPolyNode( Node node ) {
        Map<PolyString, PolyValue> map = new HashMap<>( node.asMap( NeoUtil::getComparableOrString ).entrySet().stream().collect( Collectors.toMap( e -> PolyString.of( e.getKey() ), Entry::getValue ) ) );
        PolyString id = map.remove( PolyString.of( "_id" ) ).asString();
        List<PolyString> labels = new ArrayList<>();
        node.labels().forEach( e -> {
            if ( !e.startsWith( "__namespace" ) && !e.endsWith( "__" ) ) {
                labels.add( PolyString.of( e ) );
            }
        } );
        return new PolyNode( id, new PolyDictionary( map ), labels, null );
    }


    static PolyEdge asPolyEdge( Relationship relationship ) {
        Map<PolyString, PolyValue> map = new HashMap<>( relationship.asMap( NeoUtil::getComparableOrString ).entrySet().stream().collect( Collectors.toMap( e -> PolyString.of( e.getKey() ), Entry::getValue ) ) );
        String id = map.remove( PolyString.of( "_id" ) ).toString();
        String sourceId = map.remove( PolyString.of( "__sourceId__" ) ).toString();
        String targetId = map.remove( PolyString.of( "__targetId__" ) ).toString();
        return new PolyEdge( PolyString.of( id ), new PolyDictionary( map ), List.of( PolyString.of( relationship.type() ) ), PolyString.of( sourceId ), PolyString.of( targetId ), EdgeDirection.LEFT_TO_RIGHT, null );
    }

    static PolyValue getComparableOrString( Value e ) {
        if ( e.isNull() ) {
            return null;
        }
        Object obj = e.asObject();
        if ( obj instanceof PolyValue value ) {
            return value;
        }

        return asPolyValue( e );
    }

    static PolyValue asPolyValue( @NonNull Value value ) {
        if ( value instanceof IntegerValue ) {
            return PolyString.of( String.valueOf( value.asLong() ) );
        } else if ( value instanceof StringValue ) {
            return PolyString.of( value.asString() );
        } else if ( value instanceof FloatValue ) {
            return PolyString.of( String.valueOf( value.asDouble() ) );
        } else if ( value instanceof ListValue ) {
            return new PolyList<>( value.asList( NeoUtil::getComparableOrString ) );
        }
        throw new NotImplementedException( "Type not supported" );
    }

    static Function1<Record, PolyValue[]> getTypesFunction( NestedPolyType types ) {

        List<Function1<Value, PolyValue>> functions = new ArrayList<>();
        for ( NestedPolyType type : types.asList().types ) {
            functions.add( getTypeFunction( type, false ) );
        }
        if ( functions.size() == 1 ) {
            // SCALAR
            return o -> new PolyValue[]{ functions.get( 0 ).apply( o.get( 0 ) ) };
        }

        // ARRAY
        return o -> Pair.zip( o.fields(), functions ).stream()
                .map( e -> e.right.apply( e.left.value() ) )
                .toArray( PolyValue[]::new );
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


    static String rexAsString( RexLiteral literal, String mappingLabel, boolean isLiteral ) {
        Object ob = literal.getValue();
        if ( ob == null ) {
            return null;
        }
        return switch ( literal.getPolyType() ) {
            case BOOLEAN -> literal.value.asBoolean().toString();
            case TINYINT, SMALLINT, INTEGER, DATE, TIME -> literal.value.asNumber().toString();
            case BIGINT, INTERVAL, SYMBOL -> literal.getValue().toString();
            case TIMESTAMP -> literal.value.asTemporal().getMillisSinceEpoch().toString();
            case DECIMAL, FLOAT, REAL, DOUBLE -> literal.getValue().toString();
            case CHAR, VARCHAR -> {
                if ( isLiteral ) {
                    yield "'" + literal.value.asString() + "'";
                }
                yield literal.value.asString().value;

            }
            case MAP, DOCUMENT, ARRAY -> literal.value.asList().toString();
            case BINARY, VARBINARY -> literal.value.asBinary().as64String();
            case FILE, IMAGE, VIDEO, AUDIO -> Arrays.toString( literal.value.asBlob().asByteArray() );
            case NULL -> null;
            case NODE -> {
                PolyNode node = literal.value.asNode();
                if ( node.isVariable() ) {
                    yield node_( node, null, false ).build();
                }
                yield node_( node, PolyString.of( mappingLabel ), isLiteral ).build();
            }
            case EDGE -> edge_( literal.value.asEdge(), isLiteral ).build();
            default -> throw new UnsupportedOperationException( "Type is not supported by the Neo4j adapter." );
        };

    }

    static Function1<List<String>, String> getOpAsNeo( OperatorName operatorName, List<RexNode> operands, AlgDataType returnType ) {
        return switch ( operatorName ) {
            case AND -> o -> o.stream().map( e -> String.format( "(%s)", e ) ).collect( Collectors.joining( " AND " ) );
            case DIVIDE -> handleDivide( operatorName, operands, returnType );
            case DIVIDE_INTEGER -> o -> String.format( "%s / %s", o.get( 0 ), o.get( 1 ) );
            case EQUALS, IS_DISTINCT_FROM -> o -> String.format( "%s = %s", o.get( 0 ), o.get( 1 ) );
            case GREATER_THAN -> o -> String.format( "%s > %s", o.get( 0 ), o.get( 1 ) );
            case GREATER_THAN_OR_EQUAL -> o -> String.format( "%s >= %s", o.get( 0 ), o.get( 1 ) );
            case LESS_THAN -> o -> String.format( "%s < %s", o.get( 0 ), o.get( 1 ) );
            case LESS_THAN_OR_EQUAL -> o -> String.format( "%s <= %s", o.get( 0 ), o.get( 1 ) );
            case MINUS -> o -> String.format( "%s - %s", o.get( 0 ), o.get( 1 ) );
            case MULTIPLY -> o -> String.format( "%s * %s", o.get( 0 ), o.get( 1 ) );
            case NOT_EQUALS, IS_NOT_DISTINCT_FROM -> o -> String.format( "%s <> %s", o.get( 0 ), o.get( 1 ) );
            case OR -> o -> o.stream().map( e -> String.format( "(%s)", e ) ).collect( Collectors.joining( " OR " ) );
            case PLUS -> o -> String.format( "%s + %s", o.get( 0 ), o.get( 1 ) );
            case IS_NOT_NULL -> o -> String.format( "%s IS NOT NULL ", o.get( 0 ) );
            case IS_NULL -> o -> String.format( "%s IS NULL", o.get( 0 ) );
            case IS_NOT_TRUE, IS_FALSE -> o -> String.format( "NOT %s", o.get( 0 ) );
            case IS_TRUE, IS_NOT_FALSE -> o -> String.format( "%s", o.get( 0 ) );
            case IS_EMPTY -> o -> String.format( "%s = []", o.get( 0 ) );
            case IS_NOT_EMPTY -> o -> String.format( "%s <> []", o.get( 0 ) );
            case EXISTS -> o -> String.format( "exists(%s)", o.get( 0 ) );
            case NOT -> o -> String.format( "NOT %s", o.get( 0 ) );
            case UNARY_MINUS -> o -> String.format( "-%s", o.get( 0 ) );
            case UNARY_PLUS -> o -> o.get( 0 );
            case COALESCE -> o -> "coalesce(" + String.join( ", ", o ) + ")";
            case NOT_LIKE -> handleLike( operands, true );
            case CYPHER_LIKE -> o -> String.format( "%s =~ '%s'", o.get( 0 ), maybeUnquote( getAsRegex( o.get( 1 ) ) ) );
            case LIKE -> handleLike( operands, false );
            case POWER -> o -> String.format( "%s^%s", o.get( 0 ), o.get( 1 ) );
            case SQRT -> o -> String.format( "sqrt(%s)", o.get( 0 ) );
            case MOD -> o -> o.get( 0 ) + " % " + o.get( 1 );
            case LOG10 -> o -> String.format( "log10(toFloat(%s))", o.get( 0 ) );
            case ABS -> o -> String.format( "abs(toFloat(%s))", o.get( 0 ) );
            case ACOS -> o -> String.format( "acos(toFloat(%s))", o.get( 0 ) );
            case ASIN -> o -> String.format( "asin(toFloat(%s))", o.get( 0 ) );
            case ATAN -> o -> String.format( "atan(toFloat(%s))", o.get( 0 ) );
            case ATAN2 -> o -> String.format( "atan2(toFloat(%s), toFloat(%s))", o.get( 0 ), o.get( 1 ) );
            case COS -> o -> String.format( "cos(toFloat(%s))", o.get( 0 ) );
            case COT -> o -> String.format( "cot(toFloat(%s))", o.get( 0 ) );
            case RADIANS -> o -> String.format( "radians(toFloat(%s))", o.get( 0 ) );
            case ROUND -> {
                if ( operands.size() == 1 ) {
                    yield o -> String.format( "round(toFloat(%s))", o.get( 0 ) );
                } else {
                    yield o -> String.format( "round(toFloat(%s), toInteger(%s))", o.get( 0 ), o.get( 1 ) );
                }
            }
            case SIGN -> o -> String.format( "sign(toFloat(%s))", o.get( 0 ) );
            case SIN -> o -> String.format( "sin(toFloat(%s))", o.get( 0 ) );
            case TAN -> o -> String.format( "tan(toFloat(%s))", o.get( 0 ) );
            case CAST -> handleCast( operands );
            case ITEM -> o -> String.format( "%s[toInteger(%s - 1)]", o.get( 0 ), o.get( 1 ) );
            case ARRAY_VALUE_CONSTRUCTOR -> o -> "[" + String.join( ", ", o ) + "]";
            case CYPHER_EXTRACT_FROM_PATH -> o -> o.get( 0 );
            case CYPHER_ADJUST_EDGE -> o -> String.format( "%s%s%s", o.get( 1 ), o.get( 0 ), o.get( 2 ) );
            case CYPHER_REMOVE_LABELS -> Object::toString;
            case CYPHER_SET_LABELS -> o -> {
                String name = o.get( 0 );
                for ( int i = 1; i < o.size(); i++ ) {
                    name += ":" + o.get( i );
                }
                return name;
            };
            case CYPHER_SET_PROPERTIES -> throw new GenericRuntimeException( "No values should land here" );
            case CYPHER_SET_PROPERTY -> o -> String.format( "%s.%s = %s", o.get( 0 ), maybeUnquote( o.get( 1 ) ), o.get( 2 ) );
            case CYPHER_EXTRACT_PROPERTY -> o -> {
                String name = o.get( 0 );
                if ( name.contains( "." ) ) {
                    name = name.split( "\\." )[0];
                }
                return String.format( "%s.%s", name, NeoUtil.maybeUnquote( o.get( 1 ) ) );
            };
            case CYPHER_EXTRACT_ID -> o -> String.format( " %s.id ", o.get( 0 ) );
            case CYPHER_HAS_PROPERTY -> o -> String.format( " EXISTS(%s.%s) ", o.get( 0 ), NeoUtil.maybeUnquote( o.get( 1 ) ) );
            case COUNT -> o -> String.format( "count(%s)", String.join( ",", o ) );
            case AVG -> o -> String.format( "avg(%s)", o.get( 0 ) );
            case MIN -> o -> String.format( "min(%s)", o.get( 0 ) );
            case MAX -> o -> String.format( "max(%s)", o.get( 0 ) );
            default -> null;
        };

    }

    static String maybeUnquote( String key ) {
        return key.startsWith( "'" ) && key.endsWith( "'" ) ? key.substring( 1, key.length() - 1 ) : key;
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

    static boolean supports( Project p ) {
        if ( p.getTraitSet().contains( ModelTrait.GRAPH ) ) {
            return false;
        }

        if ( p.getTupleType().getFieldNames().stream().anyMatch( n -> n.matches( "^[0-9].*" ) ) ) {
            return false;
        }

        NeoSupportVisitor visitor = new NeoSupportVisitor();
        p.getProjects().forEach( project -> project.accept( visitor ) );
        return visitor.supports && !UnsupportedRexCallVisitor.containsModelItem( p.getProjects() );
    }

    static Object fixParameterValue( PolyValue value, NestedPolyType type, boolean isNested ) {
        if ( value == null ) {
            return null;
        }
        if ( type == null ) {
            return value;
        }

        if ( value.isNumber() ) {
            if ( type.getType() == PolyType.DECIMAL ) {
                return value.asNumber().bigDecimalValue().toPlainString();
            }
        }
        if ( value.isList() ) {
            if ( isNested ) {
                return value.toTypedJson();
            }
            return value.asList().value.stream().map( e -> fixParameterValue( e, type.asList().types.get( 0 ), true ) ).toList();
        }

        return switch ( type.getType() ) {
            case DATE, TIME, TIMESTAMP -> value.asTemporal().getMillisSinceEpoch();
            case DOCUMENT -> value.asDocument().toTypedJson();
            case TINYINT, INTEGER, SMALLINT -> value.asNumber().IntValue();
            case BIGINT -> value.asNumber().LongValue();
            case VARCHAR, TEXT, CHAR -> value.asString().value;
            case BOOLEAN -> value.asBoolean().value;
            case BINARY, VARBINARY, FILE, IMAGE, VIDEO, AUDIO -> value.asBinary().value;
            case FLOAT, REAL, DOUBLE -> value.asNumber().doubleValue();
            case DECIMAL -> value.asNumber().bigDecimalValue();
            case GEOMETRY -> value.asGeometry().toWKT();
            case ARRAY -> value.asList().value.stream().map( e -> {
                if ( isNested ) {
                    return e.toTypedJson();
                }
                return fixParameterValue( e, type.asList().types.get( 0 ), true );
            } ).toList();
            default -> throw new NotImplementedException( "Poly to Neo4j value" );
        };
    }

    @Getter
    class NeoSupportVisitor extends RexVisitorImpl<Void> {

        private boolean supports;


        public NeoSupportVisitor() {
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
