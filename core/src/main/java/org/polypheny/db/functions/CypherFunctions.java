/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.functions;

import static java.lang.Math.toRadians;
import static org.polypheny.db.functions.spatial.GeoDistanceFunctions.EARTH_RADIUS_M;
import static org.polypheny.db.type.entity.spatial.PolyGeometry.WGS_84;
import static org.polypheny.db.type.entity.spatial.PolyGeometry.WGS_84_3D;

import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntPredicate;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.graph.GraphObject;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyBigDecimal;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.relational.PolyMap;
import org.polypheny.db.type.entity.spatial.GeometryTopologicalException;
import org.polypheny.db.type.entity.spatial.PolyGeometry;
import org.polypheny.db.type.entity.spatial.PolyPoint;
import org.polypheny.db.util.Pair;


@Deterministic
@Slf4j
public class CypherFunctions {

    private CypherFunctions() {
        // empty on purpose
    }


    /**
     * Matches all suitable paths for the given graph.
     *
     * @param graph the full graph
     * @param comp the reference path, which is matched
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyPath> pathMatch( PolyGraph graph, PolyPath comp ) {
        return Linq4j.asEnumerable( graph.matchPaths( comp ) );
    }


    /**
     * Matches all suitable nodes for the given graph.
     *
     * @param graph the full graph
     * @param node the node to match
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyNode> nodeMatch( PolyGraph graph, PolyNode node ) {
        return Linq4j.asEnumerable( graph.extract( node ) );
    }


    /**
     * Extracts all nodes as {@link Enumerable} of the given graph.
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyNode> nodeExtract( PolyGraph graph ) {
        return Linq4j.asEnumerable( graph.getNodes().values() );
    }


    /**
     * Creates a new graph from the provided nodes and edges.
     *
     * @param nodes collection of nodes
     * @param edges collection of edges
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyValue[]> toGraph( Enumerable<PolyNode[]> nodes, Enumerable<PolyEdge[]> edges ) {
        Map<PolyString, PolyNode> ns = new HashMap<>();
        for ( PolyNode[] node : nodes ) {
            ns.put( node[0].id, node[0] );
        }

        Map<PolyString, PolyEdge> es = new HashMap<>();
        for ( PolyEdge[] edge : edges ) {
            es.put( edge[0].id, edge[0] );
        }
        List<PolyValue[]> values = new ArrayList<>();
        values.add( new PolyValue[]{ new PolyGraph( PolyMap.of( ns ), PolyMap.of( es ) ) } );
        return Linq4j.asEnumerable( values );
    }


    /**
     * Transforms the relational normalized edges into {@link PolyEdge}s
     *
     * @param edge the normalized edges
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyEdge[]> toEdge( Enumerable<PolyValue[]> edge ) {
        List<PolyEdge[]> edges = new ArrayList<>();

        PolyString oldId = null;
        PolyString oldSourceId = null;
        PolyString oldTargetId = null;
        Set<PolyString> oldLabels = new HashSet<>();
        Map<PolyString, PolyValue> oldProps = new HashMap<>();

        for ( PolyValue[] value : edge ) {
            PolyString id = value[0].asString();
            PolyString label = value[1].asString();
            PolyString sourceId = value[2].asString();
            PolyString targetId = value[3].asString();
            // id is 4
            PolyString key = value[5] == null ? PolyString.of( null ) : value[5].asString();
            PolyString val = value[6] == null ? PolyString.of( null ) : value[6].asString();

            if ( !id.isNull() && !id.equals( oldId ) ) {
                if ( oldId != null && !oldId.isNull() ) {
                    edges.add( new PolyEdge[]{ new PolyEdge( oldId, new PolyDictionary( oldProps ), PolyList.of( oldLabels ), oldSourceId, oldTargetId, EdgeDirection.LEFT_TO_RIGHT, null ) } );
                }
                oldId = id;
                oldLabels = new HashSet<>();
                oldSourceId = sourceId;
                oldTargetId = targetId;
                oldProps = new HashMap<>();
            }
            oldLabels.add( label );

            if ( !key.isNull() ) {
                // id | key | value | source | target
                // 13 | null| null | 12      | 10 ( no key value present )
                oldProps.put( key, CrossModelFunctions.readGraphPropertyStorageValue( val ) );
            }
        }

        if ( oldId != null && !oldId.isNull() ) {
            edges.add( new PolyEdge[]{ new PolyEdge( oldId, new PolyDictionary( oldProps ), PolyList.of( oldLabels ), oldSourceId, oldTargetId, EdgeDirection.LEFT_TO_RIGHT, null ) } );
        }

        return Linq4j.asEnumerable( edges );
    }


    /**
     * Transforms the relational normalized edges into {@link PolyEdge}s
     *
     * @param node the normalized nodes
     */
    @SuppressWarnings("unused")
    public static Enumerable<PolyNode[]> toNode( Enumerable<PolyValue[]> node ) {
        List<PolyNode[]> nodes = new ArrayList<>();

        PolyString oldId = null;
        Set<PolyString> oldLabels = new HashSet<>();
        Map<PolyString, PolyValue> oldProps = new HashMap<>();

        for ( PolyValue[] value : node ) {
            PolyString id = value[0].asString();
            PolyString label = value[1].asString();
            // id is 2
            PolyString key = value[3] == null ? PolyString.of( null ) : value[3].asString();
            PolyString val = value[4] == null ? PolyString.of( null ) : value[4].asString();

            if ( !id.isNull() && !id.equals( oldId ) ) {
                if ( oldId != null ) {
                    nodes.add( new PolyNode[]{ new PolyNode( oldId, new PolyDictionary( oldProps ), PolyList.of( oldLabels ), null ) } );
                }
                oldId = id;
                oldLabels = new HashSet<>();
                oldProps = new HashMap<>();
            }
            if ( !label.isNull() && !label.value.equals( "$" ) ) {
                // eventually no labels
                oldLabels.add( label );
            }
            if ( !key.isNull() ) {
                // eventually no properties present
                oldProps.put( key, CrossModelFunctions.readGraphPropertyStorageValue( val ) );
            }
        }

        if ( oldId != null && !oldId.isNull() ) {
            nodes.add( new PolyNode[]{ new PolyNode( oldId, new PolyDictionary( oldProps ), PolyList.of( oldLabels ), null ) } );
        }

        return Linq4j.asEnumerable( nodes );
    }


    /**
     * Returns if the given node has the given key.
     *
     * @param holder the target node or relationship
     * @param property the key to check
     */
    @SuppressWarnings("unused")
    public static PolyBoolean hasProperty( GraphPropertyHolder holder, PolyString property ) {
        return PolyBoolean.of( holder != null && holder.getProperties().containsKey( property ) );
    }


    /**
     * Returns if the given node has the given label.
     *
     * @param node the target node
     * @param label the label to check
     */
    @SuppressWarnings("unused")
    public static PolyBoolean hasLabel( PolyNode node, PolyValue label ) {
        return node == null ? null : PolyBoolean.of( node.labels.contains( label.asString() ) );
    }


    /**
     * Extracts the given variable from a provided path e.g. ()-[e]-(), e -> e
     *
     * @param path the path containing the element with the given variable
     * @param variableName the name of the given variable
     */
    @SuppressWarnings("unused")
    public static PolyValue extractFrom( PolyPath path, PolyString variableName ) {
        return path == null ? null : path.getBinding( variableName );
    }


    @SuppressWarnings("unused")
    public static PolyBoolean pathsDisjoint( PolyPath left, PolyPath right ) {
        Set<PolyString> relationships = new HashSet<>();
        left.getEdges().forEach( edge -> relationships.add( edge.getId() ) );
        return PolyBoolean.of( right.getEdges().stream().noneMatch( edge -> relationships.contains( edge.getId() ) ) );
    }


    /**
     * Extracts a given property from a graph element.
     *
     * @param holder the target from which the property is extracted
     * @param key the key of the property to extract
     * @return the property value
     */
    @SuppressWarnings("unused")
    public static PolyValue extractProperty( GraphPropertyHolder holder, PolyString key ) {
        return holder == null ? null : holder.getProperties().get( key );
    }


    @SuppressWarnings("unused")
    public static PolyValue extractProperties( GraphPropertyHolder holder ) {
        return holder == null ? null : holder.getProperties();
    }


    @SuppressWarnings("unused")
    public static PolyString extractId( GraphPropertyHolder holder ) {
        return holder == null ? null : holder.getId();
    }


    /**
     * Returns a single label of a given graph element.
     *
     * @param holder the target from which the label is extracted
     */
    @SuppressWarnings("unused")
    public static PolyString extractLabel( GraphPropertyHolder holder ) {
        return holder == null ? null : holder.getLabels().get( 0 );
    }


    /**
     * Returns all labels of a given graph element.
     *
     * @param holder the target from which the labels are extracted
     */
    @SuppressWarnings("unused")
    public static List<PolyString> extractLabels( GraphPropertyHolder holder ) {
        return holder == null ? null : holder.getLabels();
    }


    /**
     * Transform the object into a list by either wrapping it or leaving it as is if it already is.
     *
     * @param obj the object to transform
     */
    @SuppressWarnings("unused")
    public static PolyList<?> toList( PolyValue obj ) {
        if ( obj == null ) {
            return PolyList.of();
        }

        if ( obj.isList() ) {
            return obj.asList();
        } else if ( obj.isString() ) {
            try {
                return (PolyList<?>) PolyValue.fromJson( obj.asString().value );
            } catch ( Exception ignored ) {
                // ignore
            }
        }
        return PolyList.of( obj );
    }


    /**
     * Replaces the left and right side of the given edge with the provided elements.
     *
     * @param edge the edge to adjust
     * @param left the left element of the edge
     * @param right the right element of the edge
     * @return the adjusted edge, where both elements are replaced
     */
    @SuppressWarnings("unused")
    public static PolyEdge adjustEdge( PolyEdge edge, PolyNode left, PolyNode right ) {
        // If isVariable is true, the node is only a placeholder and the id should not be replaced
        return edge.from( left.isVariable() ? null : left.id, right.isVariable() ? null : right.id );
    }



    public static PolyNode createNode( PolyNode template, PolyString rowId, List<PolyString> keys, List<PolyValue> values ) {
        return new PolyNode( createdId( template.id, rowId ), properties( keys, values ), template.labels, template.getVariableName() );
    }


    public static PolyEdge createEdge( PolyEdge template, PolyString rowId, PolyNode source, PolyNode target,
            List<PolyString> keys, List<PolyValue> values ) {
        if ( source == null || target == null ) {
            throw new GenericRuntimeException( "CREATE requires a bound source and target node" );
        }
        return new PolyEdge( createdId( template.id, rowId ), properties( keys, values ), template.labels, source.id, target.id,
                template.direction, template.getVariableName() );
    }


    public static PolyPath createPath( List<PolyValue> elements ) {
        List<Pair<PolyString, PolyNode>> nodes = new ArrayList<>();
        List<Pair<PolyString, PolyEdge>> edges = new ArrayList<>();
        for ( PolyValue element : elements ) {
            if ( element.isNode() ) {
                PolyNode node = element.asNode();
                nodes.add( Pair.of( node.getVariableName(), node ) );
            } else {
                PolyEdge edge = element.asEdge();
                edges.add( Pair.of( edge.getVariableName(), edge ) );
            }
        }
        return PolyPath.create( nodes, edges );
    }


    private static PolyString createdId( PolyString template, PolyString rowId ) {
        return PolyString.of( UUID.nameUUIDFromBytes( (template.value + rowId.value).getBytes( StandardCharsets.UTF_8 ) ).toString() );
    }


    private static PolyDictionary properties( List<PolyString> keys, List<PolyValue> values ) {
        PolyDictionary properties = new PolyDictionary();
        for ( int i = 0; i < keys.size(); i++ ) {
            PolyValue value = values.get( i );
            if ( value != null && !value.isNull() ) {
                properties.put( keys.get( i ), value );
            }
        }
        return properties;
    }


    /**
     * Sets a property value for a given key.
     * If the key does not exist it gets created, if it does, it gets replaced.
     *
     * @param target the graph element to modify
     * @param key the key of the property to replace
     * @param value the value to replace it by
     * @return the adjusted graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperty( GraphPropertyHolder target, PolyString key, PolyValue value ) {
        if ( target == null ) {
            return null;
        }
        if ( value == null || value.isNull() ) {
            target.properties.remove( key );
        } else {
            target.properties.put( key, value );
        }
        return target;
    }


    /**
     * Sets all given labels for the target graph element.
     *
     * @param target the graph element to modify
     * @param labels the labels to set
     * @param replace if the labels a replaced completely or only added
     * @return the adjusted graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setLabels( GraphPropertyHolder target, List<PolyString> labels, PolyBoolean replace ) {
        if ( target == null ) {
            return null;
        }
        if ( replace.value ) {
            target.labels.clear();
        }
        target.setLabels( PolyList.of( labels ) );
        return target;
    }


    /**
     * Sets all given properties for the given graph element.
     *
     * @param target the graph element to modify
     * @param keys the keys, which are set
     * @param values the values for the keys
     * @param replace if the properties are replaced completely or only added and replaced
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperties( GraphPropertyHolder target, List<PolyString> keys, List<PolyValue> values, PolyBoolean replace ) {
        if ( target == null ) {
            return null;
        }
        if ( replace.value ) {
            target.properties.clear();
        }

        int i = 0;
        for ( PolyString key : keys ) {
            setProperty( target, key, values.get( i ) );
            i++;
        }
        return target;
    }


    /**
     * Removes the given labels for graph object.
     *
     * @param target the graph element to modify
     * @param labels the labels to remove
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeLabels( GraphPropertyHolder target, List<PolyString> labels ) {
        if ( target == null ) {
            return null;
        }
        target.labels.removeAll( labels );
        return target;
    }


    /**
     * Removes a property from a given graph element.
     *
     * @param target the graph element to modify
     * @param key the key of the property to remove
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeProperty( GraphPropertyHolder target, PolyString key ) {
        if ( target == null ) {
            return null;
        }
        target.properties.remove( key );
        return target;
    }


    public static PolyBoolean like( PolyValue b0, PolyValue b1 ) {
        if ( b0 == null || b1 == null || b0.isNull() || b1.isNull() ) {
            return null;
        }
        if ( b0.isString() && b1.isString() ) {
            return Functions.like( b0.asString(), b1.asString() );
        }
        return PolyBoolean.FALSE;
    }


    public static PolyBoolean equal( PolyValue left, PolyValue right ) {
        if ( left == null || left.isNull() || right == null || right.isNull() ) {
            return null;
        }
        if ( left instanceof PolyNumber && right instanceof PolyNumber
                || left instanceof GraphObject && right instanceof GraphObject ) {
            return PolyBoolean.of( aggregationKey( left ).equals( aggregationKey( right ) ) );
        }
        if ( left instanceof PolyList<?> leftList && right instanceof PolyList<?> rightList ) {
            return equalLists( leftList, rightList );
        }
        return Functions.eq( left, right );
    }


    private static PolyBoolean equalLists( PolyList<?> left, PolyList<?> right ) {
        if ( left.size() != right.size() ) {
            return PolyBoolean.FALSE;
        }
        boolean hasUnknown = false;
        for ( int i = 0; i < left.size(); i++ ) {
            PolyBoolean equal = equal( left.get( i ), right.get( i ) );
            if ( PolyBoolean.FALSE.equals( equal ) ) {
                return PolyBoolean.FALSE;
            }
            hasUnknown |= equal == null;
        }
        return hasUnknown ? null : PolyBoolean.TRUE;
    }


    public static PolyBoolean in( PolyValue value, PolyValue list ) {
        if ( list == null || list.isNull() ) {
            return null;
        }
        if ( !list.isList() ) {
            throw new GenericRuntimeException( "IN requires a list on its right side." );
        }
        boolean hasUnknown = false;
        for ( PolyValue candidate : list.asList() ) {
            PolyBoolean equal = equal( value, candidate );
            if ( PolyBoolean.TRUE.equals( equal ) ) {
                return PolyBoolean.TRUE;
            }
            hasUnknown |= equal == null;
        }
        return hasUnknown ? null : PolyBoolean.FALSE;
    }


    public static PolyValue last( PolyValue list ) {
        if ( list == null || list.isNull() ) {
            return null;
        }
        if ( !list.isList() ) {
            throw new GenericRuntimeException( "last() requires a list." );
        }
        PolyList<?> values = list.asList();
        return values.isEmpty() ? null : values.get( values.size() - 1 );
    }


    /**
     * Uses the same exact decimal normalization as numeric equality, so values
     * with different numeric types or scales also agree as hash keys.
     */
    public static Object aggregationKey( Object value ) {
        if ( value instanceof PolyValue poly && poly.isNull() ) {
            return null;
        }
        if ( value instanceof PolyNumber number ) {
            if ( PolyType.APPROX_TYPES.contains( number.getType() ) && !Double.isFinite( number.doubleValue() ) ) {
                return number.doubleValue();
            }
            return number.bigDecimalValue().stripTrailingZeros();
        }
        if ( value instanceof GraphPropertyHolder element ) {
            return Pair.of( element.getType(), element.getId() );
        }
        if ( value instanceof PolyPath path ) {
            return Pair.of( PolyType.PATH, path.getPath().stream().map( GraphPropertyHolder::getId ).toList() );
        }
        if ( value instanceof List<?> list ) {
            return list.stream().map( CypherFunctions::aggregationKey ).toList();
        }
        return value;
    }


    /**
     * Compares keys without changing the types of the original projected values.
     * A composite row key is distinct from a single key whose value is a list.
     */
    public static EqualityComparer<Object> groupingComparer( boolean composite ) {
        Function<Object, Object> key = composite
                ? row -> ((List<?>) row).stream().map( CypherFunctions::aggregationKey ).toList()
                : CypherFunctions::aggregationKey;
        return new EqualityComparer<>() {
            @Override
            public boolean equal( Object left, Object right ) {
                return Objects.equals( key.apply( left ), key.apply( right ) );
            }


            @Override
            public int hashCode( Object row ) {
                return Objects.hashCode( key.apply( row ) );
            }
        };
    }


    public static PolyValue aggregateExtremum( PolyValue current, PolyValue candidate, boolean minimum ) {
        if ( current == null ) {
            return candidate;
        }
        int comparison = compareForOrdering( current, candidate, false );
        return (minimum ? comparison > 0 : comparison < 0) ? candidate : current;
    }


    public static Comparator<PolyValue> orderingComparator( boolean nullsFirst, boolean descending ) {
        return descending
                ? (left, right) -> compareForOrdering( right, left, !nullsFirst )
                : (left, right) -> compareForOrdering( left, right, nullsFirst );
    }


    public static int compareForOrdering( PolyValue left, PolyValue right, boolean nullsFirst ) {
        boolean leftNull = left == null || left.isNull();
        boolean rightNull = right == null || right.isNull();
        if ( leftNull || rightNull ) {
            return nullsFirst ? Boolean.compare( rightNull, leftNull ) : Boolean.compare( leftNull, rightNull );
        }
        if ( left instanceof PolyNumber a && right instanceof PolyNumber b ) {
            return compareNumbers( a, b );
        }
        int typeOrder = Integer.compare( orderingRank( left ), orderingRank( right ) );
        if ( typeOrder != 0 ) {
            return typeOrder;
        }
        if ( left instanceof PolyList<?> a && right instanceof PolyList<?> b ) {
            for ( int i = 0; i < Math.min( a.size(), b.size() ); i++ ) {
                int elementOrder = compareForOrdering( a.get( i ), b.get( i ), false );
                if ( elementOrder != 0 ) {
                    return elementOrder;
                }
            }
            return Integer.compare( a.size(), b.size() );
        }
        return left.compareTo( right );
    }


    private static int orderingRank( PolyValue value ) {
        if ( value instanceof PolyNumber ) {
            return 12;
        }
        return switch ( value.getType() ) {
            case MAP, DOCUMENT -> 0;
            case NODE -> 1;
            case EDGE -> 2;
            case ARRAY -> 3;
            case PATH -> 4;
            case TIMESTAMP -> 5;
            case DATE -> 6;
            case TIME -> 7;
            case INTERVAL -> 8;
            case GEOMETRY -> 9;
            case CHAR, VARCHAR -> 10;
            case BOOLEAN -> 11;
            default -> 13 + value.getType().ordinal();
        };
    }


    public static final class NumericAggregate {

        private BigDecimal exactSum = BigDecimal.ZERO;
        private double approximateSum;
        private boolean approximate;
        private boolean decimal;
        private long count;


        public void add( PolyValue value ) {
            if ( value == null || value.isNull() ) {
                return;
            }
            PolyNumber number = value.asNumber();
            if ( approximate ) {
                approximateSum += number.doubleValue();
            } else if ( PolyType.APPROX_TYPES.contains( number.getType() ) ) {
                approximate = true;
                approximateSum = exactSum.doubleValue() + number.doubleValue();
            } else {
                exactSum = exactSum.add( number.bigDecimalValue() );
                decimal |= number.getType() == PolyType.DECIMAL;
            }
            count++;
        }


        public PolyNumber sum( PolyType resultType ) {
            if ( approximate || resultType == PolyType.DOUBLE ) {
                return PolyDouble.of( approximate ? approximateSum : exactSum.doubleValue() );
            }
            return decimal || resultType == PolyType.DECIMAL ? PolyBigDecimal.of( exactSum ) : PolyLong.of( exactSum.longValueExact() );
        }


        public PolyDouble average() {
            if ( count == 0 ) {
                return null;
            }
            return PolyDouble.of( approximate ? approximateSum / count : exactSum.divide( BigDecimal.valueOf( count ), MathContext.DECIMAL128 ).doubleValue() );
        }

    }


    public static PolyBoolean notEqual( PolyValue left, PolyValue right ) {
        PolyBoolean equal = equal( left, right );
        return equal == null ? null : PolyBoolean.of( !equal.value );
    }


    public static PolyBoolean lessThan( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order < 0, Functions::lt );
    }


    public static PolyBoolean lessThanOrEqual( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order <= 0, Functions::le );
    }


    public static PolyBoolean greaterThan( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order > 0, Functions::gt );
    }


    public static PolyBoolean greaterThanOrEqual( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order >= 0, Functions::ge );
    }


    private static PolyBoolean orderedComparison( PolyValue left, PolyValue right, IntPredicate test,
            BiFunction<PolyValue, PolyValue, PolyBoolean> fallback ) {
        if ( left == null || right == null || left.isNull() || right.isNull() ) {
            return null;
        }
        if ( left instanceof PolyNumber a && right instanceof PolyNumber b ) {
            return PolyBoolean.of( !Double.isNaN( a.doubleValue() ) && !Double.isNaN( b.doubleValue() )
                    && test.test( compareNumbers( a, b ) ) );
        }
        if ( left.isList() || right.isList() || left instanceof GraphObject || right instanceof GraphObject ) {
            Integer comparison = compareStructuredValues( left, right );
            return comparison == null ? null : PolyBoolean.of( test.test( comparison ) );
        }
        return fallback.apply( left, right );
    }


    private static Integer compareStructuredValues( PolyValue left, PolyValue right ) {
        if ( left == null || right == null || left.isNull() || right.isNull() ) {
            return null;
        }
        if ( left instanceof PolyNumber a && right instanceof PolyNumber b ) {
            return Double.isNaN( a.doubleValue() ) || Double.isNaN( b.doubleValue() ) ? null : compareNumbers( a, b );
        }
        if ( !left.isSameType( right ) ) {
            return null;
        }
        if ( left instanceof PolyList<?> a && right instanceof PolyList<?> b ) {
            for ( int i = 0; i < Math.min( a.size(), b.size() ); i++ ) {
                Integer order = compareStructuredValues( a.get( i ), b.get( i ) );
                if ( order == null || order != 0 ) {
                    return order;
                }
            }
            return Integer.compare( a.size(), b.size() );
        }
        return left.compareTo( right );
    }


    public static int compareNumbers( PolyNumber left, PolyNumber right ) {
        int leftRank = numericRank( left );
        int rightRank = numericRank( right );
        return leftRank == 0 && rightRank == 0
                ? left.bigDecimalValue().compareTo( right.bigDecimalValue() )
                : Integer.compare( leftRank, rightRank );
    }


    private static int numericRank( PolyNumber number ) {
        if ( PolyType.APPROX_TYPES.contains( number.getType() ) && !Double.isFinite( number.doubleValue() ) ) {
            // Order negative infinity before finite values, then positive infinity and NaN.
            double value = number.doubleValue();
            if ( value == Double.NEGATIVE_INFINITY ) {
                return -1;
            }
            return value == Double.POSITIVE_INFINITY ? 1 : 2;
        }
        return 0;
    }


    public static PolyBoolean startsWith( PolyValue value, PolyValue prefix ) {
        return compareStrings( value, prefix, String::startsWith );
    }


    public static PolyBoolean endsWith( PolyValue value, PolyValue suffix ) {
        return compareStrings( value, suffix, String::endsWith );
    }


    public static PolyBoolean contains( PolyValue value, PolyValue part ) {
        return compareStrings( value, part, String::contains );
    }


    private static PolyBoolean compareStrings( PolyValue left, PolyValue right, BiPredicate<String, String> predicate ) {
        if ( left == null || right == null || left.isNull() || right.isNull() || !left.isString() || !right.isString() ) {
            return null;
        }
        return PolyBoolean.of( predicate.test( left.asString().value, right.asString().value ) );
    }


    /**
     * 5 possible argument names with values:
     * - 3 coordinates
     * - 2 options
     */
    @SuppressWarnings("unused")
    public static PolyGeometry point(
            PolyValue argName1,
            PolyValue argValue1,
            PolyValue argName2,
            PolyValue argValue2,
            PolyValue argName3,
            PolyValue argValue3,
            PolyValue argName4,
            PolyValue argValue4,
            PolyValue argName5,
            PolyValue argValue5 ) {
        Map<PolyValue, PolyValue> map = new HashMap<>();
        if ( argName1 != null && argValue1 != null ) {
            map.put( argName1, argValue1 );
        }
        if ( argName2 != null && argValue2 != null ) {
            map.put( argName2, argValue2 );
        }
        if ( argName3 != null && argValue3 != null ) {
            map.put( argName3, argValue3 );
        }
        if ( argName4 != null && argValue4 != null ) {
            map.put( argName4, argValue4 );
        }
        if ( argName5 != null && argValue5 != null ) {
            map.put( argName5, argValue5 );
        }
        return point( PolyMap.of( map ) );
    }


    @SuppressWarnings("unused")
    public static PolyGeometry point( PolyValue map ) {
        if ( !map.isMap() ) {
            throw new GenericRuntimeException( "point() expects a map." );
        }

        PolyMap<PolyValue, PolyValue> polyMap = map.asMap();
        PolyString x = new PolyString( "x" );
        PolyString y = new PolyString( "y" );
        PolyString z = new PolyString( "z" );
        PolyString longitude = new PolyString( "longitude" );
        PolyString latitude = new PolyString( "latitude" );
        PolyString height = new PolyString( "height" );
        PolyString srid = new PolyString( "srid" );
        PolyString crs = new PolyString( "crs" );

        // In Cypher, it is possible to define the following four SRIDs
        //  7203: cartesian with 2 coordinates (no relation to ESPG:7203) -> 0
        //  9157: cartesian with 3 coordinates (no relation to ESPG:9157) -> 0
        //  4326: spherical with 2 coordinates                            -> 4326
        //  4979: spherical with 3 coordinates                            -> 4979
        int SRID = 0;
        Coordinate coordinate = new Coordinate();

        if ( polyMap.containsKey( x ) && polyMap.containsKey( y ) ) {
            if ( polyMap.get( x ).isNull() ) {
                return null;
            }
            coordinate.setX( convertPolyValueToDouble( polyMap.get( x ) ) );
            if ( polyMap.get( y ).isNull() ) {
                return null;
            }
            coordinate.setY( convertPolyValueToDouble( polyMap.get( y ) ) );
            if ( polyMap.containsKey( z ) ) {
                if ( polyMap.get( z ).isNull() ) {
                    return null;
                }
                coordinate.setZ( convertPolyValueToDouble( polyMap.get( z ) ) );
            }

            if ( polyMap.containsKey( srid ) ) {
                SRID = switch ( polyMap.get( srid ).asInteger().intValue() ) {
                    case WGS_84 -> WGS_84;
                    case WGS_84_3D -> WGS_84_3D;
                    default -> 0;
                };
            } else if ( polyMap.containsKey( crs ) ) {
                SRID = switch ( polyMap.get( crs ).asString().value ) {
                    case "WGS-84-2D" -> WGS_84;
                    case "WGS-84-3D" -> WGS_84_3D;
                    default -> 0;
                };
            }

        } else if ( polyMap.containsKey( longitude ) && polyMap.containsKey( latitude ) ) {
            if ( polyMap.get( longitude ).isNull() ) {
                return null;
            }
            coordinate.setX( convertPolyValueToDouble( polyMap.get( longitude ) ) );
            if ( polyMap.get( latitude ).isNull() ) {
                return null;
            }
            coordinate.setY( convertPolyValueToDouble( polyMap.get( latitude ) ) );
            if ( polyMap.containsKey( height ) ) {
                if ( polyMap.get( height ).isNull() ) {
                    return null;
                }
                coordinate.setZ( convertPolyValueToDouble( polyMap.get( height ) ) );
                SRID = WGS_84_3D;
            } else {
                SRID = WGS_84;
            }
        }
        GeometryFactory geometryFactory = new GeometryFactory( new PrecisionModel(), SRID );
        return PolyGeometry.of( geometryFactory.createPoint( coordinate ) );
    }


    @SuppressWarnings("unused")
    public static PolyDouble distance( PolyValue p1, PolyValue p2 ) {
        PolyPoint g1 = p1.asGeometry().asPoint();
        PolyPoint g2 = p2.asGeometry().asPoint();

        if ( !Objects.equals( g1.getSRID(), g2.getSRID() ) ) {
            throw new GenericRuntimeException( "Cannot compute point.distance(%s, %s) because of different SRIDs.".formatted( g1, g2 ) );
        }
        Integer srid = g1.getSRID();

        try {
            if ( g1.hasZ() && g2.hasZ() ) {
                if ( srid == 0 ) {
                    return new PolyDouble(
                            Math.sqrt( Math.pow( g2.getX() - g1.getX(), 2 ) + Math.pow( g2.getY() - g1.getY(), 2 ) + Math.pow( g2.getZ() - g1.getZ(), 2 ) )
                    );
                } else if ( srid == WGS_84_3D ) {
                    // See https://github.com/neo4j/neo4j/blob/5.20/community/values/src/main/java/org/neo4j/values/storable/CRSCalculator.java
                    double greatCircleDistance = getGreatCircleDistance( g1, g2 );
                    double avgHeight = (g1.getZ() + g2.getZ()) / 2;
                    // Note: Neo4j uses a different earth radius of 6378140.0, which is why the same calculation
                    //       in Neo4j does not yield (exactly) the same results.
                    double distance2D = (EARTH_RADIUS_M + avgHeight) * greatCircleDistance;
                    double heightDifference = g1.getZ() - g2.getZ();
                    return new PolyDouble( Math.sqrt( Math.pow( distance2D, 2 ) + Math.pow( heightDifference, 2 ) ) );
                }
            } else {
                return new PolyDouble( g1.distance( g2 ) );
            }
        } catch ( GeometryTopologicalException e ) {
            throw new GenericRuntimeException( e );
        }

        throw new GenericRuntimeException( "This should not be possible!" );
    }


    @SuppressWarnings("unused")
    public static PolyDouble distanceNeo4j( PolyValue p1, PolyValue p2 ) {
        PolyPoint g1 = p1.asGeometry().asPoint();
        PolyPoint g2 = p2.asGeometry().asPoint();

        if ( !Objects.equals( g1.getSRID(), g2.getSRID() ) ) {
            throw new GenericRuntimeException( "Cannot compute point.distance(%s, %s) because of different SRIDs.".formatted( g1, g2 ) );
        }
        Integer srid = g1.getSRID();

        try {
            if ( g1.hasZ() && g2.hasZ() ) {
                if ( srid == 0 ) {
                    return distance( g1, g2 );
                } else if ( srid == WGS_84_3D ) {
                    // See https://github.com/neo4j/neo4j/blob/5.20/community/values/src/main/java/org/neo4j/values/storable/CRSCalculator.java
                    double greatCircleDistance = getGreatCircleDistance( g1, g2 );
                    double avgHeight = (g1.getZ() + g2.getZ()) / 2;
                    final double EARTH_RADIUS_M_NEO4J = 6378140.0;
                    double distance2D = (EARTH_RADIUS_M_NEO4J + avgHeight) * greatCircleDistance;
                    double heightDifference = g1.getZ() - g2.getZ();
                    return new PolyDouble( Math.sqrt( Math.pow( distance2D, 2 ) + Math.pow( heightDifference, 2 ) ) );
                }
            } else {
                return new PolyDouble( g1.distance( g2 ) );
            }
        } catch ( GeometryTopologicalException e ) {
            throw new GenericRuntimeException( e );
        }

        throw new GenericRuntimeException( "This should not be possible!" );
    }


    /**
     * Use same logic as Neo4j to calculate the spherical distance.
     * See: <a href="https://github.com/neo4j/neo4j/blob/5.20/community/values/src/main/java/org/neo4j/values/storable/CRSCalculator.java">GitHub</a>
     */
    private static double getGreatCircleDistance( PolyPoint g1, PolyPoint g2 ) {
        double lat1 = toRadians( g1.getY() );
        double lat2 = toRadians( g2.getY() );
        double latDifference = lat2 - lat1;
        double lonDifference = toRadians( g2.getX() - g1.getX() );
        double alpha = Math.pow( Math.sin( latDifference / 2 ), 2 ) +
                Math.cos( lat1 ) * Math.cos( lat2 ) * Math.pow( Math.sin( lonDifference / 2 ), 2 );
        return 2.0 * Math.atan2( Math.sqrt( alpha ), Math.sqrt( 1 - alpha ) );
    }


    @SuppressWarnings("unused")
    public static PolyBoolean withinBBox( PolyValue point, PolyValue lowerLeft, PolyValue upperRight ) {
        PolyPoint g = point.asGeometry().asPoint();
        PolyPoint lowerLeftGeometry = lowerLeft.asGeometry().asPoint();
        PolyPoint upperRightGeometry = upperRight.asGeometry().asPoint();

        if ( !(g.getSRID().equals( lowerLeftGeometry.getSRID() ) && lowerLeftGeometry.getSRID().equals( upperRightGeometry.getSRID() )) ) {
            // Return null if the CRS of all points are not the same.
            //return PolyNull.NULL;
            return null;
        }
        if ( !(g.hasZ() == lowerLeftGeometry.hasZ() && lowerLeftGeometry.hasZ() == upperRightGeometry.hasZ()) ) {
            // Return null if the CRS of all points are not the same.
            //return PolyNull.NULL;
            return null;
        }

        PolyGeometry gBBox = createBbox( lowerLeftGeometry, upperRightGeometry );
        return new PolyBoolean( g.coveredBy( gBBox ) );
    }


    @SuppressWarnings("unused")
    public static PolyBoolean withinGeometry( PolyValue point, PolyValue geometry ) {
        PolyPoint g = point.asGeometry().asPoint();
        PolyGeometry polyGeometry = geometry.asGeometry();

        if ( !(g.getSRID().equals( polyGeometry.getSRID() )) ) {
            // Return null if the CRS of all points are not the same.
            return null;
        }

        return new PolyBoolean( g.coveredBy( polyGeometry ) );
    }


    private static PolyGeometry createBbox( PolyPoint lowerLeft, PolyPoint upperRight ) {
        Coordinate bottomLeft = new Coordinate( lowerLeft.getX(), lowerLeft.getY() );
        Coordinate topRight = new Coordinate( upperRight.getX(), upperRight.getY() );
        Coordinate topLeft = new Coordinate( bottomLeft.x, topRight.y );
        Coordinate bottomRight = new Coordinate( topRight.x, bottomLeft.y );
        // Form a closed Ring, starting on the bottom left and going clockwise.
        Coordinate[] linearRing = new Coordinate[]{
                bottomLeft,
                topLeft,
                topRight,
                bottomRight,
                bottomLeft
        };
        GeometryFactory geoFactory = new GeometryFactory();
        return new PolyGeometry( geoFactory.createPolygon( linearRing ) );
    }


    private static double convertPolyValueToDouble( PolyValue value ) {
        if ( value.isNumber() ) {
            return value.asNumber().doubleValue();
        }
        if ( value.isString() ) {
            // Existing normalized graph properties may have decimal coordinates stored as text.
            return Double.parseDouble( value.asString().value );
        }
        throw new GenericRuntimeException( "Point coordinates must be numeric." );
    }

}
