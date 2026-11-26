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
import java.math.BigInteger;
import java.math.MathContext;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.EqualityComparer;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function1;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.runtime.SqlStateException;
import org.polypheny.db.languages.CypherTableProcedureProvider.Field;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyNull;
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
import org.polypheny.db.type.entity.temporal.PolyCypherTemporal;
import org.polypheny.db.util.Pair;


@Deterministic
@Slf4j
public class CypherFunctions {

    private CypherFunctions() {
        // empty on purpose
    }


    public static Enumerable<PolyGraph> graphValue( PolyGraph graph ) {
        return Linq4j.singletonEnumerable( graph );
    }


    public static PolyBoolean patternExists( PolyGraph graph, PolyPath template, PolyValue bindings,
            PolyString[] propertyBindings, Function0<PolyValue>[] properties ) {
        return PolyBoolean.of( matchingPaths( graph, template, bindings, propertyBindings, properties ).findAny().isPresent() );
    }


    public static PolyList<PolyPath> patternPaths( PolyGraph graph, PolyPath template, PolyValue bindings,
            PolyString[] propertyBindings, Function0<PolyValue>[] properties ) {
        return PolyList.copyOf( matchingPaths( graph, template, bindings, propertyBindings, properties ).toList() );
    }


    private static Stream<PolyPath> matchingPaths( PolyGraph graph, PolyPath template, PolyValue bindings,
            PolyString[] propertyBindings, Function0<PolyValue>[] properties ) {
        Map<PolyValue, PolyValue> known = bindings.asMap();
        List<PolyNode> nodes = template.getNodes();
        for ( PolyNode node : nodes ) {
            validateMatchNode( known.get( node.getVariableName() ), PolyBoolean.TRUE );
        }
        for ( PolyEdge relationship : template.getEdges() ) {
            if ( relationship.fromTo() != null ) {
                validateMatchRelationshipList( known.get( relationship.getVariableName() ), PolyBoolean.TRUE );
            }
        }
        if ( known.values().stream().anyMatch( CypherFunctions::isNull ) ) {
            return Stream.empty();
        }
        PolyValue first = known.get( nodes.get( 0 ).getVariableName() );
        PolyValue last = known.get( nodes.get( nodes.size() - 1 ).getVariableName() );
        PolyString start = first instanceof PolyNode source ? source.id : null;
        PolyString end = last instanceof PolyNode target ? target.id : null;
        return StreamSupport.stream( graph.matchPaths( template, start, end ).spliterator(), false )
                .filter( match -> known.entrySet().stream().allMatch(
                        binding -> PolyBoolean.TRUE.equals( equal( match.getBinding( binding.getKey().asString() ), binding.getValue() ) ) ) )
                .filter( match -> matchesPatternProperties( match, propertyBindings, properties ) );
    }


    private static boolean matchesPatternProperties( PolyPath match, PolyString[] propertyBindings, Function0<PolyValue>[] properties ) {
        for ( int i = 0; i < properties.length; i++ ) {
            // Preserve the property checks for each occurrence of a repeated node variable.
            PolyValue candidate = match.getBinding( propertyBindings[i] );
            if ( !matchProperties( candidate, properties[i] ).value ) {
                return false;
            }
        }
        return true;
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


    public static Enumerable<PolyPath> shortestPathMatch( PolyGraph graph, PolyPath pattern ) {
        return Linq4j.asEnumerable( graph.matchShortestPaths( pattern ) );
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
     * Tests whether a node has all requested labels.
     *
     * @param value the target node or null
     * @param labels a single label or a list of labels to check
     */
    @SuppressWarnings("unused")
    public static PolyBoolean hasLabel( PolyValue value, PolyValue labels ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( !(value instanceof PolyNode node) ) {
            throw new GenericRuntimeException( "Cypher label predicates require a NODE, got %s.", value.getType() );
        }
        node.requireReadable();
        if ( labels instanceof PolyList<?> list ) {
            return PolyBoolean.of( node.labels.containsAll( list ) );
        }
        return PolyBoolean.of( node.labels.contains( labels.asString() ) );
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
    public static PolyValue extractProperty( PolyValue holder, PolyString key ) {
        if ( isNull( holder ) || isNull( key ) ) {
            return null;
        }
        PolyCypherTemporal temporal = PolyCypherTemporal.from( holder );
        if ( temporal != null ) {
            return temporal.kind == PolyCypherTemporal.Kind.DURATION
                    ? CypherDurations.component( temporal, key.value )
                    : CypherTemporalInstants.component( temporal, key.value );
        }
        PolyValue value;
        if ( holder instanceof GraphPropertyHolder element ) {
            element.requireReadable();
            value = element.getProperties().get( key );
        } else if ( holder.isMap() ) {
            value = holder.asMap().get( key );
        } else {
            throw new GenericRuntimeException( "Property access requires a map, node, relationship, or temporal value." );
        }
        return isNull( value ) ? null : value;
    }


    public static PolyBoolean propertyExists( PolyValue holder, PolyString key ) {
        if ( isNull( holder ) ) {
            return PolyBoolean.FALSE;
        }
        if ( !(holder instanceof GraphPropertyHolder) && !holder.isMap() ) {
            throw new GenericRuntimeException( "exists() requires a property of a map, node, or relationship." );
        }
        return PolyBoolean.of( !isNull( extractProperty( holder, key ) ) );
    }


    @SuppressWarnings("unused")
    public static PolyValue extractProperties( GraphPropertyHolder holder ) {
        if ( holder == null ) {
            return null;
        }
        holder.requireReadable();
        return holder.getProperties();
    }


    public static PolyList<PolyString> keys( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof GraphPropertyHolder element ) {
            element.requireReadable();
            return PolyList.copyOf( element.getProperties().entrySet().stream()
                    .filter( entry -> !isNull( entry.getValue() ) ).map( Map.Entry::getKey ).toList() );
        }
        return PolyList.copyOf( requirePropertyMap( value, "keys" ).keySet().stream().map( PolyValue::asString ).toList() );
    }


    public static PolyMap<?, ?> properties( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof GraphPropertyHolder element ) {
            element.requireReadable();
            PolyDictionary result = new PolyDictionary();
            element.getProperties().forEach( (key, property) -> {
                if ( !isNull( property ) ) {
                    result.put( key, property );
                }
            } );
            return result;
        }
        return requirePropertyMap( value, "properties" );
    }


    private static PolyMap<PolyValue, PolyValue> requirePropertyMap( PolyValue value, String function ) {
        if ( !value.isMap() ) {
            throw new GenericRuntimeException( "%s() requires a node, relationship, or map.", function );
        }
        return value.asMap();
    }


    public static PolyNode startNode( PolyGraph graph, PolyValue value ) {
        return relationshipEndpoint( graph, value, true );
    }


    public static PolyNode endNode( PolyGraph graph, PolyValue value ) {
        return relationshipEndpoint( graph, value, false );
    }


    private static PolyNode relationshipEndpoint( PolyGraph graph, PolyValue value, boolean start ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( !(value instanceof PolyEdge edge) ) {
            throw new GenericRuntimeException( "%s() requires a relationship.", start ? "startNode" : "endNode" );
        }
        boolean left = start != (edge.getDirection() == EdgeDirection.RIGHT_TO_LEFT);
        return graph.getNodes().get( left ? edge.getLeft() : edge.getRight() );
    }


    @SuppressWarnings("unused")
    public static PolyString extractId( GraphPropertyHolder holder ) {
        return holder == null ? null : holder.getId();
    }


    public static PolyString matchElementId( PolyValue value, PolyString expected ) {
        if ( value instanceof GraphPropertyHolder element && expected.value.equals( element.getType().name() ) ) {
            return element.getId();
        }
        // Dynamic node and relationship-list bindings are checked before matching.
        return null;
    }


    public static PolyPath visibleMatchPath( PolyPath path, PolyList<PolyString> hiddenBindings ) {
        return path.withoutBindings( Set.copyOf( hiddenBindings ) );
    }


    public static PolyBoolean matchProperties( PolyValue candidate, Function0<PolyValue> properties ) {
        if ( candidate instanceof GraphPropertyHolder element ) {
            return PolyBoolean.of( element.matchesProperties( properties.apply().asMap().asDictionary() ) );
        }
        if ( candidate instanceof PolyList<?> relationships ) {
            for ( PolyValue relationship : relationships ) {
                if ( !relationship.asEdge().matchesProperties( properties.apply().asMap().asDictionary() ) ) {
                    return PolyBoolean.FALSE;
                }
            }
            return PolyBoolean.TRUE;
        }
        return PolyBoolean.FALSE;
    }


    public static PolyBoolean booleanPredicate( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof PolyBoolean predicate ) {
            return predicate;
        }
        throw new GenericRuntimeException( "Cypher predicates require a BOOLEAN, got %s.", value.getType() );
    }


    public static PolyBoolean quantify( OperatorName operator, PolyValue value, Function1<PolyValue, PolyBoolean> predicate ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( !(value instanceof PolyList<?> items) ) {
            throw new GenericRuntimeException( "Cypher quantified predicates require a list, got %s.", value.getType() );
        }
        boolean unknown = false;
        int matches = 0;
        for ( PolyValue item : items ) {
            PolyBoolean accepted = predicate.apply( isNull( item ) ? null : item );
            if ( isNull( accepted ) ) {
                unknown = true;
            } else if ( accepted.value ) {
                switch ( operator ) {
                    case CYPHER_ANY_MATCH -> {
                        return PolyBoolean.TRUE;
                    }
                    case CYPHER_NONE_MATCH -> {
                        return PolyBoolean.FALSE;
                    }
                    case CYPHER_SINGLE_MATCH -> {
                        if ( ++matches > 1 ) {
                            return PolyBoolean.FALSE;
                        }
                    }
                    default -> {
                        // ALL continues until an item fails its predicate.
                    }
                }
            } else if ( operator == OperatorName.CYPHER_ALL_MATCH ) {
                return PolyBoolean.FALSE;
            }
        }
        if ( unknown ) {
            return null;
        }
        return switch ( operator ) {
            case CYPHER_ALL_MATCH, CYPHER_NONE_MATCH -> PolyBoolean.TRUE;
            case CYPHER_ANY_MATCH -> PolyBoolean.FALSE;
            case CYPHER_SINGLE_MATCH -> PolyBoolean.of( matches == 1 );
            default -> throw new IllegalArgumentException( "Unsupported Cypher quantifier " + operator );
        };
    }


    public static PolyBoolean matchWithWhere( PolyBoolean patternPredicate, PolyBoolean wherePredicate ) {
        return PolyBoolean.of( PolyBoolean.TRUE.equals( patternPredicate ) && PolyBoolean.TRUE.equals( wherePredicate ) );
    }


    public static PolyList<PolyValue> listComprehension( PolyValue value, Function1<PolyValue, PolyBoolean> predicate, Function1<PolyValue, PolyValue> projection ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( !(value instanceof PolyList<?> items) ) {
            throw new GenericRuntimeException( "List comprehension requires a list, got %s.", value.getType() );
        }
        List<PolyValue> result = new ArrayList<>();
        for ( PolyValue item : items ) {
            PolyValue element = isNull( item ) ? null : item;
            if ( PolyBoolean.TRUE.equals( predicate.apply( element ) ) ) {
                result.add( projection.apply( element ) );
            }
        }
        return PolyList.copyOf( result );
    }


    public static PolyBoolean validateMatchNode( PolyValue value, PolyBoolean reportError ) {
        boolean compatible = isNull( value ) || value instanceof PolyNode;
        if ( !compatible && reportError.value ) {
            throw new GenericRuntimeException( "MATCH requires a NODE binding, got %s.", value.getType() );
        }
        return PolyBoolean.of( compatible );
    }


    public static PolyValue validateProcedureArgument( PolyValue value, PolyString name, PolyString type, PolyBoolean nullable ) {
        PolyType actual = isNull( value ) ? PolyType.NULL : value.getType();
        if ( !Field.accepts( PolyType.valueOf( type.value ), nullable.value, actual ) ) {
            throw new SqlStateException( "InvalidArgumentType: Procedure argument " + name.value
                    + " requires " + type.value + ", got " + actual + ".", "22023" );
        }
        return value;
    }


    public static PolyBoolean evaluateProcedureArguments( PolyBoolean retainRow, PolyValue[] arguments ) {
        // Java evaluates arguments before entering this method, including calls that produce no output.
        return retainRow;
    }


    private static GraphPropertyHolder requireGraphUpdateTarget( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof GraphPropertyHolder target ) {
            target.requireReadable();
            return target;
        }
        throw new GenericRuntimeException( "Graph writes require a node or relationship binding, got %s.", value.getType() );
    }


    public static PolyBoolean validateMatchRelationshipList( PolyValue value, PolyBoolean reportError ) {
        // Neo4j 4.4 lets dynamically typed strings reach the comparison, where they match no relationship list.
        boolean compatible = isNull( value ) || value.isList() || value.isString();
        if ( !compatible && reportError.value ) {
            throw new GenericRuntimeException( "MATCH requires a relationship list binding, got %s.", value.getType() );
        }
        return PolyBoolean.of( compatible );
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
        if ( holder == null ) {
            return null;
        }
        holder.requireReadable();
        return holder.getLabels();
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


    public static PolyEdge createEdge( PolyEdge template, PolyString rowId, PolyValue source, PolyValue target,
            List<PolyString> keys, List<PolyValue> values ) {
        if ( isNull( source ) || isNull( target ) ) {
            throw new GenericRuntimeException( "CREATE requires a bound source and target node" );
        }
        if ( !(source instanceof PolyNode sourceNode) || !(target instanceof PolyNode targetNode) ) {
            throw new GenericRuntimeException( "CREATE requires node bindings for the source and target, got %s and %s.",
                    source.getType(), target.getType() );
        }
        return new PolyEdge( createdId( template.id, rowId ), properties( keys, values ), template.labels, sourceNode.id, targetNode.id,
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
            if ( !isNull( value ) ) {
                validatePropertyValue( value );
                properties.put( keys.get( i ), value );
            }
        }
        return properties;
    }


    /**
     * Sets a property value for a given key.
     * If the key does not exist it gets created, if it does, it gets replaced.
     *
     * @param targetValue the graph element to modify
     * @param key the key of the property to replace
     * @param value the value to replace it by
     * @return the adjusted graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperty( PolyValue targetValue, PolyString key, PolyValue value ) {
        GraphPropertyHolder target = requireGraphUpdateTarget( targetValue );
        if ( target == null ) {
            return null;
        }
        validatePropertyValue( value );
        assignProperty( target, key, value );
        return target;
    }


    private static void assignProperty( GraphPropertyHolder target, PolyString key, PolyValue value ) {
        if ( isNull( value ) ) {
            target.properties.remove( key );
        } else {
            target.properties.put( key, value );
        }
    }


    /**
     * Sets the given labels on a node.
     *
     * @param targetValue the node to modify
     * @param labels the labels to set
     * @param replace whether to clear the existing labels before adding the new labels
     * @return the modified node
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setLabels( PolyValue targetValue, List<PolyString> labels, PolyBoolean replace ) {
        if ( isNull( targetValue ) ) {
            return null;
        }
        if ( !(targetValue instanceof PolyNode target) ) {
            throw new GenericRuntimeException( "SET label assignment requires a node binding" );
        }
        target.requireReadable();
        if ( replace.value ) {
            target.labels.clear();
        }
        target.setLabels( PolyList.of( labels ) );
        return target;
    }


    /**
     * Sets all given properties for the given graph element.
     *
     * @param targetValue the graph element to modify
     * @param source the properties to copy
     * @param replace if the properties are replaced completely or only added and replaced
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder setProperties( PolyValue targetValue, PolyValue source, PolyBoolean replace ) {
        GraphPropertyHolder target = requireGraphUpdateTarget( targetValue );
        if ( target == null ) {
            return null;
        }

        Map<? extends PolyValue, ? extends PolyValue> sourceProperties;
        if ( source instanceof GraphPropertyHolder element ) {
            element.requireReadable();
            sourceProperties = element.properties;
        } else if ( source instanceof PolyMap<?, ?> map && !map.isNull() ) {
            sourceProperties = map;
        } else {
            throw new GenericRuntimeException( "SET requires a map, node, or relationship as the source of properties." );
        }

        // Validate the whole assignment and retain the source when it is the target itself.
        Map<PolyString, PolyValue> properties = new HashMap<>();
        sourceProperties.forEach( (key, value) -> {
            if ( !(key instanceof PolyString name) || name.isNull() ) {
                throw new GenericRuntimeException( "SET requires a string property name." );
            }
            validatePropertyValue( value );
            properties.put( name, value );
        } );
        if ( replace.value ) {
            target.properties.clear();
        }
        properties.forEach( (key, value) -> assignProperty( target, key, value ) );
        return target;
    }


    public static void validatePropertyValue( PolyValue value ) {
        if ( isNull( value ) || propertyScalarType( value ) != null ) {
            return;
        }
        if ( value instanceof PolyList<?> list ) {
            Object elementType = null;
            for ( PolyValue element : list ) {
                Object candidate = isNull( element ) ? null : propertyScalarType( element );
                if ( candidate == null || elementType != null && elementType != candidate ) {
                    throw invalidPropertyType();
                }
                elementType = candidate;
            }
            return;
        }
        throw invalidPropertyType();
    }


    private static Object propertyScalarType( PolyValue value ) {
        if ( value instanceof PolyCypherTemporal temporal ) {
            return temporal.getKind();
        }
        PolyType type = value.getType();
        return switch ( type ) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, REAL, DOUBLE, DECIMAL -> PolyType.DECIMAL;
            case CHAR, VARCHAR, TEXT -> PolyType.VARCHAR;
            case BINARY, VARBINARY -> PolyType.VARBINARY;
            case DATE -> PolyCypherTemporal.Kind.DATE;
            case TIME -> PolyCypherTemporal.Kind.LOCAL_TIME;
            case TIMESTAMP -> PolyCypherTemporal.Kind.LOCAL_DATE_TIME;
            case INTERVAL -> PolyCypherTemporal.Kind.DURATION;
            case BOOLEAN, GEOMETRY, FILE, IMAGE, VIDEO, AUDIO -> type;
            default -> null;
        };
    }


    private static GenericRuntimeException invalidPropertyType() {
        return new GenericRuntimeException( "InvalidPropertyType: Graph properties require a scalar or a homogeneous list of non-null scalars." );
    }


    /**
     * Removes the given labels for graph object.
     *
     * @param target the graph element to modify
     * @param labels the labels to remove
     * @return the modified graph element
     */
    @SuppressWarnings("unused")
    public static GraphPropertyHolder removeLabels( PolyValue targetValue, List<PolyString> labels ) {
        GraphPropertyHolder target = requireGraphUpdateTarget( targetValue );
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
    public static GraphPropertyHolder removeProperty( PolyValue targetValue, PolyString key ) {
        GraphPropertyHolder target = requireGraphUpdateTarget( targetValue );
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
        PolyCypherTemporal leftTemporal = PolyCypherTemporal.from( left );
        PolyCypherTemporal rightTemporal = PolyCypherTemporal.from( right );
        if ( leftTemporal != null || rightTemporal != null ) {
            return PolyBoolean.of( Objects.equals( leftTemporal, rightTemporal ) );
        }
        if ( left instanceof PolyNumber a && right instanceof PolyNumber b ) {
            return PolyBoolean.of( !Double.isNaN( a.doubleValue() ) && !Double.isNaN( b.doubleValue() )
                    && compareNumbers( a, b ) == 0 );
        }
        if ( left instanceof GraphObject && right instanceof GraphObject ) {
            return PolyBoolean.of( aggregationKey( left ).equals( aggregationKey( right ) ) );
        }
        if ( left instanceof PolyList<?> leftList && right instanceof PolyList<?> rightList ) {
            return equalLists( leftList, rightList );
        }
        if ( left.isMap() && right.isMap() ) {
            return equalMaps( left.asMap(), right.asMap() );
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


    private static PolyBoolean equalMaps( Map<PolyValue, PolyValue> left, Map<PolyValue, PolyValue> right ) {
        if ( !left.keySet().equals( right.keySet() ) ) {
            return PolyBoolean.FALSE;
        }
        boolean hasUnknown = false;
        for ( Map.Entry<PolyValue, PolyValue> entry : left.entrySet() ) {
            PolyBoolean equal = equal( entry.getValue(), right.get( entry.getKey() ) );
            if ( PolyBoolean.FALSE.equals( equal ) ) {
                return PolyBoolean.FALSE;
            }
            hasUnknown |= equal == null;
        }
        return hasUnknown ? null : PolyBoolean.TRUE;
    }


    private static boolean isNull( PolyValue value ) {
        return value == null || value.isNull();
    }


    public static void addCollectedValue( PolyList<PolyValue> values, PolyValue value ) {
        if ( !isNull( value ) ) {
            values.add( value );
        }
    }


    public static void addDistinctCollectedValue( PolyList<PolyValue> values, Set<Object> seen, PolyValue value ) {
        if ( !isNull( value ) && seen.add( aggregationKey( value ) ) ) {
            values.add( value );
        }
    }


    public static PolyValue percentile( PolyValue pairs, PolyValue fallback, PolyBoolean continuous, PolyBoolean distinct ) {
        PolyList<?> rows = pairs.asList();
        if ( rows.isEmpty() ) {
            if ( !isNull( fallback ) ) {
                percentileFraction( fallback );
            }
            return null;
        }

        double fraction = percentileFraction( rows.get( 0 ).asList().get( 1 ) );
        List<PolyNumber> values = new ArrayList<>();
        Set<Object> seen = distinct.value ? new HashSet<>() : null;
        for ( PolyValue row : rows ) {
            PolyValue value = row.asList().get( 0 );
            if ( isNull( value ) ) {
                continue;
            }
            if ( !(value instanceof PolyNumber number) ) {
                throw new SqlStateException( "InvalidArgumentValue: Percentile functions require numeric values.", "22023" );
            }
            if ( seen == null || seen.add( aggregationKey( number ) ) ) {
                values.add( number );
            }
        }
        if ( values.isEmpty() ) {
            return null;
        }

        values.sort( CypherFunctions::compareNumbers );
        if ( !continuous.value ) {
            return values.get( Math.max( 0, (int) Math.ceil( fraction * values.size() ) - 1 ) );
        }
        double rank = fraction * (values.size() - 1);
        int low = (int) rank;
        int high = (int) Math.ceil( rank );
        if ( low == high ) {
            return values.get( low );
        }
        double weight = rank - low;
        return PolyDouble.of( (1 - weight) * values.get( low ).doubleValue() + weight * values.get( high ).doubleValue() );
    }


    private static double percentileFraction( PolyValue value ) {
        if ( value instanceof PolyNumber number && !isNull( number ) ) {
            double fraction = number.doubleValue();
            if ( fraction >= 0 && fraction <= 1 ) {
                return fraction;
            }
        }
        throw new SqlStateException( "NumberOutOfRange: A percentile must be a number between 0 and 1.", "22003" );
    }


    public static PolyValue index( PolyValue subject, PolyValue index ) {
        if ( isNull( subject ) || isNull( index ) ) {
            return null;
        }
        if ( subject.isList() ) {
            PolyList<?> list = subject.asList();
            long offset = listOffset( index, list.size() );
            PolyValue value = offset < 0 || offset >= list.size() ? null : list.get( (int) offset );
            return isNull( value ) ? null : value;
        }
        if ( subject.isMap() || subject instanceof GraphPropertyHolder ) {
            if ( !index.isString() ) {
                throw new SqlStateException( "MapElementAccessByNonString: A map or property key must be a string.", "2202E" );
            }
            return extractProperty( subject, index.asString() );
        }
        throw new SqlStateException( "InvalidArgumentType: Indexing requires a list, map, node, or relationship.", "2202E" );
    }


    public static PolyList<?> slice( PolyValue subject, PolyValue from, PolyValue to ) {
        if ( isNull( subject ) || isNull( from ) || isNull( to ) ) {
            return null;
        }
        if ( !subject.isList() ) {
            throw new GenericRuntimeException( "Slicing requires a list." );
        }
        PolyList<?> list = subject.asList();
        int start = (int) Math.max( 0, Math.min( listOffset( from, list.size() ), list.size() ) );
        int end = (int) Math.max( start, Math.min( listOffset( to, list.size() ), list.size() ) );
        return PolyList.copyOf( list.subList( start, end ) );
    }


    private static long listOffset( PolyValue index, int length ) {
        if ( !PolyType.INT_TYPES.contains( index.getType() ) ) {
            throw new SqlStateException( "InvalidArgumentType: A list index or slice bound must be an integer.", "2202E" );
        }
        long offset = index.asNumber().longValue();
        return offset < 0 ? length + offset : offset;
    }


    public static PolyDictionary constructMap( PolyValue[] entries ) {
        Map<PolyString, PolyValue> values = new HashMap<>();
        for ( int i = 0; i < entries.length; i += 2 ) {
            PolyValue value = entries[i + 1];
            values.put( entries[i].asString(), value == null ? PolyNull.NULL : value );
        }
        return new PolyDictionary( values );
    }


    public static PolyList<PolyNode> nodes( PolyValue value ) {
        return isNull( value ) ? null : requirePath( value ).getNodes();
    }


    public static PolyList<PolyEdge> relationships( PolyValue value ) {
        return isNull( value ) ? null : requirePath( value ).getEdges();
    }


    public static PolyPath shortestPathValue( PolyGraph graph, PolyPath template, PolyValue start, PolyValue end, PolyValue relationship,
            Function0<PolyValue> startProperties, Function0<PolyValue> relationshipProperties, Function0<PolyValue> endProperties ) {
        if ( isNull( start ) || isNull( end ) ) {
            return null;
        }
        if ( !(start instanceof PolyNode source) || !(end instanceof PolyNode target) ) {
            throw new GenericRuntimeException( "shortestPath() requires node values for both endpoints." );
        }
        PolyNode left = template.getNodes().get( 0 );
        PolyNode right = template.getNodes().get( 1 );
        PolyEdge edge = template.getEdges().get( 0 );
        PolyString name = edge.getVariableName();
        boolean boundRelationship = name != null && !name.isNull();
        if ( boundRelationship ) {
            if ( isNull( relationship ) ) {
                return null;
            }
            boolean compatible = edge.fromTo() == null ? relationship instanceof PolyEdge
                    : relationship instanceof PolyList<?> edges && edges.stream().allMatch( PolyEdge.class::isInstance );
            if ( !compatible ) {
                throw new GenericRuntimeException( "shortestPath() requires a relationship or relationship-list binding matching the pattern." );
            }
        }
        Predicate<PolyPath> boundPath = boundRelationship
                ? path -> PolyBoolean.TRUE.equals( equal( path.getBinding( name ), relationship ) )
                : null;
        if ( startProperties == null && relationshipProperties == null && endProperties == null ) {
            return graph.shortestPath( template, source, target, boundPath );
        }
        // A path predicate or a larger minimum can require enumerating trails. Stop at the first
        // admissible trail in these cases and find the shortest path only after applying the maps.
        boolean breadthFirst = boundPath == null && edge.getMinLength() <= 1;
        PolyPath candidate = breadthFirst
                ? graph.shortestPath( template, source, target )
                : graph.firstPath( template, source, target, boundPath );
        if ( candidate == null ) {
            return null;
        }

        // Every candidate shares these endpoints. Match against the nodes from the current graph.
        List<PolyNode> nodes = candidate.getNodes();
        if ( (startProperties != null && !nodes.get( 0 ).matchesProperties( shortestProperties( startProperties.apply() ) ))
                || (endProperties != null && !nodes.get( nodes.size() - 1 ).matchesProperties( shortestProperties( endProperties.apply() ) )) ) {
            return null;
        }
        // A bound relationship or list fixes the edges and length of any admissible path.
        boolean shortest = breadthFirst || boundPath != null || candidate.getEdges().size() == edge.getMinLength();
        if ( relationshipProperties == null || candidate.getEdges().isEmpty() ) {
            return shortest ? candidate : graph.shortestPath( template, source, target );
        }
        PolyDictionary properties = shortestProperties( relationshipProperties.apply() );
        if ( shortest ) {
            if ( candidate.getEdges().stream().allMatch( actual -> actual.matchesProperties( properties ) ) ) {
                return candidate;
            }
            if ( boundPath != null ) {
                return null;
            }
        }

        // Filter edges during traversal and retain any literal constraints on the template.
        PolyDictionary combinedProperties = new PolyDictionary( edge.properties );
        combinedProperties.putAll( properties );
        PolyEdge edgePattern = new PolyEdge( edge.id, combinedProperties, edge.labels, edge.left, edge.right, edge.direction, name );
        edgePattern.fromTo( edge.fromTo() );
        PolyPath pattern = PolyPath.create( List.of( Pair.of( left.getVariableName(), left ), Pair.of( right.getVariableName(), right ) ),
                List.of( Pair.of( name, edgePattern ) ) );
        return graph.shortestPath( pattern, source, target );
    }


    private static PolyDictionary shortestProperties( PolyValue value ) {
        if ( isNull( value ) ) {
            return new PolyDictionary();
        }
        if ( !value.isMap() ) {
            throw new GenericRuntimeException( "shortestPath() pattern properties require a map." );
        }
        return value.asMap().asDictionary();
    }


    public static PolyLong pathLength( PolyValue value ) {
        return isNull( value ) ? null : PolyLong.of( requirePath( value ).getEdges().size() );
    }


    public static PolyLong size( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value.isList() ) {
            return PolyLong.of( value.asList().size() );
        }
        if ( value.isString() ) {
            String string = value.asString().value;
            return PolyLong.of( string.codePointCount( 0, string.length() ) );
        }
        throw new GenericRuntimeException( "size() requires a list or string." );
    }


    public static PolyList<PolyLong> range( PolyValue start, PolyValue end, PolyValue increment ) {
        if ( isNull( start ) || isNull( end ) || isNull( increment ) ) {
            return null;
        }
        if ( !PolyType.INT_TYPES.contains( start.getType() )
                || !PolyType.INT_TYPES.contains( end.getType() )
                || !PolyType.INT_TYPES.contains( increment.getType() ) ) {
            throw new SqlStateException( "InvalidArgumentType: range() requires integer arguments.", "22023" );
        }
        long first = start.asNumber().longValue();
        long last = end.asNumber().longValue();
        long step = increment.asNumber().longValue();
        if ( step == 0 ) {
            throw new SqlStateException( "NumberOutOfRange: range() requires a non-zero step.", "22003" );
        }
        PolyList<PolyLong> values = new PolyList<>();
        if ( (step > 0 && first > last) || (step < 0 && first < last) ) {
            return values;
        }
        // Either subtracting the endpoints or taking the absolute step may overflow a long.
        BigInteger count = BigInteger.valueOf( last ).subtract( BigInteger.valueOf( first ) )
                .divide( BigInteger.valueOf( step ) ).add( BigInteger.ONE );
        if ( count.compareTo( BigInteger.valueOf( Integer.MAX_VALUE - 8L ) ) > 0 ) {
            throw new SqlStateException( "NumberOutOfRange: range() result is too large to represent as a list.", "22003" );
        }
        int length = count.intValue();
        long next = first;
        for ( int i = 1; i < length; i++ ) {
            values.add( PolyLong.of( next ) );
            next += step;
        }
        values.add( PolyLong.of( next ) );
        return values;
    }


    public static PolyString relationshipType( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof PolyEdge edge ) {
            return edge.getLabels().isEmpty() ? null : edge.getLabels().get( 0 );
        }
        throw new GenericRuntimeException( "type() requires a relationship." );
    }


    /** Returns the persisted graph identity also used in graph values exposed by Prism. */
    public static PolyString entityId( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof GraphPropertyHolder entity ) {
            return entity.getId();
        }
        throw new GenericRuntimeException( "id() requires a node or relationship." );
    }


    public static PolyList<PolyString> nodeLabels( PolyValue value ) {
        if ( isNull( value ) ) {
            return null;
        }
        if ( value instanceof PolyNode node ) {
            node.requireReadable();
            return node.getLabels();
        }
        throw new GenericRuntimeException( "labels() requires a node." );
    }


    private static PolyPath requirePath( PolyValue value ) {
        if ( value instanceof PolyPath path ) {
            return path;
        }
        throw new GenericRuntimeException( "The function requires a path." );
    }


    public static PolyValue arithmetic( PolyString operation, PolyValue left, PolyValue right ) {
        if ( isNull( left ) || isNull( right ) ) {
            return null;
        }
        OperatorName op = OperatorName.valueOf( operation.value );
        if ( op == OperatorName.PLUS && (left.isList() || right.isList()) ) {
            List<PolyValue> values = new ArrayList<>( left.isList() ? left.asList() : List.of( left ) );
            values.addAll( right.isList() ? right.asList() : List.of( right ) );
            return PolyList.copyOf( values );
        }
        PolyCypherTemporal leftTemporal = PolyCypherTemporal.from( left );
        PolyCypherTemporal rightTemporal = PolyCypherTemporal.from( right );
        if ( leftTemporal != null || rightTemporal != null ) {
            return temporalArithmetic( op, left, right, leftTemporal, rightTemporal );
        }
        if ( op == OperatorName.PLUS
                && ((left.isString() && (right.isString() || right.isNumber())) || (right.isString() && left.isNumber())) ) {
            return PolyString.of( left.toString() + right.toString() );
        }
        if ( !left.isNumber() || !right.isNumber() ) {
            throw new GenericRuntimeException( "Cypher arithmetic requires numeric operands." );
        }
        PolyNumber a = left.asNumber();
        PolyNumber b = right.asNumber();
        if ( op == OperatorName.POWER || PolyType.APPROX_TYPES.contains( left.getType() ) || PolyType.APPROX_TYPES.contains( right.getType() ) ) {
            double x = a.doubleValue();
            double y = b.doubleValue();
            return PolyDouble.of( switch ( op ) {
                case PLUS -> x + y;
                case MINUS -> x - y;
                case MULTIPLY -> x * y;
                case DIVIDE -> x / y;
                case MOD -> x % y;
                case POWER -> Math.pow( x, y );
                case UNARY_PLUS -> y;
                case UNARY_MINUS -> -y;
                default -> throw new IllegalArgumentException( "Unknown Cypher arithmetic operator " + op );
            } );
        }
        if ( left.getType() == PolyType.DECIMAL || right.getType() == PolyType.DECIMAL ) {
            BigDecimal x = a.bigDecimalValue();
            BigDecimal y = b.bigDecimalValue();
            return PolyBigDecimal.of( switch ( op ) {
                case PLUS -> x.add( y );
                case MINUS -> x.subtract( y );
                case MULTIPLY -> x.multiply( y );
                case DIVIDE -> x.divide( y, MathContext.DECIMAL128 );
                case MOD -> x.remainder( y );
                case UNARY_PLUS -> y;
                case UNARY_MINUS -> y.negate();
                default -> throw new IllegalArgumentException( "Unknown Cypher arithmetic operator " + op );
            } );
        }
        long x = a.longValue();
        long y = b.longValue();
        return PolyLong.of( switch ( op ) {
            case PLUS -> Math.addExact( x, y );
            case MINUS -> Math.subtractExact( x, y );
            case MULTIPLY -> Math.multiplyExact( x, y );
            case DIVIDE -> {
                if ( x == Long.MIN_VALUE && y == -1 ) {
                    throw new ArithmeticException( "Integer overflow in division." );
                }
                yield x / y;
            }
            case MOD -> x % y;
            case UNARY_PLUS -> y;
            case UNARY_MINUS -> Math.negateExact( y );
            default -> throw new IllegalArgumentException( "Unknown Cypher arithmetic operator " + op );
        } );
    }


    private static PolyCypherTemporal temporalArithmetic(
            OperatorName operation, PolyValue left, PolyValue right, PolyCypherTemporal leftTemporal, PolyCypherTemporal rightTemporal ) {
        if ( leftTemporal != null && rightTemporal != null ) {
            if ( operation == OperatorName.PLUS ) {
                return CypherDurations.add( leftTemporal, rightTemporal );
            }
            if ( operation == OperatorName.MINUS ) {
                return CypherDurations.subtract( leftTemporal, rightTemporal );
            }
        } else if ( (operation == OperatorName.MULTIPLY || operation == OperatorName.DIVIDE)
                && leftTemporal != null && right instanceof PolyNumber factor ) {
            return CypherDurations.scale( leftTemporal, factor, operation == OperatorName.DIVIDE );
        } else if ( operation == OperatorName.MULTIPLY && left instanceof PolyNumber factor && rightTemporal != null ) {
            return CypherDurations.scale( rightTemporal, factor, false );
        }
        throw new GenericRuntimeException( "Invalid operands for temporal arithmetic." );
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
        if ( value instanceof PolyValue poly ) {
            if ( poly.isNull() ) {
                return null;
            }
            PolyCypherTemporal temporal = PolyCypherTemporal.from( poly );
            if ( temporal != null ) {
                return temporal;
            }
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
        if ( value instanceof PolyMap<?, ?> map && map.isMap() ) {
            Map<PolyValue, Object> entries = new HashMap<>();
            map.forEach( (key, element) -> entries.put( key, aggregationKey( element ) ) );
            return entries;
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


    public static Enumerable<PolyValue[]> distinctRows( Enumerable<PolyValue[]> rows ) {
        EqualityComparer<Object> comparer = groupingComparer( true );
        return rows.distinct( new EqualityComparer<>() {
            @Override
            public boolean equal( PolyValue[] left, PolyValue[] right ) {
                return comparer.equal( Arrays.asList( left ), Arrays.asList( right ) );
            }


            @Override
            public int hashCode( PolyValue[] row ) {
                return comparer.hashCode( Arrays.asList( row ) );
            }
        } );
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
        PolyCypherTemporal leftTemporal = PolyCypherTemporal.from( left );
        PolyCypherTemporal rightTemporal = PolyCypherTemporal.from( right );
        if ( leftTemporal != null && rightTemporal != null ) {
            if ( leftTemporal.kind == PolyCypherTemporal.Kind.DURATION && rightTemporal.kind == PolyCypherTemporal.Kind.DURATION ) {
                return CypherDurations.compareForOrdering( leftTemporal, rightTemporal );
            }
            return leftTemporal.compareTo( rightTemporal );
        }
        if ( leftTemporal == null && rightTemporal == null && left instanceof PolyNumber a && right instanceof PolyNumber b ) {
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
        if ( left.isMap() && right.isMap() ) {
            return compareMaps( left.asMap(), right.asMap(), false );
        }
        return left.compareTo( right );
    }


    private static int orderingRank( PolyValue value ) {
        return switch ( value.getType() ) {
            case MAP, DOCUMENT -> 0;
            case NODE -> 1;
            case EDGE -> 2;
            case ARRAY -> 3;
            case PATH -> 4;
            case CYPHER_TEMPORAL -> 5 + ((PolyCypherTemporal) value).kind.ordinal();
            case TIMESTAMP -> 6;
            case DATE -> 7;
            case TIME -> 9;
            case INTERVAL -> 10;
            case GEOMETRY -> 11;
            case CHAR, VARCHAR -> 12;
            case BOOLEAN -> 13;
            default -> value instanceof PolyNumber ? 14 : 15 + value.getType().ordinal();
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


    public static final class StandardDeviation {

        private final Set<Object> distinctValues;
        private BigDecimal sum = BigDecimal.ZERO;
        private BigDecimal sumOfSquares = BigDecimal.ZERO;
        private boolean nonFinite;
        private long count;


        public StandardDeviation( boolean distinct ) {
            distinctValues = distinct ? new HashSet<>() : null;
        }


        public void add( PolyValue value ) {
            if ( isNull( value ) ) {
                return;
            }
            if ( !(value instanceof PolyNumber number) ) {
                throw new GenericRuntimeException( "Standard deviation requires a number." );
            }
            if ( distinctValues != null && !distinctValues.add( aggregationKey( number ) ) ) {
                return;
            }
            count++;
            if ( PolyType.APPROX_TYPES.contains( number.getType() ) && !Double.isFinite( number.doubleValue() ) ) {
                nonFinite = true;
            } else {
                BigDecimal decimal = number.bigDecimalValue();
                sum = sum.add( decimal );
                sumOfSquares = sumOfSquares.add( decimal.multiply( decimal ) );
            }
        }


        public PolyDouble deviation( boolean sample ) {
            if ( count < 2 ) {
                return PolyDouble.of( 0 );
            }
            if ( nonFinite ) {
                return PolyDouble.of( Double.NaN );
            }
            // Keep the subtraction exact so close values at large magnitudes still differ.
            BigDecimal size = BigDecimal.valueOf( count );
            BigDecimal divisor = size.multiply( BigDecimal.valueOf( sample ? count - 1 : count ) );
            BigDecimal variance = sumOfSquares.multiply( size ).subtract( sum.multiply( sum ) )
                    .divide( divisor, MathContext.DECIMAL128 );
            return PolyDouble.of( variance.sqrt( MathContext.DECIMAL128 ).doubleValue() );
        }

    }


    public static PolyBoolean notEqual( PolyValue left, PolyValue right ) {
        PolyBoolean equal = equal( left, right );
        return equal == null ? null : PolyBoolean.of( !equal.value );
    }


    public static PolyBoolean lessThan( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order < 0 );
    }


    public static PolyBoolean lessThanOrEqual( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order <= 0 );
    }


    public static PolyBoolean greaterThan( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order > 0 );
    }


    public static PolyBoolean greaterThanOrEqual( PolyValue left, PolyValue right ) {
        return orderedComparison( left, right, order -> order >= 0 );
    }


    private static PolyBoolean orderedComparison( PolyValue left, PolyValue right, IntPredicate test ) {
        if ( left == null || right == null || left.isNull() || right.isNull() ) {
            return null;
        }
        if ( left instanceof PolyNumber a && right instanceof PolyNumber b && !a.isTemporal() && !b.isTemporal() ) {
            return PolyBoolean.of( !Double.isNaN( a.doubleValue() ) && !Double.isNaN( b.doubleValue() )
                    && test.test( compareNumbers( a, b ) ) );
        }
        Integer comparison = compareStructuredValues( left, right );
        return comparison == null ? null : PolyBoolean.of( test.test( comparison ) );
    }


    private static Integer compareStructuredValues( PolyValue left, PolyValue right ) {
        if ( left == null || right == null || left.isNull() || right.isNull() ) {
            return null;
        }
        PolyCypherTemporal leftTemporal = PolyCypherTemporal.from( left );
        PolyCypherTemporal rightTemporal = PolyCypherTemporal.from( right );
        if ( leftTemporal != null || rightTemporal != null ) {
            return leftTemporal == null || rightTemporal == null || leftTemporal.kind != rightTemporal.kind
                    || leftTemporal.kind == PolyCypherTemporal.Kind.DURATION ? null : leftTemporal.compareTo( rightTemporal );
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
        if ( left.isMap() && right.isMap() ) {
            return compareMaps( left.asMap(), right.asMap(), true );
        }
        return left.compareTo( right );
    }


    private static Integer compareMaps( Map<PolyValue, PolyValue> left, Map<PolyValue, PolyValue> right, boolean predicate ) {
        // A map with a null entry is incomparable in predicates. ORDER BY still orders such maps.
        if ( predicate && Stream.concat( left.values().stream(), right.values().stream() ).anyMatch( value -> value == null || value.isNull() ) ) {
            return null;
        }
        int sizeOrder = Integer.compare( left.size(), right.size() );
        if ( sizeOrder != 0 ) {
            return sizeOrder;
        }
        List<PolyValue> leftKeys = left.keySet().stream().sorted().toList();
        List<PolyValue> rightKeys = right.keySet().stream().sorted().toList();
        for ( int i = 0; i < leftKeys.size(); i++ ) {
            int keyOrder = leftKeys.get( i ).compareTo( rightKeys.get( i ) );
            if ( keyOrder != 0 ) {
                return keyOrder;
            }
        }
        for ( int i = 0; i < leftKeys.size(); i++ ) {
            PolyValue leftValue = left.get( leftKeys.get( i ) );
            PolyValue rightValue = right.get( rightKeys.get( i ) );
            Integer valueOrder;
            if ( predicate ) {
                valueOrder = compareStructuredValues( leftValue, rightValue );
            } else {
                valueOrder = compareForOrdering( leftValue, rightValue, false );
            }
            if ( valueOrder == null || valueOrder != 0 ) {
                return valueOrder;
            }
        }
        return 0;
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
