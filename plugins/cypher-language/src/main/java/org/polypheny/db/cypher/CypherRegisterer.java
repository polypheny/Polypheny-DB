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

package org.polypheny.db.cypher;

import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.type.PolyType;

public class CypherRegisterer {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;


    public static void registerOperators() {
        if ( isInit ) {
            throw new GenericRuntimeException( "Cypher operators were already registered." );
        }

        register( OperatorName.CYPHER_LIKE, new LangFunctionOperator( OperatorName.CYPHER_LIKE.name(), Kind.LIKE, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_EQUALS, new LangFunctionOperator( OperatorName.CYPHER_EQUALS.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_NOT_EQUALS, new LangFunctionOperator( OperatorName.CYPHER_NOT_EQUALS.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_IN, new LangFunctionOperator( OperatorName.CYPHER_IN.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_LAST, new LangFunctionOperator( OperatorName.CYPHER_LAST.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_INDEX, new LangFunctionOperator( OperatorName.CYPHER_INDEX.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_SLICE, new LangFunctionOperator( OperatorName.CYPHER_SLICE.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        register( OperatorName.CYPHER_MAP_CONSTRUCTOR, new LangFunctionOperator( OperatorName.CYPHER_MAP_CONSTRUCTOR.name(), Kind.CYPHER_FUNCTION, PolyType.MAP ) );
        register( OperatorName.CYPHER_NODES, new LangFunctionOperator( OperatorName.CYPHER_NODES.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        register( OperatorName.CYPHER_RELATIONSHIPS, new LangFunctionOperator( OperatorName.CYPHER_RELATIONSHIPS.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        register( OperatorName.CYPHER_SHORTEST_PATH_VALUE, new LangFunctionOperator( OperatorName.CYPHER_SHORTEST_PATH_VALUE.name(), Kind.CYPHER_FUNCTION, PolyType.PATH ) );
        register( OperatorName.CYPHER_ALL_SHORTEST_PATHS_VALUE, new LangFunctionOperator( OperatorName.CYPHER_ALL_SHORTEST_PATHS_VALUE.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        register( OperatorName.CYPHER_LENGTH, new LangFunctionOperator( OperatorName.CYPHER_LENGTH.name(), Kind.CYPHER_FUNCTION, PolyType.BIGINT ) );
        register( OperatorName.CYPHER_SIZE, new LangFunctionOperator( OperatorName.CYPHER_SIZE.name(), Kind.CYPHER_FUNCTION, PolyType.BIGINT ) );
        register( OperatorName.CYPHER_SCALAR_FUNCTION, new LangFunctionOperator( OperatorName.CYPHER_SCALAR_FUNCTION.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_VALIDATE_PROCEDURE_ARGUMENT,
                new LangFunctionOperator( OperatorName.CYPHER_VALIDATE_PROCEDURE_ARGUMENT.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_EVALUATE_PROCEDURE_ARGUMENTS,
                new LangFunctionOperator( OperatorName.CYPHER_EVALUATE_PROCEDURE_ARGUMENTS.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) {
                    @Override
                    public boolean isDeterministic() {
                        return false;
                    }
                } );
        register( OperatorName.CYPHER_TEMPORAL_FUNCTION, new LangFunctionOperator( OperatorName.CYPHER_TEMPORAL_FUNCTION.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) {
            @Override
            public boolean isDeterministic() {
                return false;
            }


            @Override
            public boolean isDynamicFunction() {
                return true;
            }
        } );
        register( OperatorName.CYPHER_RAND, new LangFunctionOperator( OperatorName.CYPHER_RAND.name(), Kind.CYPHER_FUNCTION, PolyType.DOUBLE ) {
            @Override
            public boolean isDeterministic() {
                return false;
            }


            @Override
            public boolean isDynamicFunction() {
                return true;
            }
        } );
        register( OperatorName.CYPHER_RANGE, new LangFunctionOperator( OperatorName.CYPHER_RANGE.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        register( OperatorName.CYPHER_PERCENTILE, new LangFunctionOperator( OperatorName.CYPHER_PERCENTILE.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_TYPE, new LangFunctionOperator( OperatorName.CYPHER_TYPE.name(), Kind.CYPHER_FUNCTION, PolyType.VARCHAR ) );
        register( OperatorName.CYPHER_ID, new LangFunctionOperator( OperatorName.CYPHER_ID.name(), Kind.CYPHER_FUNCTION, PolyType.VARCHAR ) );
        register( OperatorName.CYPHER_LABELS, new LangFunctionOperator( OperatorName.CYPHER_LABELS.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY, PolyType.VARCHAR ) );
        register( OperatorName.CYPHER_KEYS, new LangFunctionOperator( OperatorName.CYPHER_KEYS.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY, PolyType.VARCHAR ) );
        register( OperatorName.CYPHER_PROPERTIES, new LangFunctionOperator( OperatorName.CYPHER_PROPERTIES.name(), Kind.CYPHER_FUNCTION, PolyType.MAP ) );
        register( OperatorName.CYPHER_START_NODE, new LangFunctionOperator( OperatorName.CYPHER_START_NODE.name(), Kind.CYPHER_FUNCTION, PolyType.NODE ) );
        register( OperatorName.CYPHER_END_NODE, new LangFunctionOperator( OperatorName.CYPHER_END_NODE.name(), Kind.CYPHER_FUNCTION, PolyType.NODE ) );
        register( OperatorName.CYPHER_ARITHMETIC, new LangFunctionOperator( OperatorName.CYPHER_ARITHMETIC.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_LESS_THAN, new LangFunctionOperator( OperatorName.CYPHER_LESS_THAN.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_LESS_THAN_OR_EQUAL, new LangFunctionOperator( OperatorName.CYPHER_LESS_THAN_OR_EQUAL.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_GREATER_THAN, new LangFunctionOperator( OperatorName.CYPHER_GREATER_THAN.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_GREATER_THAN_OR_EQUAL, new LangFunctionOperator( OperatorName.CYPHER_GREATER_THAN_OR_EQUAL.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.STARTS_WITH, new LangFunctionOperator( OperatorName.STARTS_WITH.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.ENDS_WITH, new LangFunctionOperator( OperatorName.ENDS_WITH.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_CONTAINS, new LangFunctionOperator( OperatorName.CYPHER_CONTAINS.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_HAS_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_HAS_PROPERTY.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_PROPERTY_EXISTS, new LangFunctionOperator( OperatorName.CYPHER_PROPERTY_EXISTS.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_PATTERN_EXISTS, new LangFunctionOperator( OperatorName.CYPHER_PATTERN_EXISTS.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_EXISTS_SUBQUERY, new LangFunctionOperator( OperatorName.CYPHER_EXISTS_SUBQUERY.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) {
            @Override
            public boolean isDeterministic() {
                return false;
            }
        } );
        register( OperatorName.CYPHER_PATTERN_PATHS, new LangFunctionOperator( OperatorName.CYPHER_PATTERN_PATHS.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        register( OperatorName.CYPHER_GRAPH_VALUE, new LangFunctionOperator( OperatorName.CYPHER_GRAPH_VALUE.name(), Kind.CYPHER_FUNCTION, PolyType.GRAPH ) );

        register( OperatorName.CYPHER_HAS_LABEL, new LangFunctionOperator( OperatorName.CYPHER_HAS_LABEL.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_PATH_MATCH, new LangFunctionOperator( OperatorName.CYPHER_PATH_MATCH.name(), Kind.CYPHER_FUNCTION, CypherPathMatchUtil::inferReturnType, CypherPathMatchUtil::inferReturnType ) );
        register( OperatorName.CYPHER_SHORTEST_PATH_MATCH, new LangFunctionOperator( OperatorName.CYPHER_SHORTEST_PATH_MATCH.name(), Kind.CYPHER_FUNCTION, CypherPathMatchUtil::inferReturnType, CypherPathMatchUtil::inferReturnType ) );
        register( OperatorName.CYPHER_ALL_SHORTEST_PATHS_MATCH, new LangFunctionOperator( OperatorName.CYPHER_ALL_SHORTEST_PATHS_MATCH.name(), Kind.CYPHER_FUNCTION, CypherPathMatchUtil::inferReturnType, CypherPathMatchUtil::inferReturnType ) );

        register( OperatorName.CYPHER_NODE_EXTRACT, new LangFunctionOperator( OperatorName.CYPHER_NODE_EXTRACT.name(), Kind.CYPHER_FUNCTION, PolyType.NODE ) );

        register( OperatorName.CYPHER_EXTRACT_FROM_PATH, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_FROM_PATH.name(), Kind.CYPHER_FUNCTION, CypherFromPathUtil::inferReturnType, CypherFromPathUtil::inferReturnType ) );

        register( OperatorName.CYPHER_PATHS_DISJOINT, new LangFunctionOperator( OperatorName.CYPHER_PATHS_DISJOINT.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_NODE_MATCH, new LangFunctionOperator( OperatorName.CYPHER_NODE_MATCH.name(), Kind.CYPHER_FUNCTION, PolyType.NODE ) );

        register( OperatorName.CYPHER_EXTRACT_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_PROPERTY.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_EXTRACT_PROPERTIES, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_PROPERTIES.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_EXTRACT_ID, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_ID.name(), Kind.CYPHER_FUNCTION, PolyType.VARCHAR ) );
        register( OperatorName.CYPHER_MATCH_ELEMENT_ID, new LangFunctionOperator( OperatorName.CYPHER_MATCH_ELEMENT_ID.name(), Kind.CYPHER_FUNCTION, PolyType.VARCHAR ) );
        register( OperatorName.CYPHER_VALIDATE_MATCH_NODE, new LangFunctionOperator( OperatorName.CYPHER_VALIDATE_MATCH_NODE.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_MATCH_PROPERTIES, new LangFunctionOperator( OperatorName.CYPHER_MATCH_PROPERTIES.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_MATCH_VISIBLE_PATH, new LangFunctionOperator( OperatorName.CYPHER_MATCH_VISIBLE_PATH.name(), Kind.CYPHER_FUNCTION, PolyType.PATH ) );
        register( OperatorName.CYPHER_VALIDATE_MATCH_RELATIONSHIP_LIST, new LangFunctionOperator( OperatorName.CYPHER_VALIDATE_MATCH_RELATIONSHIP_LIST.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_MATCH_BINDING_GUARD, new LangFunctionOperator( OperatorName.CYPHER_MATCH_BINDING_GUARD.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_CASE, new LangFunctionOperator( OperatorName.CYPHER_CASE.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_COALESCE, new LangFunctionOperator( OperatorName.CYPHER_COALESCE.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );
        register( OperatorName.CYPHER_LIST_COMPREHENSION, new LangFunctionOperator( OperatorName.CYPHER_LIST_COMPREHENSION.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY ) );
        for ( OperatorName quantifier : List.of( OperatorName.CYPHER_ALL_MATCH, OperatorName.CYPHER_ANY_MATCH, OperatorName.CYPHER_NONE_MATCH, OperatorName.CYPHER_SINGLE_MATCH ) ) {
            register( quantifier, new LangFunctionOperator( quantifier.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        }
        register( OperatorName.CYPHER_BOOLEAN_PREDICATE, new LangFunctionOperator( OperatorName.CYPHER_BOOLEAN_PREDICATE.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );
        register( OperatorName.CYPHER_MATCH_WITH_WHERE, new LangFunctionOperator( OperatorName.CYPHER_MATCH_WITH_WHERE.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_EXTRACT_LABELS, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_LABELS.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY, PolyType.VARCHAR ) );

        register( OperatorName.CYPHER_HAS_LABEL, new LangFunctionOperator( OperatorName.CYPHER_HAS_LABEL.name(), Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_TO_LIST, new LangFunctionOperator( OperatorName.CYPHER_TO_LIST.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY, PolyType.ANY ) );
        register( OperatorName.CYPHER_LIST_CONSTRUCTOR, new LangFunctionOperator( OperatorName.CYPHER_LIST_CONSTRUCTOR.name(), Kind.CYPHER_FUNCTION, PolyType.ARRAY, PolyType.ANY ) );

        register( OperatorName.CYPHER_ADJUST_EDGE, new LangFunctionOperator( OperatorName.CYPHER_ADJUST_EDGE.name(), Kind.CYPHER_FUNCTION, PolyType.EDGE ) );
        register( OperatorName.CYPHER_CREATE_NODE, new LangFunctionOperator( OperatorName.CYPHER_CREATE_NODE.name(), Kind.CYPHER_FUNCTION, PolyType.NODE ) );
        register( OperatorName.CYPHER_CREATE_EDGE, new LangFunctionOperator( OperatorName.CYPHER_CREATE_EDGE.name(), Kind.CYPHER_FUNCTION, PolyType.EDGE ) );
        register( OperatorName.CYPHER_CREATE_PATH, new LangFunctionOperator( OperatorName.CYPHER_CREATE_PATH.name(), Kind.CYPHER_FUNCTION, PolyType.PATH ) );

        register( OperatorName.CYPHER_SET_LABELS, new LangFunctionOperator( OperatorName.CYPHER_SET_LABELS.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_SET_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_SET_PROPERTY.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_SET_PROPERTIES, new LangFunctionOperator( OperatorName.CYPHER_SET_PROPERTIES.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_REMOVE_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_REMOVE_PROPERTY.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_REMOVE_LABELS, new LangFunctionOperator( OperatorName.CYPHER_REMOVE_LABELS.name(), Kind.CYPHER_FUNCTION, PolyType.ANY ) );

        register( OperatorName.CYPHER_GEO_DISTANCE, new LangFunctionOperator( "GEO_DISTANCE", Kind.CYPHER_FUNCTION, PolyType.GEOMETRY ) );

        register( OperatorName.CYPHER_GEO_CONTAINS, new LangFunctionOperator( "GEO_CONTAINS", Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_GEO_INTERSECTS, new LangFunctionOperator( "GEO_INTERSECTS", Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_GEO_WITHIN, new LangFunctionOperator( "GEO_WITHIN", Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_POINT, new LangFunctionOperator( "CYPHER_POINT", Kind.CYPHER_FUNCTION, PolyType.DOCUMENT ) );

        register( OperatorName.DISTANCE, new LangFunctionOperator( "DISTANCE", Kind.CYPHER_FUNCTION, PolyType.DOUBLE ) );

        register( OperatorName.DISTANCE_NEO4J, new LangFunctionOperator( "DISTANCE_NEO4J", Kind.CYPHER_FUNCTION, PolyType.DOUBLE ) );

        register( OperatorName.CYPHER_WITHIN_BBOX, new LangFunctionOperator( "CYPHER_WITHIN_BBOX", Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        register( OperatorName.CYPHER_WITHIN_GEOMETRY, new LangFunctionOperator( "CYPHER_WITHIN_GEOMETRY", Kind.CYPHER_FUNCTION, PolyType.BOOLEAN ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( QueryLanguage.from( "cypher" ), name, operator );
    }


    public static void removeOperators() {
        OperatorRegistry.remove( QueryLanguage.from( "cypher" ) );
    }

}
