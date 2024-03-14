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

package org.polypheny.db.adapter.mongodb.rules;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.gridfs.GridFSBucket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonNull;
import org.bson.BsonRegularExpression;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.bson.BsonFunctionHelper;
import org.polypheny.db.adapter.mongodb.bson.BsonKeyValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Filter;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.fun.SqlItemOperator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.JsonBuilder;


/**
 * Implementation of a {@link org.polypheny.db.algebra.core.Filter}
 * relational expression in MongoDB.
 */
public class MongoFilter extends Filter implements MongoAlg {

    public MongoFilter( AlgCluster cluster, AlgTraitSet traitSet, AlgNode child, RexNode condition ) {
        super( cluster, traitSet, child, condition );
        assert getConvention() == CONVENTION;
        assert getConvention() == child.getConvention();
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.1 );
    }


    @Override
    public MongoFilter copy( AlgTraitSet traitSet, AlgNode input, RexNode condition ) {
        return new MongoFilter( getCluster(), traitSet, input, condition );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.visitChild( 0, getInput() );
        // to not break the existing functionality for now we have to handle it this way
        Translator translator;

        translator = new Translator( MongoRules.mongoFieldNames( getTupleType() ), getTupleType(), implementor );
        translator.translateMatch( condition, implementor );
    }


    /**
     * Translates {@link RexNode} expressions into MongoDB expression strings.
     */
    static class Translator {

        final JsonBuilder builder = new JsonBuilder();
        private final List<String> fieldNames;
        private final AlgDataType rowType;
        private final List<BsonDocument> ors = new ArrayList<>();
        private final GridFSBucket bucket;
        private final BsonDocument preProjections = new BsonDocument();
        private final Implementor implementor;
        private Map<String, List<BsonValue>> map = new HashMap<>();
        private boolean inExpr;
        private RexNode tempElem = null;

        private String modifier = null;


        Translator( List<String> fieldNames, Implementor implementor ) {
            this( fieldNames, null, implementor );
        }


        Translator( List<String> fieldNames, AlgDataType rowType, Implementor implementor ) {
            this.builder.setMongo( true );
            this.fieldNames = fieldNames;
            this.rowType = rowType;
            this.bucket = implementor.bucket;
            this.implementor = implementor;
        }


        private void translateMatch( RexNode condition, Implementor implementor ) {
            BsonDocument value = translateFinalOr( condition );
            if ( !value.isEmpty() ) {
                if ( !preProjections.isEmpty() ) {
                    implementor.add( null, MongoAlg.Implementor.toJson( new BsonDocument( "$addFields", preProjections ) ) );
                }

                implementor.filter.add( value );
                implementor.add( null, MongoAlg.Implementor.toJson( new BsonDocument( "$match", getFilter( value ) ) ) );
            }

        }


        public static BsonDocument getFilter( BsonDocument filter ) {
            if ( filter.size() != 1 ) {
                return filter;
            }
            String key = filter.keySet().iterator().next();
            BsonValue value = filter.values().iterator().next();
            if ( !key.equals( "$or" ) && !key.equals( "$and" ) ) {
                return filter;
            }

            if ( !value.isArray() || value.asArray().size() != 1 ) {
                return filter;
            }
            return getFilter( value.asArray().get( 0 ).asDocument() );
        }


        private BsonDocument translateFinalOr( RexNode condition ) {
            for ( RexNode node : AlgOptUtil.disjunctions( condition ) ) {
                Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
                this.map = new HashMap<>();
                translateAnd( node );

                String op = "$or";
                if ( modifier != null ) {
                    // we have a negation and have to flip the operator
                    op = modifier.equals( "$not" ) ? "$and" : "$or";
                }

                mergeMaps( this.map, shallowCopy, op );
                this.map = shallowCopy;
            }

            return removeUnnecessaryGate( asConditionDocument( this.map ) ).asDocument();
        }


        private BsonValue removeUnnecessaryGate( BsonValue raw ) {
            BsonValue value = raw;
            if ( raw.isDocument() ) {
                BsonDocument document = raw.asDocument();
                if ( document.size() == 1 && (document.getFirstKey().equals( "$and" ) || document.getFirstKey().equals( "$or" )) && document.get( document.getFirstKey() ).isArray() && document.get( document.getFirstKey() ).asArray().size() == 1 ) {
                    return removeUnnecessaryGate( document.get( document.getFirstKey() ).asArray().get( 0 ) );
                } else {
                    BsonDocument newDoc = new BsonDocument();
                    document.forEach( ( k, v ) -> newDoc.append( k, removeUnnecessaryGate( v ) ) );
                    value = newDoc;
                }


            } else if ( raw.isArray() ) {
                List<BsonValue> values = new ArrayList<>();
                for ( BsonValue v : raw.asArray() ) {
                    values.add( removeUnnecessaryGate( v ) );
                }
                value = new BsonArray( values );
            }
            return value;

        }


        private BsonDocument asConditionDocument( Map<String, List<BsonValue>> map ) {
            if ( map.isEmpty() ) {
                return new BsonDocument();
            }
            BsonDocument doc = new BsonDocument();
            List<BsonValue> ands = new ArrayList<>();
            for ( Entry<String, List<BsonValue>> entry : map.entrySet() ) {
                if ( entry.getValue().size() == 1 && !entry.getKey().equals( "$or" ) && !entry.getKey().equals( "$and" ) ) {
                    doc.put( entry.getKey(), entry.getValue().get( 0 ) );
                } else if ( entry.getKey().equals( "$or" ) || entry.getKey().equals( "$and" ) ) {
                    doc.put( entry.getKey(), new BsonArray( entry.getValue() ) );
                } else {
                    ands.addAll( entry.getValue().stream().map( e -> new BsonDocument( entry.getKey(), e ) ).toList() );
                }
            }
            if ( !ands.isEmpty() ) {
                doc.put( "$and", new BsonArray( ands ) );
            }
            return doc;
        }


        private BsonValue asCondition( List<BsonValue> bsonValues, String key ) {
            if ( bsonValues.size() == 1 ) {
                return bsonValues.get( 0 );
            } else {
                if ( key.equals( "$and" ) ) {
                    return new BsonDocument( key, new BsonArray( bsonValues ) );
                } else {
                    return new BsonDocument( "$or", new BsonArray( bsonValues ) );
                }

            }
        }


        /**
         * Translates a condition that may be an AND of other conditions. Gathers together conditions that apply to the same field.
         */
        private void translateAnd( RexNode node0 ) {
            for ( RexNode node : AlgOptUtil.conjunctions( node0 ) ) {
                Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
                this.map = new HashMap<>();
                translateMatch2( node );
                String op = "$and";
                if ( modifier != null ) {
                    // we have a negation and have to flip the operator
                    op = "$or";
                }
                mergeMaps( this.map, shallowCopy, op );
                this.map = shallowCopy;
            }
        }


        /**
         * Returns whether {@code v0} is a stronger value for operator {@code key} than {@code v1}.
         * <p>
         * For example, {@code stronger("$lt", 100, 200)} returns true, because "&lt; 100" is a more powerful condition than "&lt; 200".
         */
        private boolean stronger( String key, Object v0, Object v1 ) {
            if ( key.equals( "$lt" ) || key.equals( "$lte" ) ) {
                if ( v0 instanceof Number && v1 instanceof Number ) {
                    return ((Number) v0).doubleValue() < ((Number) v1).doubleValue();
                }
                if ( v0 instanceof String && v1 instanceof String ) {
                    return v0.toString().compareTo( v1.toString() ) < 0;
                }
            }
            if ( key.equals( "$gt" ) || key.equals( "$gte" ) ) {
                return stronger( "$lt", v1, v0 );
            }
            return false;
        }


        private void translateMatch2( RexNode node ) {
            switch ( node.getKind() ) {
                case EQUALS:
                    translateBinary( null, null, (RexCall) node );
                    return;
                case LESS_THAN:
                    translateBinary( "$lt", "$gt", (RexCall) node );
                    return;
                case LESS_THAN_OR_EQUAL:
                    translateBinary( "$lte", "$gte", (RexCall) node );
                    return;
                case NOT_EQUALS:
                    translateBinary( "$ne", "$ne", (RexCall) node );
                    return;
                case GREATER_THAN:
                    translateBinary( "$gt", "$lt", (RexCall) node );
                    return;
                case GREATER_THAN_OR_EQUAL:
                    translateBinary( "$gte", "$lte", (RexCall) node );
                    return;
                case IS_NOT_NULL:
                    translateIsNull( (RexCall) node, true );
                    return;
                case IS_NULL:
                    translateIsNull( (RexCall) node, false );
                    return;
                case LIKE:
                    translateLike( (RexCall) node );
                    return;
                case MQL_SIZE_MATCH:
                    translateSize( (RexCall) node );
                    return;
                case MQL_REGEX_MATCH:
                    translateRegex( (RexCall) node );
                    return;
                case MQL_TYPE_MATCH:
                    translateTypeMatch( (RexCall) node );
                    return;
                case MQL_ELEM_MATCH:
                    translateElemMatch( (RexCall) node );
                    return;
                case IS_NOT_TRUE:
                    translateIsTrue( (RexCall) node, true );
                    return;
                case IS_TRUE:
                    translateIsTrue( (RexCall) node, false );
                    return;
                case NOT:
                case IS_FALSE:
                    translateNot( (RexCall) node );
                    return;
                case OR:
                    translateOr( (RexCall) node );
                    return;
                case MQL_EXISTS:
                    translateExists( (RexCall) node );
                    return;
                case DYNAMIC_PARAM:
                    translateBooleanDyn( (RexDynamicParam) node );
                    return;
                case INPUT_REF:
                    // means is true
                    attachCondition( "$eq", getParamAsKey( node ), new BsonBoolean( true ) );
                    return;
                default:
                    throw new AssertionError( "cannot translate " + node );
            }
        }


        /**
         * Single true or false statements are handled with function logic
         * e.g. SELECT * FROM table where true
         *
         * @param node the dynamic boolean operation
         */
        private void translateBooleanDyn( RexDynamicParam node ) {
            assert node.getType().getPolyType() == PolyType.BOOLEAN;
            String name = getRandomName();
            preProjections.put( name, new BsonDynamic( node ) );
            attachCondition( null, "$expr", new BsonDocument(
                    "$function",
                    new BsonDocument()
                            .append( "body", new BsonString( "function(val){ return val }" ) )
                            .append( "args", new BsonArray( Collections.singletonList( new BsonString( "$" + name ) ) ) )
                            .append( "lang", new BsonString( "js" ) ) ) );
        }


        private void translateOr( RexCall node ) {
            for ( RexNode operand : node.operands ) {
                Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
                this.map = new HashMap<>();
                translateNode( operand );
                mergeMaps( this.map, shallowCopy, "$or" );
                this.map = shallowCopy;
            }
        }


        private void translateNot( RexCall node ) {
            Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
            this.map = new HashMap<>();

            this.modifier = "$not";

            if ( node.operands.size() == 1 ) {
                translateNode( node.operands.get( 0 ) );
            } else {
                translateMatch2( node );
            }

            this.modifier = null;

            /*shallowCopy.put( "$not", new BsonArray(
                    List.of( new BsonDocument(
                            this.map.entrySet().stream().map( e -> new BsonElement( e.getKey(), new BsonArray( e.getValue() ) ) )
                                    .collect( Collectors.toList() ) ) ) ) );*/
            //mergeMaps( this.map, shallowCopy, "$not" );
            //this.map = shallowCopy;

        }


        private void translateNode( RexNode node0 ) {
            if ( node0.getKind() == Kind.AND ) {
                translateAnd( node0 );
            } else if ( node0.getKind() == Kind.OR ) {
                translateFinalOr( node0 );
            } else {
                translateMatch2( node0 );
            }
        }


        private void mergeMaps( Map<String, List<BsonValue>> newValueMap, Map<String, List<BsonValue>> finalValueMap, String op ) {
            if ( !op.equals( "$or" ) && !op.equals( "$and" ) ) {
                for ( Entry<String, List<BsonValue>> values : newValueMap.entrySet() ) {
                    BsonValue entry = asCondition( values.getValue(), values.getKey() );
                    if ( finalValueMap.containsKey( values.getKey() ) ) {
                        finalValueMap.get( values.getKey() ).add( new BsonDocument( op, entry ) );
                    } else {
                        List<BsonValue> bsons = new ArrayList<>();
                        bsons.add( new BsonDocument( op, entry ) );
                        finalValueMap.put( values.getKey(), bsons );
                    }
                }
            } else {
                List<BsonValue> ors = new ArrayList<>();

                for ( Entry<String, List<BsonValue>> entry : newValueMap.entrySet() ) {
                    if ( entry.getKey().equals( "$or" ) ) {
                        ors.addAll( entry.getValue() );
                    } else if ( entry.getKey().equals( "$and" ) ) {
                        ors.add( new BsonDocument( "$and", new BsonArray( entry.getValue() ) ) );
                    } else if ( entry.getKey().equals( "$not" ) ) {
                        ors.add( new BsonDocument( entry.getKey(), new BsonArray( entry.getValue() ) ) );
                    } else {
                        List<BsonValue> ands = new ArrayList<>();
                        for ( BsonValue value : entry.getValue() ) {
                            ands.add( new BsonDocument( entry.getKey(), value ) );
                        }
                        if ( ands.size() == 1 ) {
                            ors.add( ands.get( 0 ) );
                        } else {
                            ors.add( new BsonDocument( "$and", new BsonArray( ands ) ) );
                        }
                    }
                }

                if ( finalValueMap.containsKey( op ) ) {
                    if ( !op.equals( "$and" ) ) {
                        finalValueMap.get( op ).addAll( ors );
                    } else {
                        finalValueMap.get( op ).add( new BsonDocument( "$or", new BsonArray( ors ) ) );
                    }

                } else {
                    if ( !op.equals( "$and" ) ) {
                        finalValueMap.put( op, ors );
                    } else {
                        finalValueMap.put( op, new ArrayList<>( Collections.singletonList( new BsonDocument( "$or", new BsonArray( ors ) ) ) ) );
                    }
                }
            }
        }


        /**
         * Normally, negation can be handled by just prefixing with "$not":
         * key: value -> $not:{key:value}
         * <p>
         * but especially for complex queries it has to be pushed into the statement
         * key: value -> key:{$not:value}
         *
         * @param entry the child entry, which is negated
         * @return the transformed value
         */
        private BsonValue negate( BsonValue entry ) {
            // tryPushDown changes the structure of entry
            // and we need a clean copy if push down does not work
            BsonValue copy = copy( entry );
            if ( !tryPushDown( entry ) ) {
                if ( (!entry.isDocument() && !entry.isArray()) || entry instanceof BsonDynamic ) {
                    return new BsonDocument( "$ne", copy );
                } else {
                    return new BsonDocument( "$not", copy );
                }
            }
            return entry;
        }


        private BsonValue copy( BsonValue entry ) {
            BsonDocument doc = new BsonDocument();
            doc.put( "_temp", entry );
            String json = doc.toJson( JsonWriterSettings.builder().outputMode( JsonMode.EXTENDED ).build() );
            doc = BsonDocument.parse( json );
            return doc.get( "_temp" );
        }


        private boolean tryPushDown( BsonValue entry ) {
            boolean successful = false;
            if ( entry instanceof BsonDynamic ) {
                return false;
            } else if ( entry.isDocument() ) {
                List<String> toRemove = new ArrayList<>();
                for ( Entry<String, BsonValue> valueEntry : entry.asDocument().entrySet() ) {
                    // functions are structured like documents, and we cannot push it into them
                    if ( valueEntry.getKey().equals( "$function" ) ) {
                        return false;
                    }
                    if ( !valueEntry.getKey().startsWith( "$" ) ) {
                        if ( (!valueEntry.getValue().isDocument() && !valueEntry.getValue().isArray()) || valueEntry.getValue() instanceof BsonDynamic ) {
                            entry.asDocument().put( valueEntry.getKey(), new BsonDocument( "$ne", valueEntry.getValue() ) );
                        } else {
                            entry.asDocument().put( valueEntry.getKey(), new BsonDocument( "$not", valueEntry.getValue() ) );
                        }

                        successful = true;
                    } else {
                        String opposite = null;
                        if ( valueEntry.getKey().equals( "$and" ) ) {
                            opposite = "$or";
                        } else if ( valueEntry.getKey().equals( "$or" ) ) {
                            opposite = "$and";
                        }
                        if ( opposite != null ) {
                            if ( entry.asDocument().containsKey( opposite ) ) {
                                BsonValue child = entry.asDocument().get( opposite );
                                if ( child.isArray() ) {
                                    child.asArray().add( valueEntry.getValue() );
                                } else {
                                    entry.asDocument().put( opposite, new BsonArray( Arrays.asList( child, valueEntry.getValue() ) ) );
                                }

                            } else {
                                entry.asDocument().put( opposite, valueEntry.getValue() );
                            }
                            toRemove.add( valueEntry.getKey() );
                        }

                        successful |= tryPushDown( valueEntry.getValue() );
                    }
                }

                toRemove.forEach( k -> entry.asDocument().remove( k ) );

            } else if ( entry.isArray() ) {
                for ( BsonValue value : entry.asArray() ) {
                    successful |= tryPushDown( value );
                }
            }

            return successful;
        }


        private void translateExists( RexCall node ) {
            assert node.operands.size() == 3; // where / if exists / key
            assert node.operands.get( 0 ) instanceof RexIndexRef;
            String key = getParamAsKey( node.operands.get( 0 ) );

            if ( node.operands.get( 1 ).isA( Kind.DYNAMIC_PARAM ) ) {
                BsonDynamic identifier = new BsonDynamic( node.operands.get( 2 ).unwrap( RexDynamicParam.class )
                        .orElseThrow() ).setPolyFunction( "joinPoint" ).adjustType( PolyType.VARCHAR );
                BsonKeyValue keyValue = new BsonKeyValue( identifier, new BsonDocument().append( "$exists", new BsonDynamic( node.operands.get( 1 ).unwrap( RexDynamicParam.class ).orElseThrow() ) ) );
                attachCondition( null, keyValue.placeholderKey, keyValue );
            } else if ( node.operands.get( 2 ).unwrap( RexCall.class ).isPresent() ) {
                key += "." + ((RexCall) node.operands.get( 2 ))
                        .operands
                        .stream()
                        .map( o -> ((RexLiteral) o).value.asString().value )
                        .collect( Collectors.joining( "." ) );
                attachCondition( "$exists", key, BsonBoolean.valueOf( node.operands.get( 1 ).unwrap( RexLiteral.class ).orElseThrow().value.asBoolean().value ) );
            }

        }


        /**
         * Translate a Kind.DOC_ELEM_MATCH condition into the Bson format "$elemMatch":[<query1>,<query2>,,,]
         *
         * @param node the untranslated DOC_ELEM_MATCH
         */
        private void translateElemMatch( RexCall node ) {
            if ( node.operands.size() != 2 ) {
                return;
            }

            Map<String, List<BsonValue>> shallowCopy = new HashMap<>( this.map );
            this.map = new HashMap<>();

            this.tempElem = node.operands.get( 0 );

            translateNode( node.operands.get( 1 ) );

            this.tempElem = null;

            mergeMaps( this.map, shallowCopy, "$elemMatch" );
            this.map = shallowCopy;

        }


        /**
         * Translates a {@link Kind#MQL_TYPE_MATCH } to its {"$type": 3} form
         *
         * @param node the untranslated node
         */
        private void translateTypeMatch( RexCall node ) {
            if ( node.operands.size() != 2 || node.operands.get( 0 ).unwrap( RexNameRef.class ).isEmpty() ) {
                return;
            }

            String key = MongoRules.translateDocValueAsKey( rowType, node.operands.get( 0 ).unwrap( RexNameRef.class ).orElseThrow() );

            if ( node.operands.get( 1 ).unwrap( RexCall.class ).isPresent() ) {
                List<BsonValue> types = ((RexCall) node.operands.get( 1 )).operands
                        .stream()
                        .map( el -> ((RexLiteral) el).value.asNumber().intValue() )
                        .map( BsonInt32::new )
                        .collect( Collectors.toList() );
                attachCondition( "$type", key, new BsonArray( types ) );
            } else if ( node.operands.get( 1 ).isA( Kind.DYNAMIC_PARAM ) ) {
                attachCondition( "$type", key, new BsonDynamic( (RexDynamicParam) node.operands.get( 1 ) ) );
            }


        }


        /**
         * Translates a {@link Kind#EXISTS } to its {"$exists": true} form
         *
         * @param node the untranslated node
         */
        private void translateRegex( RexCall node ) {
            if ( node.operands.size() != 6 ) {
                return;
            }
            String left = getParamAsKey( node.operands.get( 0 ) );
            if ( node.isA( Kind.DYNAMIC_PARAM ) ) {
                throw new RuntimeException( "translation of " + node + " is not possible with MongoFilter" );
            }
            String value = getLiteralAs( node, 1, p -> p.asString().value );
            boolean isInsensitive = getLiteralAs( node, 2, p -> p.asBoolean().value );
            boolean isMultiline = getLiteralAs( node, 3, p -> p.asBoolean().value );
            boolean doesIgnoreWhitespace = getLiteralAs( node, 4, p -> p.asBoolean().value );
            boolean allowsDot = getLiteralAs( node, 5, p -> p.asBoolean().value );

            String options = (isInsensitive ? "i" : "")
                    + (isMultiline ? "m" : "")
                    + (doesIgnoreWhitespace ? "x" : "")
                    + (allowsDot ? "s" : "");

            attachCondition( null, left, new BsonRegularExpression( value, options ) );

        }


        private <E> E getLiteralAs( RexCall node, int pos, Function<PolyValue, E> transformer ) {
            return transformer.apply( node.operands.get( pos ).unwrap( RexLiteral.class ).orElseThrow().value );
        }


        private void translateSize( RexCall node ) {
            if ( node.operands.size() != 2 ) {
                return;
            }
            String left = getParamAsKey( node.operands.get( 0 ) );
            BsonValue value = getParamAsValue( node.operands.get( 1 ) );
            attachCondition( null, left, new BsonDocument( "$size", value ) );

        }


        private String getParamAsKey( RexNode node ) {
            if ( node.isA( Kind.INPUT_REF ) ) {
                return rowType.getFieldNames().get( ((RexIndexRef) node).getIndex() );
            } else {
                return MongoRules.translateDocValueAsKey( rowType, (RexNameRef) node );
            }
        }


        @Nullable
        private BsonValue getParamAsValue( RexNode node ) {
            if ( node.isA( Kind.INPUT_REF ) ) {
                return new BsonString( "$" + rowType.getFieldNames().get( ((RexIndexRef) node).getIndex() ) );
            } else if ( node.isA( Kind.DYNAMIC_PARAM ) ) {
                return new BsonDynamic( (RexDynamicParam) node );
            } else if ( node.isA( Kind.LITERAL ) ) {
                return BsonUtil.getAsBson( (RexLiteral) node, bucket );
            }
            return null;
        }


        /**
         * Translates a {@link Kind#LIKE } to its {"$regex": /$val/} form
         *
         * @param call the untranslated node
         */
        private void translateLike( RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );

            switch ( right.getKind() ) {
                case DYNAMIC_PARAM:
                    attachCondition( null, getPhysicalName( (RexIndexRef) left ),
                            new BsonDynamic( (RexDynamicParam) right ).setIsRegex( true ) );
                    break;

                case LITERAL:
                    attachCondition( null, getPhysicalName( (RexIndexRef) left ),
                            BsonUtil.replaceLikeWithRegex( ((RexLiteral) right).getValue().asString().value ) );
                    break;

                case INPUT_REF:
                    attachCondition( null, "$expr", new BsonDocument(
                            "$eq",
                            new BsonArray(
                                    Arrays.asList(
                                            new BsonString( "$" + getPhysicalName( (RexIndexRef) left ) ),
                                            new BsonString( "$" + getPhysicalName( (RexIndexRef) right ) ) ) ) ) );

                    break;
                default:
                    throw new IllegalStateException( "Unexpected value: " + right.getKind() );
            }
        }


        private void translateIsNull( RexCall node, boolean isInverted ) {
            final RexNode single = node.operands.get( 0 );
            if ( single.getKind() == Kind.INPUT_REF ) {
                String name = getParamAsKey( single );
                String op = isInverted ? "$ne" : "$eq";
                attachCondition( op, name, new BsonNull() );
                return;
            }

            throw new RuntimeException( "translation of " + node + " is not possible with MongoFilter" );
        }


        private void translateIsTrue( RexCall node, boolean isInverted ) {
            final RexNode single = node.operands.get( 0 );
            if ( single instanceof RexCall ) {
                if ( single.isA( Kind.INPUT_REF ) ) {
                    attachCondition( "$eq", getParamAsKey( single ), new BsonBoolean( !isInverted ) );
                } else {
                    if ( isInverted ) {
                        translateNot( (RexCall) single );
                    } else {
                        translateMatch2( single );
                    }
                }
                return;
            }

            BsonValue field;
            BsonBoolean trueBool = new BsonBoolean( true );
            String randomName = getRandomName();
            if ( single.isA( Kind.LITERAL ) && single.getType().getPolyType() == PolyType.BOOLEAN ) {
                field = new BsonBoolean( ((RexLiteral) single).value.asBoolean().value );
                this.preProjections.put( randomName, field );

                attachCondition( isInverted ? "$ne" : "$eq", randomName, trueBool );
                return;
            } else if ( single.isA( Kind.DYNAMIC_PARAM ) ) {
                field = new BsonDynamic( (RexDynamicParam) single );
                this.preProjections.put( randomName, field );

                attachCondition( isInverted ? "$ne" : "$eq", randomName, trueBool );
                return;
            }

            throw new RuntimeException( "translation of " + node + " is not possible with MongoFilter" );
        }


        /**
         * Translates a call to a binary operator, reversing arguments if necessary.
         */
        private void translateBinary( String op, String rop, RexCall call ) {
            final RexNode left = call.operands.get( 0 );
            final RexNode right = call.operands.get( 1 );
            boolean b = translateBinary2( op, left, right );
            if ( b ) {
                return;
            }
            b = translateBinary2( rop, right, left );
            if ( b ) {
                return;
            }

            b = translateArray( op, left, right );
            if ( b ) {
                return;
            }

            b = translateArray( op, right, left );
            if ( b ) {
                return;
            }

            b = translateExpr( op, left, right );
            if ( b ) {
                return;
            }

            if ( left.isA( Kind.NAME_INDEX_REF ) ) {
                b = translateDocValue( op, left.unwrap( RexNameRef.class ).orElseThrow(), right );
                if ( b ) {
                    return;
                }
            }

            throw new AssertionError( "cannot translate op " + op + " call " + call );
        }


        private boolean translateExpr( String op, RexNode left, RexNode right ) {
            BsonValue l;
            BsonValue r;
            if ( left.isA( Kind.INPUT_REF ) ) {
                l = new BsonString( "$" + getPhysicalName( (RexIndexRef) left ) );
            } else if ( left.isA( Kind.NAME_INDEX_REF ) ) {
                l = new BsonString( "$" + MongoRules.translateDocValueAsKey( rowType, (RexNameRef) left ) );
            } else {
                return false;
            }

            if ( right.isA( Kind.INPUT_REF ) ) {
                r = new BsonString( "$" + getPhysicalName( (RexIndexRef) right ) );
            } else if ( right.isA( Kind.NAME_INDEX_REF ) ) {
                r = new BsonString( "$" + MongoRules.translateDocValueAsKey( rowType, (RexNameRef) right ) );
            } else {
                return false;
            }

            this.inExpr = true;
            attachCondition( null, "$expr", new BsonDocument( Objects.requireNonNullElse( op, "$eq" ), new BsonArray( Arrays.asList( l, r ) ) ) );
            this.inExpr = false;
            return true;

        }


        /**
         * Translate the given parameters as parts of arrays
         *
         * @param op the operation, which matches left to right
         * @param right the matching fields
         * @param left the condition
         * @return if the translation was successful
         */
        private boolean translateArray( String op, RexNode right, RexNode left ) {
            if ( right instanceof RexCall && left instanceof RexIndexRef ) {
                // $9 ( index ) -> [el1, el2]
                String name = getPhysicalName( (RexIndexRef) left );
                attachCondition( op, "$expr", translateCall( name, (RexCall) right ) );
                return true;
            } else if ( right instanceof RexCall && left instanceof RexLiteral ) {
                if ( right.isA( Kind.DISTANCE ) ) {
                    translateFunction( op, (RexCall) right, left );

                } else if ( right.isA( Kind.MOD ) ) {
                    return translateMod( (RexCall) right, left );

                } else {
                    // $9[1] -> el1
                    String name = getPhysicalName( (RexIndexRef) ((RexCall) right).operands.get( 0 ) );
                    // we have to adjust as mongodb arrays start at 0 and sql at 1
                    int pos = ((RexLiteral) ((RexCall) right).operands.get( 1 )).value.asInteger().value - 1;
                    translateOp2( null, name + "." + pos, (RexLiteral) left );
                }

                return true;
            }
            return false;
        }


        private boolean translateMod( RexCall node, RexNode comp ) {
            RexNode l = node.operands.get( 0 );
            RexNode r = node.operands.get( 1 );
            if ( l.isA( Kind.CAST ) ) {
                l = ((RexCall) l).operands.get( 0 );
            }
            if ( r.isA( Kind.CAST ) ) {
                r = ((RexCall) r).operands.get( 0 );
            }
            String name = getParamAsKey( l );
            BsonValue rNode = getParamAsValue( r );

            BsonValue compNode = getParamAsValue( comp );

            attachCondition( null, name,
                    new BsonDocument().append( "$mod", new BsonArray( Arrays.asList( rNode, compNode ) ) ) );

            return true;
        }


        /**
         * Translates the RexCall into its appropriate form
         * <p>
         * left:[right]
         *
         * @param left the corresponding field
         * @param right the matching clause
         * @return the condition in the form  e.g. left -> [{"$eq": 3}, {"$lt": 15}]
         */
        private BsonArray translateCall( String left, RexCall right ) {
            BsonArray array = new BsonArray();
            array.add( 0, new BsonString( "$" + left ) );
            array.add( getArray( right ) );
            return array;
        }


        /**
         * Translates the right side of an assignment
         *
         * @param right the matching side of a single the assignment
         * @return a single document, which represents the condition
         */
        private BsonDocument getArray( RexCall right ) {
            BsonArray array = new BsonArray( right.operands.stream().map( el -> {
                if ( el.isA( Kind.INPUT_REF ) ) {
                    return new BsonString( getPhysicalName( (RexIndexRef) el ) );
                } else if ( el.isA( Kind.DYNAMIC_PARAM ) ) {
                    return new BsonDynamic( (RexDynamicParam) el );
                } else if ( el.isA( Kind.LITERAL ) ) {
                    return BsonUtil.getAsBson( (RexLiteral) el, bucket );
                } else if ( el instanceof RexCall ) {
                    return getOperation( ((RexCall) el).op, ((RexCall) el).operands );
                } else {
                    throw new GenericRuntimeException( "Input in array is not translatable." );
                }
            } ).collect( Collectors.toList() ) );
            if ( right.op.getKind() == Kind.CAST ) {
                if ( array.size() == 1 ) {
                    return (BsonDocument) array.get( 0 );
                } else {
                    return new BsonDocument( "$and", array );
                }
            }
            return new BsonDocument( getOp( right.op ), array );
        }


        /**
         * Translates the multiple conditions, which belong to the given operation
         *
         * @param op the operation e.g. Kind.EQUALS, which translates to "$eq"
         * @param operands the fields, which have to match the given operation
         * @return the operations as a single value
         */
        private BsonValue getOperation( Operator op, ImmutableList<RexNode> operands ) {
            String operator = getOp( op );

            return new BsonDocument( operator, new BsonArray( operands.stream().map( this::getSingle ).collect( Collectors.toList() ) ) );

        }


        /**
         * Translates a single assign value.
         *
         * @param node the node to translate
         * @return the node in its BSON form
         */
        private BsonValue getSingle( RexNode node ) {
            if ( node.isA( Kind.LITERAL ) ) {
                return BsonUtil.getAsBson( (RexLiteral) node, bucket );
            } else if ( node.isA( Kind.DYNAMIC_PARAM ) ) {
                return new BsonDynamic( (RexDynamicParam) node );
            } else if ( node instanceof RexCall && ((RexCall) node).op.getKind() == Kind.CAST ) {
                return getSingle( ((RexCall) node).operands.get( 0 ) );
            } else {
                throw new RuntimeException( "operations need to consist of literals" );
            }
        }


        /**
         * Translate the operation to its BSON from
         *
         * @param op the operation to transform e.g. Kind.PLUS
         * @return The operation translated
         */
        private String getOp( Operator op ) {
            switch ( op.getKind() ) {
                case PLUS:
                    return "$add";
                case MINUS:
                    return "$substr";
                case TIMES:
                    return "$multiply";
                case DIVIDE:
                    return "$divide";
                case ARRAY_VALUE_CONSTRUCTOR:
                    return "$eq";
                default:
                    throw new GenericRuntimeException( "Sql operation is not supported" );
            }
        }


        /**
         * Tries to translate the given left, right combination to a BSON  function form
         *
         * @param op the operation, which specifies how left matches right
         * @param right the matching conditions
         * @param left the matching filed
         * @return if the translation was possible, with the given parameters
         */
        private boolean translateFunction( String op, RexCall right, RexNode left ) {
            String randomName = getRandomName();
            this.preProjections.put( randomName, BsonFunctionHelper.getFunction( right, rowType, implementor ) );

            switch ( left.getKind() ) {
                case LITERAL:
                    attachCondition( op, randomName, BsonUtil.getAsBson( (RexLiteral) left, bucket ) );
                    return true;
                case DYNAMIC_PARAM:
                    attachCondition( op, randomName, new BsonDynamic( (RexDynamicParam) left ) );
                    return true;
                default:
                    return false;
            }

        }


        /**
         * Returns a random name, which grows with increasing pre-projections.
         *
         * @return the name
         */
        private String getRandomName() {
            return "__temp" + preProjections.size();
        }


        /**
         * Translates a call to a binary operator. Returns whether successful.
         */
        private boolean translateBinary2( String op, RexNode left, RexNode right ) {
            return switch ( right.getKind() ) {
                case LITERAL -> translateLiteral( op, left, (RexLiteral) right );
                case DYNAMIC_PARAM -> translateDynamic( op, left, (RexDynamicParam) right );
                default -> false;
            };
        }


        /**
         * Translate a condition, which includes a RexLiteral
         *
         * @param op the operation, which defines how the condition has to match the field
         * @param left a general field, which is not yet identified
         * @param right a field, which is a RexLiteral
         * @return if the translation was possible
         */
        private boolean translateLiteral( String op, RexNode left, RexLiteral right ) {
            switch ( left.getKind() ) {
                case INPUT_REF:
                    translateOp2( op, getPhysicalName( (RexIndexRef) left ), right );
                    return true;

                case CAST:
                    return translateBinary2( op, ((RexCall) left).operands.get( 0 ), right );

                case OTHER_FUNCTION:
                    String itemName = MongoRules.isItem( (RexCall) left );
                    if ( itemName != null ) {
                        translateOp2( op, itemName, right );
                        return true;
                    }

                case MQL_QUERY_VALUE:
                    return translateDocValue( op, (RexNameRef) left, right );

                case ELEMENT_REF:
                    if ( this.tempElem != null ) {
                        translateOp2( op, getPhysicalName( (RexIndexRef) left ), right );
                        return false;
                    }

                    translateElemMatch( (RexCall) left );
                    return true;

                // fall through

                default:
                    return false;
            }
        }


        /**
         * Translates a field, which contains a RexDynamicParam
         *
         * @param op the operation, which describes how the left field has to match the RexDynamicParam
         * @param left a not yet identified condition
         * @param right the RexDynamicParam
         * @return if the translation was possible
         */
        private boolean translateDynamic( String op, RexNode left, RexDynamicParam right ) {
            return switch ( left.getKind() ) {
                case INPUT_REF -> {
                    attachCondition( op, getPhysicalName( (RexIndexRef) left ), new BsonDynamic( right ) );
                    yield true;
                }
                case ELEMENT_REF -> {
                    if ( this.tempElem == null ) {
                        yield false;
                    }
                    attachCondition( op, this.tempElem.unwrap( RexNameRef.class ).orElseThrow().getName(), new BsonDynamic( right ) );
                    yield true;
                }
                case DISTANCE -> translateFunction( op, (RexCall) left, right );
                case OTHER_FUNCTION -> translateItem( op, (RexCall) left, right );
                case MOD -> translateMod( (RexCall) left, right );
                case NAME_INDEX_REF -> translateDocValue( op, (RexNameRef) left, right );
                case CAST -> translateDynamic( op, ((RexCall) left).operands.get( 0 ), right );
                default -> false;
            };

        }


        /**
         * Translates a DOC_QUERY_VALUE ( document model )
         * to the correct BSON form
         *
         * @param op the operation, which specifies how the left has to match right
         * @param left
         * @param right the condition, which has to match the DOC_QUERY_VALUE
         * @return if the translation was successful
         */
        private boolean translateDocValue( String op, RexNameRef left, RexNode right ) {
            BsonValue item = getItem( right );
            if ( item == null ) {
                return false;
            }
            /*if ( !left.getOperands().get( 0 ).isA( Kind.INPUT_REF ) || left.operands.size() != 2 ) {
                return false;
            }*/
            /*if ( left.operands.get( 1 ) instanceof RexDynamicParam || left.operands.get( 1 ) instanceof RexCall ) {
                if ( left.operands.get( 1 ) instanceof RexCall && left.operands.get( 1 ).isA( Kind.ARRAY_VALUE_CONSTRUCTOR ) &&
                        ((RexCall) left.operands.get( 1 )).operands.size() == 0 && this.tempElem != null ) {
                    String mergedName = handleElemMatch( left );

                    attachCondition( op, mergedName, item );
                    return true;
                }

                attachCondition( op, MongoRules.translateDocValueAsKey( rowType, left ), item );
                return true;
            }*/
            attachCondition( op, MongoRules.translateDocValueAsKey( rowType, left ), item );
            return true;
            /*if ( !(left.operands.get( 1 ) instanceof RexCall) || !left.getOperands().get( 1 ).isA( Kind.ARRAY_VALUE_CONSTRUCTOR ) ) {
                return false;
            }

            RexInputRef parent = (RexInputRef) left.getOperands().get( 0 );
            RexCall names = (RexCall) left.operands.get( 1 );
            if ( names.isA( Kind.ARRAY_VALUE_CONSTRUCTOR ) && names.operands.size() == 0 && this.tempElem != null ) {
                names = (RexCall) ((RexCall) this.tempElem).operands.get( 1 );
            }

            String mergedName = rowType.getFieldNames().get( parent.getIndex() );

            if ( names.operands.size() > 0 ) {
                mergedName += "." + names.operands
                        .stream()
                        .map( name -> ((RexLiteral) name).getValue( String.class ) )
                        .collect( Collectors.joining( "." ) );
            }

            attachCondition( op, mergedName, item );
            return true;*/
        }


        private String handleElemMatch( RexNameRef left ) {
            RexCall names = (RexCall) ((RexCall) this.tempElem).operands.get( 1 );
            //RexIndexRef parent = (RexIndexRef) left.getOperands().get( 0 );
            String mergedName = left.name;

            if ( !names.operands.isEmpty() ) {
                mergedName += "." + names.operands
                        .stream()
                        .map( name -> ((RexLiteral) name).value.asString().value )
                        .collect( Collectors.joining( "." ) );
            }
            return mergedName;
        }


        /**
         * Tries to translate the given condition as a Kind.ITEM
         *
         * @param op the operation, which specifies how the left has to match right
         * @param left the unparsed ITEM
         * @param right the field to which the ITEM belongs
         * @return if the translation was successful
         */
        private boolean translateItem( String op, RexCall left, RexDynamicParam right ) {
            if ( left.op instanceof SqlItemOperator ) {
                RexNode l = left.operands.get( 0 );
                RexNode r = left.operands.get( 1 );

                if ( l.isA( Kind.INPUT_REF ) ) {
                    BsonValue item = getItem( r );
                    if ( item == null ) {
                        return false;
                    }
                    String name = getRandomName();
                    BsonArray array = new BsonArray(
                            Arrays.asList(
                                    new BsonString( "$" + getPhysicalName( (RexIndexRef) l ) ),
                                    new BsonDocument( "$add", new BsonArray( Arrays.asList( item, new BsonInt32( -1 ) ) ) ) ) );
                    this.preProjections.put( name, new BsonDocument( "$arrayElemAt", array ) );

                    attachCondition( null, name, new BsonDynamic( right ) );

                    return true;
                }
            }
            return false;
        }


        @Nullable
        private BsonValue getItem( RexNode r ) {
            BsonValue item;
            if ( r.isA( Kind.LITERAL ) ) {
                item = BsonUtil.getAsBson( (RexLiteral) r, bucket );
            } else if ( r.isA( Kind.DYNAMIC_PARAM ) ) {
                item = new BsonDynamic( (RexDynamicParam) r );
            } else if ( r.isA( Kind.NAME_INDEX_REF ) ) {
                item = MongoRules.translateDocValue( rowType, (RexNameRef) r );
            } else if ( r instanceof RexCall ) {
                item = getArray( (RexCall) r );
            } else {
                return null;
            }
            return item;
        }


        private String getPhysicalName( RexIndexRef input ) {
            String name = fieldNames.get( input.getIndex() );
            // DML (and also DDL) have to use the physical name, as they do not allow
            // to use projections beforehand
            if ( implementor.isDML() ) {
                if ( rowType != null && rowType.getFields().get( input.getIndex() ) != null ) {
                    name = rowType.getFields().get( input.getIndex() ).getPhysicalName();
                }
                return name;
            }
            implementor.physicalMapper.add( name );
            return name;
        }


        private void translateOp2( String op, String name, RexLiteral right ) {
            attachCondition( op, name, BsonUtil.getAsBson( right, bucket ) );
        }


        /**
         * Attaches the provided condition to the map of conditions
         *
         * @param op the used operation
         * @param name the key, which specifies the field
         * @param right specifies the condition, which matches name
         */
        private void attachCondition( String op, String name, BsonValue right ) {
            // right is a "single" statement, which needs an "$eq" for complex statements
            /*if ( op == null && ((!right.isDocument() && !right.isArray()) || right instanceof BsonDynamic) && !(right instanceof BsonRegularExpression) ) {
                op = "$eq";
            }*/
            // E.g.: {deptno: 100}
            BsonValue adjustedRight = right;

            if ( op != null ) {
                // E.g. {deptno: {$lt: 100}} which may later be combined with other conditions: E.g. {deptno: [$lt: 100, $gt: 50]}
                adjustedRight = new BsonDocument( op, adjustedRight );
            }

            if ( modifier != null ) {
                if ( adjustedRight instanceof BsonKeyValue ) {
                    // $op : {keyValue}
                    adjustedRight = ((BsonKeyValue) adjustedRight).wrapValue( v -> new BsonDocument( modifier, v ) );
                } else {
                    if ( !adjustedRight.isDocument() || (adjustedRight instanceof BsonDynamic dynamic && !dynamic.isRegex()) ) {
                        // $op : [value]
                        modifier = "$ne";
                    }
                    adjustedRight = new BsonDocument( modifier, adjustedRight );

                }
            }

            //E.g. {deptno: [$lt: 100, $gt: 50]}
            if ( map.containsKey( name ) ) {
                map.get( name ).add( adjustedRight );
            } else {
                map.put( name, new ArrayList<>( Collections.singletonList( adjustedRight ) ) );
            }

        }

    }

}

