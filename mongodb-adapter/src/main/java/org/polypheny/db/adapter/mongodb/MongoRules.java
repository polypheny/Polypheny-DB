/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.mongodb;

import com.google.common.collect.ImmutableList;
import com.mongodb.client.gridfs.GridFSBucket;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.enumerable.RexImpTable;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.mongodb.MongoAlg.Implementor;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Documents;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.TableModify;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.sql.sql.fun.SqlDatetimePlusOperator;
import org.polypheny.db.sql.sql.fun.SqlDatetimeSubtractionOperator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.ValidatorUtil;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for {@link MongoAlg#CONVENTION MONGO} calling convention.
 */
public class MongoRules {

    private MongoRules() {
    }


    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();
    public static final MongoConvention convention = MongoConvention.INSTANCE;

    @Getter
    public static final AlgOptRule[] RULES = {
            MongoToEnumerableConverterRule.INSTANCE,
            MongoValuesRule.INSTANCE,
            MongoSortRule.INSTANCE,
            MongoFilterRule.INSTANCE,
            MongoProjectRule.INSTANCE,
            MongoAggregateRule.INSTANCE,
            MongoTableModificationRule.INSTANCE
    };


    /**
     * Returns 'string' if it is a call to item['string'], null otherwise.
     */
    static String isItem( RexCall call ) {
        if ( call.getOperator().getOperatorName() != OperatorName.ITEM ) {
            return null;
        }
        final RexNode op0 = call.operands.get( 0 );
        final RexNode op1 = call.operands.get( 1 );
        if ( op0 instanceof RexInputRef
                && ((RexInputRef) op0).getIndex() == 0
                && op1 instanceof RexLiteral
                && ((RexLiteral) op1).getValue2() instanceof String ) {
            return (String) ((RexLiteral) op1).getValue2();
        }
        /*if ( op0 instanceof RexInputRef && op1 instanceof RexDynamicParam ) {
            return new BsonDynamic( (RexDynamicParam) op1 ).toJson();
        }*/

        if ( op0.getType().getPolyType() == PolyType.ARRAY & op1 instanceof RexLiteral && op0 instanceof RexInputRef ) {
            return null;
        }
        return null;
    }


    static List<String> mongoFieldNames( final AlgDataType rowType ) {
        return ValidatorUtil.uniquify(
                new AbstractList<String>() {
                    @Override
                    public String get( int index ) {
                        final String name = MongoRules.maybeFix( rowType.getFieldList().get( index ).getName() );
                        return name.startsWith( "$" ) ? "_" + name.substring( 2 ) : name;
                    }


                    @Override
                    public int size() {
                        return rowType.getFieldCount();
                    }
                },
                ValidatorUtil.EXPR_SUGGESTER, true );
    }


    static String maybeQuote( String s ) {
        if ( !needsQuote( s ) ) {
            return s;
        }
        return quote( s );
    }


    static String quote( String s ) {
        return "'" + s + "'"; // TODO: handle embedded quotes
    }


    private static boolean needsQuote( String s ) {
        for ( int i = 0, n = s.length(); i < n; i++ ) {
            char c = s.charAt( i );
            if ( !Character.isJavaIdentifierPart( c ) || c == '$' ) {
                return true;
            }
        }
        return false;
    }


    public static String maybeFix( String name ) {
        if ( name.contains( "." ) ) {
            String[] splits = name.split( "\\." );
            return splits[splits.length - 1];
        }
        return name;
    }


    public static Pair<String, RexNode> getAddFields( RexCall call, AlgDataType rowType ) {
        assert call.operands.size() == 3;
        assert call.operands.get( 0 ) instanceof RexInputRef;
        assert call.operands.get( 1 ) instanceof RexLiteral;
        String field = rowType.getFieldNames().get( ((RexInputRef) call.operands.get( 0 )).getIndex() );
        field += "." + ((RexLiteral) call.operands.get( 1 )).getValueAs( String.class );
        return new Pair<>( field, call.operands.get( 2 ) );
    }


    public static String adjustName( String name ) {
        return name.startsWith( "$" )
                ? "_" + maybeFix( name.substring( 2 ) )
                : maybeFix( name );
    }


    /**
     * Translator from {@link RexNode} to strings in MongoDB's expression language.
     */
    static class RexToMongoTranslator extends RexVisitorImpl<String> {

        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;

        static final Map<Operator, String> MONGO_OPERATORS = new HashMap<>();


        static {
            // Arithmetic
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.DIVIDE ), "$divide" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.MULTIPLY ), "$multiply" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.MOD ), "$mod" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.PLUS ), "$add" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.MINUS ), "$subtract" );
            // Boolean
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.AND ), "$and" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.OR ), "$or" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.NOT ), "$not" );
            // Comparison
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.EQUALS ), "$eq" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.NOT_EQUALS ), "$ne" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.GREATER_THAN ), "$gt" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), "$gte" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LESS_THAN ), "$lt" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), "$lte" );

            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.FLOOR ), "$floor" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.CEIL ), "$ceil" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.EXP ), "$exp" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LN ), "$ln" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LOG10 ), "$log10" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ABS ), "$abs" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.CHAR_LENGTH ), "$strLenCP" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.SUBSTRING ), "$substrCP" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ROUND ), "$round" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ACOS ), "$acos" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.TAN ), "$tan" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.COS ), "$cos" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ASIN ), "$asin" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.SIN ), "$sin" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ATAN ), "$atan" );
            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ATAN2 ), "$atan2" );

            MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.POWER ), "$pow" );
        }


        private final Implementor implementor;


        protected RexToMongoTranslator( JavaTypeFactory typeFactory, List<String> inFields, Implementor implementor ) {
            super( true );
            this.implementor = implementor;
            this.typeFactory = typeFactory;
            this.inFields = inFields;
        }


        @Override
        public String visitLiteral( RexLiteral literal ) {
            if ( literal.getValue() == null ) {
                return "null";
            }
            return "{$literal: " + RexToLixTranslator.translateLiteral( literal, literal.getType(), typeFactory, RexImpTable.NullAs.NOT_POSSIBLE ) + "}";
        }


        @Override
        public String visitDynamicParam( RexDynamicParam dynamicParam ) {
            return new BsonDynamic( dynamicParam ).toJson();
        }


        @Override
        public String visitInputRef( RexInputRef inputRef ) {
            implementor.physicalMapper.add( inFields.get( inputRef.getIndex() ) );
            return maybeQuote( "$" + inFields.get( inputRef.getIndex() ) );
        }


        @Override
        public String visitCall( RexCall call ) {
            String name = isItem( call );
            if ( name != null ) {
                return "'$" + name + "'";
            }
            final List<String> strings = translateList( call.operands );
            if ( call.getKind() == Kind.CAST ) {
                return strings.get( 0 );
            }
            String stdOperator = MONGO_OPERATORS.get( call.getOperator() );
            if ( stdOperator != null ) {
                if ( call.getOperator().equals( OperatorRegistry.get( OperatorName.SUBSTRING ) ) ) {
                    String first = strings.get( 1 );
                    first = "{\"$subtract\":[" + first + ", 1]}";
                    strings.remove( 1 );
                    strings.add( first );
                    if ( call.getOperands().size() == 2 ) {
                        strings.add( " { \"$strLenCP\":" + strings.get( 0 ) + "}" );
                    }
                }
                return "{" + stdOperator + ": [" + Util.commaList( strings ) + "]}";
            }
            if ( call.getOperator().getOperatorName() == OperatorName.ITEM ) {
                final RexNode op1 = call.operands.get( 1 );
                // normal
                if ( op1 instanceof RexLiteral && op1.getType().

                        getPolyType() == PolyType.INTEGER ) {
                    return "{$arrayElemAt:[" + strings.get( 0 ) + "," + (((RexLiteral) op1).getValueAs( Integer.class ) - 1) + "]}";
                }
                // prepared
                if ( op1 instanceof RexDynamicParam ) {
                    return "{$arrayElemAt:[" + strings.get( 0 ) + ", {$subtract:[" + new BsonDynamic( (RexDynamicParam) op1 ).toJson() + ", 1]}]}";
                }
            }
            if ( call.getOperator().equals( OperatorRegistry.get( OperatorName.CASE ) ) ) {
                StringBuilder sb = new StringBuilder();
                StringBuilder finish = new StringBuilder();
                // case(a, b, c)  -> $cond:[a, b, c]
                // case(a, b, c, d) -> $cond:[a, b, $cond:[c, d, null]]
                // case(a, b, c, d, e) -> $cond:[a, b, $cond:[c, d, e]]
                for (
                        int i = 0; i < strings.size(); i += 2 ) {
                    sb.append( "{$cond:[" );
                    finish.append( "]}" );

                    sb.append( strings.get( i ) );
                    sb.append( ',' );
                    sb.append( strings.get( i + 1 ) );
                    sb.append( ',' );
                    if ( i == strings.size() - 3 ) {
                        sb.append( strings.get( i + 2 ) );
                        break;
                    }
                    if ( i == strings.size() - 2 ) {
                        sb.append( "null" );
                        break;
                    }
                }
                sb.append( finish );
                return sb.toString();
            }
            if ( call.op.equals( OperatorRegistry.get( OperatorName.UNARY_MINUS ) ) ) {
                if ( strings.size() == 1 ) {
                    return "{\"$multiply\":[" + strings.get( 0 ) + ",-1]}";
                }
            }

            String special = handleSpecialCases( call );
            if ( special != null ) {
                return special;
            }
            if ( call.op.getOperatorName() == OperatorName.MQL_JSONIFY ) {
                return call.operands.get( 0 ).accept( this );
            }

            if ( call.op.getOperatorName() == OperatorName.SIGN ) {
                // x < 0, -1
                // x == 0, 0
                // x > 0, 1
                StringBuilder sb = new StringBuilder();
                String oper = call.operands.get( 0 ).accept( this );
                sb.append( "{\"$switch\":\n"
                        + "            {\n"
                        + "              \"branches\": [\n"
                        + "                {\n"
                        + "                  \"case\": { \"$lt\" : [ " );
                sb.append( oper );
                sb.append( ", 0 ] },\n"
                        + "                  \"then\": -1.0"
                        + "                },\n"
                        + "                {\n"
                        + "                  \"case\": { \"$gt\" : [ " );
                sb.append( oper );
                sb.append( ", 0 ] },\n"
                        + "                  \"then\": 1.0"
                        + "                },\n"
                        + "              ],\n"
                        + "              \"default\": 0.0"
                        + "            }}" );

                return sb.toString();
            }

            if ( call.op.equals( OperatorRegistry.get( OperatorName.IS_NOT_NULL ) ) ) {
                return call.operands.get( 0 ).

                        accept( this );

            }

            throw new

                    IllegalArgumentException( "Translation of " + call + " is not supported by MongoProject" );


        }


        public String handleSpecialCases( RexCall call ) {
            if ( call.getType().getPolyType() == PolyType.ARRAY ) {
                BsonArray array = new BsonArray();
                array.addAll( translateList( call.operands ).stream().map( BsonString::new ).collect( Collectors.toList() ) );
                return array.toString();
            } else if ( call.isA( Kind.MQL_QUERY_VALUE ) ) {
                return RexToMongoTranslator.translateDocValue( implementor.getStaticRowType(), call );

            } else if ( call.isA( Kind.MQL_ITEM ) ) {
                RexNode leftPre = call.operands.get( 0 );
                String left = leftPre.accept( this );

                String right = call.operands.get( 1 ).accept( this );

                return "{\"$arrayElemAt\":[" + left + "," + right + "]}";
            } else if ( call.isA( Kind.MQL_SLICE ) ) {
                String left = call.operands.get( 0 ).accept( this );
                String skip = call.operands.get( 1 ).accept( this );
                String return_ = call.operands.get( 2 ).accept( this );

                return "{\"$slice\":[ " + left + "," + skip + "," + return_ + "]}";
            } else if ( call.isA( Kind.MQL_EXCLUDE ) ) {
                String parent = implementor
                        .getStaticRowType()
                        .getFieldNames()
                        .get( ((RexInputRef) call.operands.get( 0 )).getIndex() );

                if ( !(call.operands.get( 1 ) instanceof RexCall) || call.operands.size() != 2 ) {
                    return null;
                }
                RexCall excludes = (RexCall) call.operands.get( 1 );
                List<String> fields = new ArrayList<>();
                for ( RexNode operand : excludes.operands ) {
                    if ( !(operand instanceof RexCall) ) {
                        return null;
                    }
                    fields.add( "\"" + parent + "." + ((RexCall) operand)
                            .operands
                            .stream()
                            .map( op -> ((RexLiteral) op).getValueAs( String.class ) )
                            .collect( Collectors.joining( "." ) ) + "\": 0" );
                }

                return String.join( ",", fields );
            } else if ( call.isA( Kind.UNWIND ) ) {
                return call.operands.get( 0 ).accept( this );
            }
            return null;
        }


        public static String translateDocValue( AlgDataType rowType, RexCall call ) {
            RexInputRef parent = (RexInputRef) call.getOperands().get( 0 );
            RexCall names = (RexCall) call.operands.get( 1 );
            return "\"$" + rowType.getFieldNames().get( parent.getIndex() )
                    + "."
                    + names.operands
                    .stream()
                    .map( n -> ((RexLiteral) n).getValueAs( String.class ) )
                    .collect( Collectors.joining( "." ) ) + "\"";
        }


        private String stripQuotes( String s ) {
            return s.startsWith( "'" ) && s.endsWith( "'" )
                    ? s.substring( 1, s.length() - 1 )
                    : s;
        }


        public List<String> translateList( List<RexNode> list ) {
            final List<String> strings = new ArrayList<>();
            for ( RexNode node : list ) {
                strings.add( node.accept( this ) );
            }
            return strings;
        }

    }


    /**
     * Base class for planner rules that convert a relational expression to MongoDB calling convention.
     */
    abstract static class MongoConverterRule extends ConverterRule {

        protected final Convention out;


        <R extends AlgNode> MongoConverterRule( Class<R> clazz, Predicate<? super R> supports, AlgTrait in, Convention out, String description ) {
            super( clazz, supports, in, out, AlgFactories.LOGICAL_BUILDER, description );
            this.out = out;
        }

    }


    /**
     * Rule to convert a {@link Sort} to a {@link MongoSort}.
     */
    private static class MongoSortRule extends MongoConverterRule {

        public static final MongoSortRule INSTANCE = new MongoSortRule();


        private MongoSortRule() {
            super( Sort.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, "MongoSortRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final Sort sort = (Sort) alg;
            final AlgTraitSet traitSet = sort.getTraitSet().replace( out ).replace( sort.getCollation() );
            return new MongoSort(
                    alg.getCluster(),
                    traitSet,
                    convert( sort.getInput(), traitSet.replace( AlgCollations.EMPTY ) ),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch );
        }

    }


    /**
     * Rule to convert a {@link org.polypheny.db.algebra.logical.LogicalFilter} to a {@link MongoFilter}.
     */
    private static class MongoFilterRule extends MongoConverterRule {

        private static final MongoFilterRule INSTANCE = new MongoFilterRule();


        private MongoFilterRule() {
            super(
                    LogicalFilter.class,
                    project -> MongoConvention.mapsDocuments || !DocumentRules.containsDocument( project ),
                    Convention.NONE,
                    MongoAlg.CONVENTION,
                    "MongoFilterRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalFilter filter = (LogicalFilter) alg;
            final AlgTraitSet traitSet = filter.getTraitSet().replace( out );
            return new MongoFilter(
                    alg.getCluster(),
                    traitSet,
                    convert( filter.getInput(), out ),
                    filter.getCondition() );
        }

    }


    /**
     * Rule to convert a {@link org.polypheny.db.algebra.logical.LogicalProject} to a {@link MongoProject}.
     */
    private static class MongoProjectRule extends MongoConverterRule {

        private static final MongoProjectRule INSTANCE = new MongoProjectRule();


        private MongoProjectRule() {
            super(
                    LogicalProject.class,
                    project -> (MongoConvention.mapsDocuments || !DocumentRules.containsDocument( project ))
                            && !containsIncompatible( project ),
                    Convention.NONE,
                    MongoAlg.CONVENTION,
                    "MongoProjectRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalProject project = (LogicalProject) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( out );
            return new MongoProject(
                    project.getCluster(),
                    traitSet,
                    convert( project.getInput(), out ),
                    project.getProjects(),
                    project.getRowType() );
        }

    }


    private static boolean containsIncompatible( SingleAlg alg ) {
        MongoExcludeVisitor visitor = new MongoExcludeVisitor();
        for ( RexNode node : alg.getChildExps() ) {
            node.accept( visitor );
            if ( visitor.isContainsIncompatible() ) {
                return true;
            }
        }
        return false;
    }


    /**
     * This visitor is used to exclude different function for MongoProject and MongoFilters,
     * some of them are not supported or would need to use the Javascript engine extensively,
     * which for one have similar performance to a JavaScript implementation but don't need
     * maintenance
     */
    public static class MongoExcludeVisitor extends RexVisitorImpl<Void> {

        @Getter
        private boolean containsIncompatible = false;


        protected MongoExcludeVisitor() {
            super( true );
        }


        @Override
        public Void visitCall( RexCall call ) {
            Operator operator = call.getOperator();
            if ( operator.getOperatorName() == OperatorName.COALESCE
                    || operator.getOperatorName() == OperatorName.EXTRACT
                    || operator.getOperatorName() == OperatorName.OVERLAY
                    || operator.getOperatorName() == OperatorName.COT
                    || operator.getOperatorName() == OperatorName.FLOOR
                    || (operator.getOperatorName() == OperatorName.CAST
                    && call.operands.get( 0 ).getType().getPolyType() == PolyType.DATE)
                    || operator instanceof SqlDatetimeSubtractionOperator
                    || operator instanceof SqlDatetimePlusOperator ) {
                containsIncompatible = true;
            }
            return super.visitCall( call );
        }

    }


    public static class MongoValuesRule extends MongoConverterRule {

        private static final MongoValuesRule INSTANCE = new MongoValuesRule();


        private MongoValuesRule() {
            super( Values.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, "MongoValuesRule." + MongoAlg.CONVENTION );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            Values values = (Values) alg;
            if ( values.getModel() == SchemaType.DOCUMENT ) {
                Documents documents = (Documents) alg;
                return new MongoDocuments(
                        alg.getCluster(),
                        alg.getRowType(),
                        documents.getDocumentTuples(),
                        alg.getTraitSet().replace( out ),
                        values.getTuples()
                );
            }
            return new MongoValues(
                    values.getCluster(),
                    values.getRowType(),
                    values.getTuples(),
                    values.getTraitSet().replace( out ) );
        }

    }


    public static class MongoValues extends Values implements MongoAlg {

        MongoValues( AlgOptCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traitSet ) {
            super( cluster, rowType, tuples, traitSet );
        }


        @Override
        public void implement( Implementor implementor ) {

        }

    }


    public static class MongoDocuments extends Values implements MongoAlg {


        private final ImmutableList<BsonValue> documentTuples;


        public MongoDocuments( AlgOptCluster cluster, AlgDataType defaultRowType, ImmutableList<BsonValue> documentTuples, AlgTraitSet traitSet, ImmutableList<ImmutableList<RexLiteral>> normalizedTuples ) {
            super( cluster, defaultRowType, normalizedTuples, traitSet );
            //this.tuples = normalizedTuples;
            this.documentTuples = documentTuples;
        }


        @Override
        public void implement( Implementor implementor ) {
            // empty on purpose
        }

    }


    private static class MongoTableModificationRule extends MongoConverterRule {

        private static final MongoTableModificationRule INSTANCE = new MongoTableModificationRule();


        MongoTableModificationRule() {
            super( TableModify.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, "MongoTableModificationRule." + MongoAlg.CONVENTION );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final TableModify modify = (TableModify) alg;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            if ( modifiableTable == null ) {
                return null;
            }
            if ( modify.getTable().unwrap( MongoTable.class ) == null ) {
                return null;
            }

            final AlgTraitSet traitSet = modify.getTraitSet().replace( out );
            return new MongoTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    AlgOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened() );
        }

    }


    private static class MongoTableModify extends TableModify implements MongoAlg {


        private final GridFSBucket bucket;
        private Implementor implementor;


        protected MongoTableModify(
                AlgOptCluster cluster,
                AlgTraitSet traitSet,
                AlgOptTable table,
                CatalogReader catalogReader,
                AlgNode input,
                Operation operation,
                List<String> updateColumnList,
                List<RexNode> sourceExpressionList,
                boolean flattened ) {
            super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
            this.bucket = table.unwrap( MongoTable.class ).getMongoSchema().getBucket();
        }


        @Override
        public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new MongoTableModify(
                    getCluster(),
                    traitSet,
                    getTable(),
                    getCatalogReader(),
                    AbstractAlgNode.sole( inputs ),
                    getOperation(),
                    getUpdateColumnList(),
                    getSourceExpressionList(),
                    isFlattened() );
        }


        @Override
        public void implement( Implementor implementor ) {
            implementor.setDML( true );
            Table preTable = table.getTable();
            this.implementor = implementor;

            if ( !(preTable instanceof MongoTable) ) {
                throw new RuntimeException( "There seems to be a problem with the correct costs for one of stores." );
            }
            implementor.mongoTable = (MongoTable) preTable;
            implementor.table = table;
            implementor.setOperation( this.getOperation() );

            switch ( this.getOperation() ) {
                case INSERT:
                    if ( input instanceof MongoValues ) {
                        handleDirectInsert( implementor, ((MongoValues) input) );
                    } else if ( input instanceof MongoDocuments ) {
                        handleDocumentInsert( implementor, ((MongoDocuments) input) );
                    } else if ( input instanceof MongoProject ) {
                        handlePreparedInsert( implementor, ((MongoProject) input) );
                    } else {
                        return;
                    }
                    break;
                case UPDATE:
                    MongoAlg.Implementor condImplementor = new Implementor( true );
                    condImplementor.setStaticRowType( implementor.getStaticRowType() );
                    ((MongoAlg) input).implement( condImplementor );
                    implementor.filter = condImplementor.filter;
                    assert condImplementor.getStaticRowType() instanceof MongoRowType;
                    MongoRowType rowType = (MongoRowType) condImplementor.getStaticRowType();
                    int pos = 0;
                    BsonDocument doc = new BsonDocument();
                    List<BsonDocument> docDocs = new ArrayList<>();
                    GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();
                    for ( RexNode el : getSourceExpressionList() ) {
                        if ( el instanceof RexLiteral ) {
                            doc.append(
                                    rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                                    BsonUtil.getAsBson( (RexLiteral) el, bucket ) );
                        } else if ( el instanceof RexCall ) {
                            if ( ((RexCall) el).op.getKind() == Kind.PLUS ) {
                                doc.append(
                                        rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                                        visitCall( implementor, (RexCall) el, Kind.PLUS, el.getType().getPolyType() ) );
                            } else if ( ((RexCall) el).op.getKind().belongsTo( Kind.MQL_KIND ) ) {
                                docDocs.add( handleDocumentUpdate( (RexCall) el, bucket, rowType ) );
                            } else {
                                doc.append(
                                        rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                                        BsonUtil.getBsonArray( (RexCall) el, bucket ) );
                            }
                        } else if ( el instanceof RexDynamicParam ) {
                            doc.append(
                                    rowType.getPhysicalName( getUpdateColumnList().get( pos ), implementor ),
                                    new BsonDynamic( (RexDynamicParam) el ) );
                        }
                        pos++;
                    }
                    if ( doc.size() > 0 ) {
                        BsonDocument update = new BsonDocument().append( "$set", doc );

                        implementor.operations = Collections.singletonList( update );
                    } else {
                        implementor.operations = docDocs;
                    }

                    if ( Pair.right( condImplementor.list ).contains( "{$limit: 1}" ) ) {
                        implementor.onlyOne = true;
                    }

                    break;
                case MERGE:
                    break;
                case DELETE:
                    MongoAlg.Implementor filterCollector = new Implementor( true );
                    filterCollector.setStaticRowType( implementor.getStaticRowType() );
                    ((MongoAlg) input).implement( filterCollector );
                    implementor.filter = filterCollector.filter;
                    if ( Pair.right( filterCollector.list ).contains( "{$limit: 1}" ) ) {
                        implementor.onlyOne = true;
                    }

                    break;
            }
        }


        private BsonDocument handleDocumentUpdate( RexCall el, GridFSBucket bucket, MongoRowType rowType ) {
            if ( el.op.getKind() == Kind.MQL_JSONIFY ) {
                assert el.getOperands().size() == 1;
                return handleDocumentUpdate( (RexCall) el.getOperands().get( 0 ), bucket, rowType );
            }

            BsonDocument doc = new BsonDocument();
            assert el.getOperands().size() >= 2;
            assert el.getOperands().get( 0 ) instanceof RexInputRef;

            String key = getDocParentKey( (RexInputRef) el.operands.get( 0 ), rowType );
            attachUpdateStep( doc, el, rowType, key );

            return doc;
        }


        private void attachUpdateStep( BsonDocument doc, RexCall el, MongoRowType rowType, String key ) {
            List<String> keys = getDocUpdateKey( (RexInputRef) el.operands.get( 0 ), (RexCall) el.operands.get( 1 ), rowType );
            switch ( el.op.getKind() ) {
                case MQL_UPDATE_REPLACE:
                    assert el.getOperands().size() == 3;
                    assert el.getOperands().get( 2 ) instanceof RexCall;

                    doc.putAll( getReplaceUpdate( keys, (RexCall) el.operands.get( 2 ) ) );
                    break;
                case MQL_ADD_FIELDS:
                    assert el.getOperands().size() == 3;
                    assert el.getOperands().get( 2 ) instanceof RexCall;

                    doc.putAll( getAddUpdate( keys, (RexCall) el.operands.get( 2 ) ) );
                    break;
                case MQL_UPDATE_REMOVE:
                    assert el.getOperands().size() == 2;

                    doc.putAll( getRemoveUpdate( keys, (RexCall) el.operands.get( 1 ) ) );
                    break;
                case MQL_UPDATE_RENAME:
                    assert el.getOperands().size() == 3;
                    assert el.getOperands().get( 2 ) instanceof RexCall;

                    doc.putAll( getRenameUpdate( keys, key, (RexCall) el.operands.get( 2 ) ) );
                    break;
                default:
                    throw new RuntimeException( "The used update operation is not supported by the MongoDB adapter." );
            }
        }


        private String getDocParentKey( RexInputRef rexInputRef, MongoRowType rowType ) {
            return rowType.getFieldNames().get( rexInputRef.getIndex() );
        }


        private BsonDocument getRenameUpdate( List<String> keys, String parentKey, RexCall call ) {
            BsonDocument doc = new BsonDocument();
            assert keys.size() == call.operands.size();
            int pos = 0;
            for ( String key : keys ) {
                doc.put( key, new BsonString( parentKey + "." + ((RexLiteral) call.operands.get( pos )).getValueAs( String.class ) ) );
                pos++;
            }

            return new BsonDocument( "$rename", doc );
        }


        private BsonDocument getRemoveUpdate( List<String> keys, RexCall call ) {
            BsonDocument doc = new BsonDocument();
            for ( String key : keys ) {
                doc.put( key, new BsonString( "" ) );
            }

            return new BsonDocument( "$unset", doc );
        }


        private BsonDocument getAddUpdate( List<String> keys, RexCall call ) {
            BsonDocument doc = new BsonDocument();
            assert keys.size() == call.operands.size();
            int pos = 0;
            for ( String key : keys ) {
                doc.put( key, BsonUtil.getAsBson( (RexLiteral) call.operands.get( pos ), this.bucket ) );
                pos++;
            }

            return new BsonDocument( "$set", doc );
        }


        private BsonDocument getReplaceUpdate( List<String> keys, RexCall call ) {
            BsonDocument doc = new BsonDocument();
            assert keys.size() == call.operands.size();

            int pos = 0;
            for ( RexNode operand : call.operands ) {
                if ( !(operand instanceof RexCall) ) {
                    doc.append( "$set", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) operand, this.bucket ) ) );
                } else {
                    RexCall op = (RexCall) operand;
                    this.implementor.isDocumentUpdate = true;
                    switch ( op.getKind() ) {
                        case PLUS:
                            doc.append( "$inc", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), this.bucket ) ) );
                            break;
                        case TIMES:
                            doc.append( "$mul", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), this.bucket ) ) );
                            break;
                        case MIN:
                            doc.append( "$min", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), this.bucket ) ) );
                            break;
                        case MAX:
                            doc.append( "$max", new BsonDocument( keys.get( pos ), BsonUtil.getAsBson( (RexLiteral) op.operands.get( 1 ), this.bucket ) ) );
                            break;
                    }
                }
                pos++;
            }

            return doc;
        }


        private List<String> getDocUpdateKey( RexInputRef row, RexCall subfield, MongoRowType rowType ) {
            String name = rowType.getFieldNames().get( row.getIndex() );
            return subfield
                    .operands
                    .stream()
                    .map( n -> ((RexLiteral) n).getValueAs( String.class ) )
                    .map( n -> name + "." + n )
                    .collect( Collectors.toList() );
        }


        private void handleDocumentInsert( Implementor implementor, MongoDocuments documents ) {
            implementor.operations = documents.documentTuples
                    .stream()
                    .filter( BsonValue::isDocument )
                    .map( BsonValue::asDocument )
                    .collect( Collectors.toList() );
        }


        private BsonValue visitCall( Implementor implementor, RexCall call, Kind op, PolyType type ) {
            BsonDocument doc = new BsonDocument();

            BsonArray array = new BsonArray();
            for ( RexNode operand : call.operands ) {
                if ( operand.getKind() == Kind.FIELD_ACCESS ) {
                    String physicalName = "$" + implementor.getPhysicalName( ((RexFieldAccess) operand).getField().getName() );
                    array.add( new BsonString( physicalName ) );
                } else if ( operand instanceof RexCall ) {
                    array.add( visitCall( implementor, (RexCall) operand, ((RexCall) operand).op.getKind(), type ) );
                } else if ( operand.getKind() == Kind.LITERAL ) {
                    array.add( BsonUtil.getAsBson( ((RexLiteral) operand).getValueAs( BsonUtil.getClassFromType( type ) ), type, implementor.mongoTable.getMongoSchema().getBucket() ) );
                } else if ( operand.getKind() == Kind.DYNAMIC_PARAM ) {
                    array.add( new BsonDynamic( (RexDynamicParam) operand ) );
                } else {
                    throw new RuntimeException( "Not implemented yet" );
                }
            }
            if ( op == Kind.PLUS ) {
                doc.append( "$add", array );
            } else if ( op == Kind.MINUS ) {
                doc.append( "$subtract", array );
            } else {
                throw new RuntimeException( "Not implemented yet" );
            }

            return doc;
        }


        private void handlePreparedInsert( Implementor implementor, MongoProject input ) {
            if ( !(input.getInput() instanceof MongoValues) && input.getInput().getRowType().getFieldList().size() == 1 ) {
                return;
            }

            BsonDocument doc = new BsonDocument();
            CatalogTable catalogTable = implementor.mongoTable.getCatalogTable();
            GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();
            Map<Integer, String> physicalMapping = getPhysicalMap( input.getRowType().getFieldList(), catalogTable );

            implementor.setStaticRowType( (AlgRecordType) input.getRowType() );

            int pos = 0;
            for ( RexNode rexNode : input.getChildExps() ) {
                if ( rexNode instanceof RexDynamicParam ) {
                    // preparedInsert
                    doc.append( physicalMapping.get( pos ), new BsonDynamic( (RexDynamicParam) rexNode ) );

                } else if ( rexNode instanceof RexLiteral ) {
                    doc.append( getPhysicalName( input, catalogTable, pos ), BsonUtil.getAsBson( (RexLiteral) rexNode, bucket ) );
                } else if ( rexNode instanceof RexCall ) {
                    PolyType type = table
                            .getTable()
                            .getRowType( getCluster().getTypeFactory() )
                            .getFieldList()
                            .get( pos )
                            .getType()
                            .getComponentType()
                            .getPolyType();

                    doc.append( physicalMapping.get( pos ), getBsonArray( (RexCall) rexNode, type, bucket ) );

                } else if ( rexNode.getKind() == Kind.INPUT_REF && input.getInput() instanceof MongoValues ) {
                    handleDirectInsert( implementor, (MongoValues) input.getInput() );
                    return;
                } else {
                    throw new RuntimeException( "This rexType was not considered" );
                }

                pos++;
            }
            implementor.operations = Collections.singletonList( doc );
        }


        private Map<Integer, String> getPhysicalMap( List<AlgDataTypeField> fieldList, CatalogTable catalogTable ) {
            Map<Integer, String> map = new HashMap<>();
            List<String> names = catalogTable.getColumnNames();
            List<Long> ids = catalogTable.columnIds;
            int pos = 0;
            for ( String name : Pair.left( fieldList ) ) {
                map.put( pos, MongoStore.getPhysicalColumnName( name, ids.get( names.indexOf( name ) ) ) );
                pos++;
            }
            return map;
        }


        private String getPhysicalName( MongoProject input, CatalogTable catalogTable, int pos ) {
            String logicalName = input.getRowType().getFieldNames().get( pos );
            int index = catalogTable.getColumnNames().indexOf( logicalName );
            return MongoStore.getPhysicalColumnName( logicalName, catalogTable.columnIds.get( index ) );
        }


        private BsonValue getBsonArray( RexCall el, PolyType type, GridFSBucket bucket ) {
            if ( el.op.getKind() == Kind.ARRAY_VALUE_CONSTRUCTOR ) {
                BsonArray array = new BsonArray();
                array.addAll( el.operands.stream().map( operand -> {
                    if ( operand instanceof RexLiteral ) {
                        return BsonUtil.getAsBson( BsonUtil.getMongoComparable( type, (RexLiteral) operand ), type, bucket );
                    } else if ( operand instanceof RexCall ) {
                        return getBsonArray( (RexCall) operand, type, bucket );
                    }
                    throw new RuntimeException( "The given RexCall could not be transformed correctly." );
                } ).collect( Collectors.toList() ) );
                return array;
            }
            throw new RuntimeException( "The given RexCall could not be transformed correctly." );
        }


        private void handleDirectInsert( Implementor implementor, MongoValues values ) {
            List<BsonDocument> docs = new ArrayList<>();
            CatalogTable catalogTable = implementor.mongoTable.getCatalogTable();
            GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();

            AlgDataType valRowType = rowType;

            if ( valRowType == null ) {
                valRowType = values.getRowType();
            }

            List<String> columnNames = catalogTable.getColumnNames();
            List<Long> columnIds = catalogTable.columnIds;
            for ( ImmutableList<RexLiteral> literals : values.tuples ) {
                BsonDocument doc = new BsonDocument();
                int pos = 0;
                for ( RexLiteral literal : literals ) {
                    String name = valRowType.getFieldNames().get( pos );
                    if ( columnNames.contains( name ) ) {
                        doc.append(
                                MongoStore.getPhysicalColumnName( name, columnIds.get( columnNames.indexOf( name ) ) ),
                                BsonUtil.getAsBson( literal, bucket ) );
                    } else {
                        doc.append(
                                rowType.getFieldNames().get( pos ),
                                BsonUtil.getAsBson( literal, bucket ) );
                    }
                    pos++;
                }
                docs.add( doc );
            }
            implementor.operations = docs;
        }

    }


    /**
     * Rule to convert an {@link org.polypheny.db.algebra.logical.LogicalAggregate}
     * to an {@link MongoAggregate}.
     */
    private static class MongoAggregateRule extends MongoConverterRule {

        public static final AlgOptRule INSTANCE = new MongoAggregateRule();


        private MongoAggregateRule() {
            super( LogicalAggregate.class, r -> true, Convention.NONE, MongoAlg.CONVENTION,
                    "MongoAggregateRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalAggregate agg = (LogicalAggregate) alg;
            final AlgTraitSet traitSet =
                    agg.getTraitSet().replace( out );
            try {
                return new MongoAggregate(
                        alg.getCluster(),
                        traitSet,
                        convert( agg.getInput(), traitSet.simplify() ),
                        agg.indicator,
                        agg.getGroupSet(),
                        agg.getGroupSets(),
                        agg.getAggCallList() );
            } catch ( InvalidAlgException e ) {
                LOGGER.warn( e.toString() );
                return null;
            }
        }

    }

}
