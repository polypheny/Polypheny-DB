/*
 * Copyright 2019-2023 The Polypheny Project
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

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoAlg.Implementor;
import org.polypheny.db.adapter.mongodb.MongoConvention;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.InvalidAlgException;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.document.DocumentSort;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.core.relational.RelScan;
import org.polypheny.db.algebra.enumerable.RexImpTable;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.relational.LogicalAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalFilter;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.document.DocumentRules;
import org.polypheny.db.schema.types.ModifiableTable;
import org.polypheny.db.sql.language.fun.SqlDatetimePlusOperator;
import org.polypheny.db.sql.language.fun.SqlDatetimeSubtractionOperator;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.UnsupportedRexCallVisitor;
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
            MongoTableModificationRule.INSTANCE,
            MongoDocumentSortRule.INSTANCE,
            MongoDocumentFilterRule.INSTANCE,
            MongoDocumentProjectRule.INSTANCE,
            MongoDocumentAggregateRule.INSTANCE,
            MongoDocumentsRule.INSTANCE,
            MongoDocumentModificationRule.INSTANCE
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
        if ( op0 instanceof RexIndexRef
                && ((RexIndexRef) op0).getIndex() == 0
                && op1 instanceof RexLiteral
                && ((RexLiteral) op1).getValue().isString() ) {
            return ((RexLiteral) op1).getValue().asString().value;
        }
        /*if ( op0 instanceof RexInputRef && op1 instanceof RexDynamicParam ) {
            return new BsonDynamic( (RexDynamicParam) op1 ).toJson();
        }*/

        op0.getType().getPolyType();
        return null;
    }


    static List<String> mongoFieldNames( final AlgDataType rowType ) {
        return ValidatorUtil.uniquify(
                new AbstractList<>() {
                    @Override
                    public String get( int index ) {
                        final String name = MongoRules.maybeFix( rowType.getFields().get( index ).getName() );
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
        assert call.operands.get( 0 ) instanceof RexIndexRef;
        assert call.operands.get( 1 ) instanceof RexLiteral;
        String field = rowType.getFieldNames().get( ((RexIndexRef) call.operands.get( 0 )).getIndex() );
        field += "." + ((RexLiteral) call.operands.get( 1 )).getValue();
        return new Pair<>( field, call.operands.get( 2 ) );
    }


    public static String adjustName( String name ) {
        return name.startsWith( "$" )
                ? "_" + maybeFix( name.substring( 2 ) )
                : maybeFix( name );
    }


    public static String translateDocValueAsKey( AlgDataType rowType, RexCall call, String prefix ) {
        BsonValue value = translateDocValue( rowType, call, prefix );
        return value.isString() ? value.asString().getValue() : value.asDocument().toJson();
    }


    public static BsonValue translateDocValue( AlgDataType rowType, RexCall call, String prefix ) {
        RexIndexRef parent = (RexIndexRef) call.getOperands().get( 0 );

        if ( call.operands.get( 1 ).isA( Kind.DYNAMIC_PARAM ) ) {
            return new BsonDynamic( (RexDynamicParam) call.operands.get( 1 ) ).setIsValue( true, prefix + rowType.getFieldNames().get( parent.getIndex() ) );
        }
        RexCall names = (RexCall) call.operands.get( 1 );
        if ( names.operands.isEmpty() ) {
            return new BsonString( prefix + rowType.getFieldNames().get( parent.getIndex() ) );
        }

        return new BsonString( prefix + rowType.getFieldNames().get( parent.getIndex() )
                + "."
                + names.operands
                .stream()
                .map( n -> ((RexLiteral) n).value.asString().value )
                .collect( Collectors.joining( "." ) ) );
    }


    /**
     * Translator from {@link RexNode} to strings in MongoDB's expression language.
     */
    static class RexToMongoTranslator extends RexVisitorImpl<String> {

        private final AlgDataTypeFactory typeFactory;
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


        protected RexToMongoTranslator( AlgDataTypeFactory typeFactory, List<String> inFields, Implementor implementor ) {
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
        public String visitIndexRef( RexIndexRef inputRef ) {
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
                    return "{$arrayElemAt:[" + strings.get( 0 ) + "," + (((RexLiteral) op1).value.asNumber().intValue() - 1) + "]}";
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
            /*if ( call.op.getOperatorName() == OperatorName.MQL ) {
                return call.operands.get( 0 ).accept( this );
            }*/

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

            throw new IllegalArgumentException( "Translation of " + call + " is not supported by MongoProject" );

        }


        public String handleSpecialCases( RexCall call ) {
            if ( call.getType().getPolyType() == PolyType.ARRAY ) {
                BsonArray array = new BsonArray();
                array.addAll( translateList( call.operands ).stream().map( BsonString::new ).collect( Collectors.toList() ) );
                return array.toString();
            } else if ( call.isA( Kind.MQL_QUERY_VALUE ) ) {
                return translateDocValueAsKey( implementor.getStaticRowType(), call, "$" );
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
                        .get( ((RexIndexRef) call.operands.get( 0 )).getIndex() );

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
                            .map( op -> ((RexLiteral) op).value.asString().value )
                            .collect( Collectors.joining( "." ) ) + "\": 0" );
                }

                return String.join( ",", fields );
            } else if ( call.isA( Kind.UNWIND ) ) {
                return call.operands.get( 0 ).accept( this );
            }
            return null;
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
                    sort.getChildExps(),
                    sort.offset,
                    sort.fetch );
        }

    }


    private static class MongoDocumentSortRule extends MongoConverterRule {

        public static final MongoDocumentSortRule INSTANCE = new MongoDocumentSortRule();


        private MongoDocumentSortRule() {
            super( DocumentSort.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, "MongoDocumentSortRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final DocumentSort sort = (DocumentSort) alg;

            final AlgTraitSet traitSet = sort.getTraitSet().replace( out );
            return new MongoSort(
                    alg.getCluster(),
                    traitSet,
                    convert( sort.getInput(), out ),
                    sort.collation,
                    sort.fieldExps,
                    sort.offset,
                    sort.fetch );
        }

    }


    /**
     * Rule to convert a {@link LogicalFilter} to a {@link MongoFilter}.
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


    private static class MongoDocumentFilterRule extends MongoConverterRule {

        private static final MongoDocumentFilterRule INSTANCE = new MongoDocumentFilterRule();


        private MongoDocumentFilterRule() {
            super(
                    LogicalDocumentFilter.class,
                    project -> MongoConvention.mapsDocuments || !DocumentRules.containsDocument( project ),
                    Convention.NONE,
                    MongoAlg.CONVENTION,
                    "MongoDocumentFilterRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalDocumentFilter filter = (LogicalDocumentFilter) alg;
            final AlgTraitSet traitSet = filter.getTraitSet().replace( out );
            return new MongoFilter(
                    alg.getCluster(),
                    traitSet,
                    convert( filter.getInput(), out ),
                    filter.condition );
        }

    }


    /**
     * Rule to convert a {@link LogicalProject} to a {@link MongoProject}.
     */
    private static class MongoProjectRule extends MongoConverterRule {

        private static final MongoProjectRule INSTANCE = new MongoProjectRule();


        private MongoProjectRule() {
            super(
                    LogicalProject.class,
                    project -> (MongoConvention.mapsDocuments || !DocumentRules.containsDocument( project ))
                            && !containsIncompatible( project ) && !UnsupportedRexCallVisitor.containsModelItem( project.getProjects() ),
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


    private static class MongoDocumentProjectRule extends MongoConverterRule {

        private static final MongoDocumentProjectRule INSTANCE = new MongoDocumentProjectRule();


        private MongoDocumentProjectRule() {
            super(
                    LogicalDocumentProject.class,
                    project -> (MongoConvention.mapsDocuments || !DocumentRules.containsDocument( project ))
                            && !containsIncompatible( project ),
                    Convention.NONE,
                    MongoAlg.CONVENTION,
                    "MongoDocumentProjectRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalDocumentProject project = (LogicalDocumentProject) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( out );
            return new MongoDocumentProject(
                    project.getCluster(),
                    traitSet,
                    convert( project.getInput(), out ),
                    project.includes,
                    project.excludes );
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
    @Getter
    public static class MongoExcludeVisitor extends RexVisitorImpl<Void> {

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

            return new MongoValues(
                    values.getCluster(),
                    values.getRowType(),
                    values.getTuples(),
                    values.getTraitSet().replace( out ) );
        }

    }


    public static class MongoDocumentsRule extends MongoConverterRule {

        private static final MongoDocumentsRule INSTANCE = new MongoDocumentsRule();


        private MongoDocumentsRule() {
            super( DocumentValues.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, "MongoDocumentRule." + MongoAlg.CONVENTION );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            DocumentValues documentValues = (DocumentValues) alg;
            return new MongoDocuments(
                    alg.getCluster(),
                    documentValues.documents,
                    alg.getTraitSet().replace( out )
            );


        }

    }


    public static class MongoDocuments extends DocumentValues implements MongoAlg {


        public MongoDocuments( AlgOptCluster cluster, List<PolyDocument> documentTuples, AlgTraitSet traitSet ) {
            super( cluster, traitSet, documentTuples );
        }


        @Override
        public void implement( Implementor implementor ) {
            // empty on purpose
        }

    }


    private static class MongoTableModificationRule extends MongoConverterRule {

        private static final MongoTableModificationRule INSTANCE = new MongoTableModificationRule();


        MongoTableModificationRule() {
            super( RelModify.class, MongoTableModificationRule::mongoSupported, Convention.NONE, MongoAlg.CONVENTION, "MongoTableModificationRule." + MongoAlg.CONVENTION );
        }


        private static boolean mongoSupported( RelModify<MongoEntity> modify ) {
            if ( !modify.isInsert() ) {
                return true;
            }

            ScanChecker scanChecker = new ScanChecker();
            modify.accept( scanChecker );
            return scanChecker.supported;
        }


        @Getter
        private static class ScanChecker extends AlgShuttleImpl {

            private boolean supported = true;


            @Override
            public AlgNode visit( RelScan<?> scan ) {
                supported = false;
                return super.visit( scan );
            }


            @Override
            public AlgNode visit( AlgNode other ) {
                if ( other instanceof AlgSubset ) {
                    ((AlgSubset) other).getAlgList().forEach( a -> a.accept( this ) );
                }
                return super.visit( other );
            }

        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final RelModify<?> modify = (RelModify<?>) alg;
            final ModifiableTable modifiableTable = modify.getEntity().unwrap( ModifiableTable.class );
            if ( modifiableTable == null ) {
                return null;
            }
            MongoEntity mongo = modify.getEntity().unwrap( MongoEntity.class );
            if ( mongo == null ) {
                return null;
            }

            final AlgTraitSet traitSet = modify.getTraitSet().replace( out );
            return new MongoTableModify(
                    modify.getCluster(),
                    traitSet,
                    mongo,
                    AlgOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumns(),
                    modify.getSourceExpressions(),
                    modify.isFlattened() );
        }

    }


    private static class MongoDocumentModificationRule extends MongoConverterRule {

        private static final MongoDocumentModificationRule INSTANCE = new MongoDocumentModificationRule();


        MongoDocumentModificationRule() {
            super( DocumentModify.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, "MongoCollectionModificationRule." + MongoAlg.CONVENTION );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final DocumentModify<MongoEntity> modify = (DocumentModify<MongoEntity>) alg;
            final ModifiableTable modifiableCollection = modify.entity.unwrap( ModifiableTable.class );
            if ( modifiableCollection == null ) {
                return null;
            }
            if ( modify.entity.unwrap( MongoEntity.class ) == null ) {
                return null;
            }

            final AlgTraitSet traitSet = modify.getTraitSet().replace( out );
            return new MongoDocumentModify(
                    traitSet,
                    modify.entity,
                    AlgOptRule.convert( modify.getInput(), traitSet ),
                    modify.operation,
                    modify.updates,
                    modify.removes,
                    modify.renames );
        }

    }


    /**
     * Rule to convert an {@link LogicalAggregate}
     * to an {@link MongoAggregate}.
     */
    private static class MongoAggregateRule extends MongoConverterRule {

        public static final AlgOptRule INSTANCE = new MongoAggregateRule();


        private MongoAggregateRule() {
            super( LogicalAggregate.class, MongoAggregateRule::supported, Convention.NONE, MongoAlg.CONVENTION,
                    "MongoAggregateRule" );
        }


        private static boolean supported( LogicalAggregate aggregate ) {
            return aggregate.getAggCallList().stream().noneMatch( AggregateCall::isDistinct );
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


    private static class MongoDocumentAggregateRule extends MongoConverterRule {

        public static final AlgOptRule INSTANCE = new MongoDocumentAggregateRule();


        private MongoDocumentAggregateRule() {
            super( LogicalDocumentAggregate.class, r -> true, Convention.NONE, MongoAlg.CONVENTION,
                    "MongoDocumentAggregateRule" );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalDocumentAggregate agg = (LogicalDocumentAggregate) alg;
            final AlgTraitSet traitSet =
                    agg.getTraitSet().replace( out );
            return new MongoDocumentAggregate(
                    alg.getCluster(),
                    traitSet,
                    convert( agg.getInput(), traitSet.simplify() ),
                    agg.indicator,
                    agg.groupSet,
                    agg.groupSets,
                    agg.aggCalls,
                    agg.names );
        }

    }

}
