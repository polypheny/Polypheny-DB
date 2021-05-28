/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.stream.Collectors;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.enumerable.RexImpTable;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.util.MongoTypeUtil;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptCost;
import org.polypheny.db.plan.RelOptPlanner;
import org.polypheny.db.plan.RelOptRule;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.plan.RelTrait;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.prepare.Prepare.CatalogReader;
import org.polypheny.db.prepare.RelOptTableImpl;
import org.polypheny.db.rel.AbstractRelNode;
import org.polypheny.db.rel.InvalidRelException;
import org.polypheny.db.rel.RelCollations;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.rel.core.Sort;
import org.polypheny.db.rel.core.TableModify;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.logical.LogicalAggregate;
import org.polypheny.db.rel.logical.LogicalFilter;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.metadata.RelMetadataQuery;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rel.type.RelRecordType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.schema.ModifiableTable;
import org.polypheny.db.schema.Table;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.validate.SqlValidatorUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Bug;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;
import org.polypheny.db.util.trace.PolyphenyDbTrace;
import org.slf4j.Logger;


/**
 * Rules and relational operators for {@link MongoRel#CONVENTION MONGO} calling convention.
 */
public class MongoRules {

    private MongoRules() {
    }


    protected static final Logger LOGGER = PolyphenyDbTrace.getPlannerTracer();

    @Getter
    public static final RelOptRule[] RULES = {
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
        if ( call.getOperator() != SqlStdOperatorTable.ITEM ) {
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


    static List<String> mongoFieldNames( final RelDataType rowType ) {
        return SqlValidatorUtil.uniquify(
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
                SqlValidatorUtil.EXPR_SUGGESTER, true );
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


    /**
     * Translator from {@link RexNode} to strings in MongoDB's expression language.
     */
    static class RexToMongoTranslator extends RexVisitorImpl<String> {

        private final JavaTypeFactory typeFactory;
        private final List<String> inFields;

        private static final Map<SqlOperator, String> MONGO_OPERATORS = new HashMap<>();


        static {
            // Arithmetic
            MONGO_OPERATORS.put( SqlStdOperatorTable.DIVIDE, "$divide" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.MULTIPLY, "$multiply" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.MOD, "$mod" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.PLUS, "$add" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.MINUS, "$subtract" );
            // Boolean
            MONGO_OPERATORS.put( SqlStdOperatorTable.AND, "$and" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.OR, "$or" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.NOT, "$not" );
            // Comparison
            MONGO_OPERATORS.put( SqlStdOperatorTable.EQUALS, "$eq" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.NOT_EQUALS, "$ne" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.GREATER_THAN, "$gt" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, "$gte" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.LESS_THAN, "$lt" );
            MONGO_OPERATORS.put( SqlStdOperatorTable.LESS_THAN_OR_EQUAL, "$lte" );
        }


        protected RexToMongoTranslator( JavaTypeFactory typeFactory, List<String> inFields ) {
            super( true );
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
        public String visitInputRef( RexInputRef inputRef ) {
            return maybeQuote( "$" + inFields.get( inputRef.getIndex() ) );
        }


        @Override
        public String visitCall( RexCall call ) {
            String name = isItem( call );
            if ( name != null ) {
                return "'$" + name + "'";
            }
            final List<String> strings = visitList( call.operands );
            if ( call.getKind() == SqlKind.CAST ) {
                return strings.get( 0 );
            }
            String stdOperator = MONGO_OPERATORS.get( call.getOperator() );
            if ( stdOperator != null ) {
                return "{" + stdOperator + ": [" + Util.commaList( strings ) + "]}";
            }
            if ( call.getOperator() == SqlStdOperatorTable.ITEM ) {
                final RexNode op1 = call.operands.get( 1 );
                // normal
                if ( op1 instanceof RexLiteral && op1.getType().getPolyType() == PolyType.INTEGER ) {
                    if ( !Bug.CALCITE_194_FIXED ) {
                        return "'" + stripQuotes( strings.get( 0 ) ) + "[" + ((RexLiteral) op1).getValue2() + "]'";
                    }
                    return strings.get( 0 ) + "[" + strings.get( 1 ) + "]";
                }
                // prepared
                if ( op1 instanceof RexDynamicParam ) {
                    return strings.get( 0 ) + "[" + new BsonDynamic( (RexDynamicParam) op1 ).toJson() + "]";
                }
            }
            if ( call.getOperator() == SqlStdOperatorTable.CASE ) {
                StringBuilder sb = new StringBuilder();
                StringBuilder finish = new StringBuilder();
                // case(a, b, c)  -> $cond:[a, b, c]
                // case(a, b, c, d) -> $cond:[a, b, $cond:[c, d, null]]
                // case(a, b, c, d, e) -> $cond:[a, b, $cond:[c, d, e]]
                for ( int i = 0; i < strings.size(); i += 2 ) {
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
            if ( call.getType().getPolyType() == PolyType.ARRAY ) {
                BsonArray array = new BsonArray();
                array.addAll( visitList( call.operands ).stream().map( BsonString::new ).collect( Collectors.toList() ) );
                return array.toString();
            }
            throw new IllegalArgumentException( "Translation of " + call + " is not supported by MongoProject" );
        }


        private String stripQuotes( String s ) {
            return s.startsWith( "'" ) && s.endsWith( "'" )
                    ? s.substring( 1, s.length() - 1 )
                    : s;
        }


        public List<String> visitList( List<RexNode> list ) {
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


        MongoConverterRule( Class<? extends RelNode> clazz, RelTrait in, Convention out, String description ) {
            super( clazz, in, out, description );
            this.out = out;
        }

    }


    /**
     * Rule to convert a {@link Sort} to a {@link MongoSort}.
     */
    private static class MongoSortRule extends MongoConverterRule {

        public static final MongoSortRule INSTANCE = new MongoSortRule();


        private MongoSortRule() {
            super( Sort.class, Convention.NONE, MongoRel.CONVENTION, "MongoSortRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final Sort sort = (Sort) rel;
            final RelTraitSet traitSet = sort.getTraitSet().replace( out ).replace( sort.getCollation() );
            return new MongoSort( rel.getCluster(), traitSet, convert( sort.getInput(), traitSet.replace( RelCollations.EMPTY ) ), sort.getCollation(), sort.offset, sort.fetch );
        }

    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.logical.LogicalFilter} to a {@link MongoFilter}.
     */
    private static class MongoFilterRule extends MongoConverterRule {

        private static final MongoFilterRule INSTANCE = new MongoFilterRule();


        private MongoFilterRule() {
            super( LogicalFilter.class, Convention.NONE, MongoRel.CONVENTION, "MongoFilterRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalFilter filter = (LogicalFilter) rel;
            final RelTraitSet traitSet = filter.getTraitSet().replace( out );
            return new MongoFilter( rel.getCluster(), traitSet, convert( filter.getInput(), out ), filter.getCondition() );
        }

    }


    /**
     * Rule to convert a {@link org.polypheny.db.rel.logical.LogicalProject} to a {@link MongoProject}.
     */
    private static class MongoProjectRule extends MongoConverterRule {

        private static final MongoProjectRule INSTANCE = new MongoProjectRule();


        private MongoProjectRule() {
            super( LogicalProject.class, Convention.NONE, MongoRel.CONVENTION, "MongoProjectRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalProject project = (LogicalProject) rel;
            final RelTraitSet traitSet = project.getTraitSet().replace( out );
            return new MongoProject( project.getCluster(), traitSet, convert( project.getInput(), out ), project.getProjects(), project.getRowType() );
        }

    }


    public static class MongoValuesRule extends MongoConverterRule {

        private static final MongoValuesRule INSTANCE = new MongoValuesRule();


        private MongoValuesRule() {
            super( Values.class, Convention.NONE, MongoRel.CONVENTION, "MongoValuesRule." + MongoRel.CONVENTION );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            Values values = (Values) rel;
            return new MongoValues( values.getCluster(), values.getRowType(), values.getTuples(), values.getTraitSet().replace( out ) );
        }

    }


    public static class MongoValues extends Values implements MongoRel {

        MongoValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traitSet ) {
            super( cluster, rowType, tuples, traitSet );
        }


        @Override
        public void implement( Implementor implementor ) {

        }

    }


    private static class MongoTableModificationRule extends MongoConverterRule {

        private static final MongoTableModificationRule INSTANCE = new MongoTableModificationRule();


        MongoTableModificationRule() {
            super( TableModify.class, Convention.NONE, MongoRel.CONVENTION, "MongoTableModificationRule." + MongoRel.CONVENTION );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final TableModify modify = (TableModify) rel;
            final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
            if ( modifiableTable == null ) {
                return null;
            }

            if ( modify.getTable().unwrap( MongoTable.class ) == null ) {
                return null;
            }

            final RelTraitSet traitSet = modify.getTraitSet().replace( out );
            return new MongoTableModify(
                    modify.getCluster(),
                    traitSet,
                    modify.getTable(),
                    modify.getCatalogReader(),
                    RelOptRule.convert( modify.getInput(), traitSet ),
                    modify.getOperation(),
                    modify.getUpdateColumnList(),
                    modify.getSourceExpressionList(),
                    modify.isFlattened() );
        }

    }


    private static class MongoTableModify extends TableModify implements MongoRel {


        protected MongoTableModify( RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, CatalogReader catalogReader, RelNode input, Operation operation, List<String> updateColumnList, List<RexNode> sourceExpressionList, boolean flattened ) {
            super( cluster, traitSet, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened );
        }


        @Override
        public RelOptCost computeSelfCost( RelOptPlanner planner, RelMetadataQuery mq ) {
            return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
        }


        @Override
        public RelNode copy( RelTraitSet traitSet, List<RelNode> inputs ) {
            return new MongoTableModify(
                    getCluster(),
                    traitSet,
                    getTable(),
                    getCatalogReader(),
                    AbstractRelNode.sole( inputs ),
                    getOperation(),
                    getUpdateColumnList(),
                    getSourceExpressionList(),
                    isFlattened() );
        }


        @Override
        public void implement( Implementor implementor ) {
            implementor.setDML( true );
            Table preTable = ((RelOptTableImpl) table).getTable();

            if ( !(preTable instanceof MongoTable) ) {
                throw new RuntimeException( "There seems to be a problem with the correct costs for one of stores." );
            }
            implementor.mongoTable = (MongoTable) preTable;
            implementor.table = table;
            implementor.setOperation( this.getOperation() );

            switch ( this.getOperation() ) {
                case INSERT: {
                    if ( input instanceof MongoValues ) {
                        handleDirectInsert( implementor, ((MongoValues) input) );
                    } else if ( input instanceof MongoProject ) {
                        handlePreparedInsert( implementor, ((MongoProject) input) );
                    } else {
                        return;
                    }
                }
                break;
                case UPDATE:
                    MongoRel.Implementor condImplementor = new Implementor( true );
                    condImplementor.setStaticRowType( implementor.getStaticRowType() );
                    ((MongoRel) input).implement( condImplementor );
                    implementor.filter = condImplementor.filter;
                    assert condImplementor.getStaticRowType() instanceof MongoRowType;
                    MongoRowType rowType = (MongoRowType) condImplementor.getStaticRowType();
                    int pos = 0;
                    BsonDocument doc = new BsonDocument();
                    GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();
                    for ( RexNode el : getSourceExpressionList() ) {
                        if ( el instanceof RexLiteral ) {
                            doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), MongoTypeUtil.getAsBson( (RexLiteral) el, bucket ) );
                        } else if ( el instanceof RexCall ) {
                            if ( ((RexCall) el).op.kind == SqlKind.PLUS ) {
                                doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), visitCall( implementor, (RexCall) el, SqlKind.PLUS, el.getType().getPolyType() ) );
                            } else {
                                doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), MongoTypeUtil.getBsonArray( (RexCall) el, bucket ) );
                            }
                        } else if ( el instanceof RexDynamicParam ) {
                            doc.append( rowType.getPhysicalName( getUpdateColumnList().get( pos ) ), new BsonDynamic( (RexDynamicParam) el ) );
                        }
                        pos++;
                    }
                    BsonDocument update = new BsonDocument().append( "$set", doc );

                    implementor.operations = Collections.singletonList( update );

                    break;
                case MERGE:
                    break;
                case DELETE: {
                    MongoRel.Implementor filterCollector = new Implementor( true );
                    filterCollector.setStaticRowType( implementor.getStaticRowType() );
                    ((MongoRel) input).implement( filterCollector );
                    List<String> docs = new ArrayList<>();
                    for ( Pair<String, String> el : filterCollector.list ) {
                        docs.add( el.right );
                    }
                    String docString = "";
                    if ( docs.size() == 1 ) {
                        docString = docs.get( 0 );
                    } else {
                        // TODO DL: evaluate if this is even possible
                    }
                    implementor.filter = filterCollector.filter;
                }

            }

        }


        private BsonValue visitCall( Implementor implementor, RexCall call, SqlKind op, PolyType type ) {
            BsonDocument doc = new BsonDocument();

            BsonArray array = new BsonArray();
            for ( RexNode operand : call.operands ) {
                if ( operand.getKind() == SqlKind.FIELD_ACCESS ) {
                    String physicalName = "$" + implementor.getPhysicalName( ((RexFieldAccess) operand).getField().getName() );
                    array.add( new BsonString( physicalName ) );
                } else if ( operand instanceof RexCall ) {
                    array.add( visitCall( implementor, (RexCall) operand, ((RexCall) operand).op.getKind(), type ) );
                } else if ( operand.getKind() == SqlKind.LITERAL ) {
                    array.add( MongoTypeUtil.getAsBson( ((RexLiteral) operand).getValueAs( MongoTypeUtil.getClassFromType( type ) ), type, implementor.mongoTable.getMongoSchema().getBucket() ) );
                } else if ( operand.getKind() == SqlKind.DYNAMIC_PARAM ) {
                    array.add( new BsonDynamic( (RexDynamicParam) operand ) );
                } else {
                    throw new RuntimeException( "Not implemented yet" );
                }
            }
            if ( op == SqlKind.PLUS ) {
                doc.append( "$add", array );
            } else if ( op == SqlKind.MINUS ) {
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
            // TODO DL: REFACTOR
            MongoValues values = (MongoValues) input.getInput();
            if ( values.tuples.size() > 0
                    && values.getRowType().getFieldList().size() != 1
                    && values.getRowType().getFieldList().get( 0 ).getName().equals( "ZERO" ) ) {
                // we have a partitioned table
                handleDirectInsert( implementor, values );
                return;
            }

            BsonDocument doc = new BsonDocument();
            CatalogTable catalogTable = implementor.mongoTable.getCatalogTable();
            GridFSBucket bucket = implementor.mongoTable.getMongoSchema().getBucket();
            Map<Integer, String> physicalMapping = getPhysicalMap( input.getRowType().getFieldList(), catalogTable );

            implementor.setStaticRowType( (RelRecordType) input.getRowType() );

            int pos = 0;
            for ( RexNode rexNode : input.getChildExps() ) {
                if ( rexNode instanceof RexDynamicParam ) {
                    // preparedInsert
                    doc.append( physicalMapping.get( pos ), new BsonDynamic( (RexDynamicParam) rexNode ) );

                } else if ( rexNode instanceof RexLiteral ) {
                    doc.append( getPhysicalName( input, catalogTable, pos ), MongoTypeUtil.getAsBson( (RexLiteral) rexNode, bucket ) );
                } else if ( rexNode instanceof RexCall ) {
                    PolyType type = ((RelOptTableImpl) table)
                            .getTable()
                            .getRowType( getCluster().getTypeFactory() )
                            .getFieldList()
                            .get( pos )
                            .getType()
                            .getComponentType()
                            .getPolyType();

                    doc.append( physicalMapping.get( pos ), getBsonArray( (RexCall) rexNode, type, bucket ) );

                } else if ( rexNode.getKind() == SqlKind.INPUT_REF && input.getInput() instanceof MongoValues ) {
                    // TODO DL handle and refactor
                    handleDirectInsert( implementor, (MongoValues) input.getInput() );
                    return;
                } else {
                    throw new RuntimeException( "This rexType was not considered" );
                }

                pos++;
            }
            // we need to use the extended json format here to not loose precision like long -> int etc.
            implementor.operations = Collections.singletonList( doc );

        }


        private Map<Integer, String> getPhysicalMap( List<RelDataTypeField> fieldList, CatalogTable catalogTable ) {
            Map<Integer, String> map = new HashMap<>();
            List<String> names = catalogTable.getColumnNames();
            List<Long> ids = catalogTable.columnIds;
            int pos = 0;
            for ( String name : Pair.left( fieldList ) ) {
                map.put( pos, MongoStore.getPhysicalColumnName( ids.get( names.indexOf( name ) ) ) );
                pos++;
            }
            return map;
        }


        private String getPhysicalName( MongoProject input, CatalogTable catalogTable, int pos ) {
            String logicalName = input.getRowType().getFieldNames().get( pos );
            int index = catalogTable.getColumnNames().indexOf( logicalName );
            return MongoStore.getPhysicalColumnName( catalogTable.columnIds.get( index ) );
        }


        private BsonValue getBsonArray( RexCall el, PolyType type, GridFSBucket bucket ) {
            if ( el.op.kind == SqlKind.ARRAY_VALUE_CONSTRUCTOR ) {
                BsonArray array = new BsonArray();
                array.addAll( el.operands.stream().map( operand -> {
                    if ( operand instanceof RexLiteral ) {
                        return MongoTypeUtil.getAsBson( MongoTypeUtil.getMongoComparable( type, (RexLiteral) operand ), type, bucket );
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

            for ( ImmutableList<RexLiteral> literals : values.tuples ) {
                BsonDocument doc = new BsonDocument();
                int pos = 0;
                for ( RexLiteral literal : literals ) {
                    doc.append( MongoStore.getPhysicalColumnName( catalogTable.columnIds.get( pos ) ), MongoTypeUtil.getAsBson( literal, bucket ) );
                    pos++;
                }
                docs.add( doc );
            }
            implementor.operations = docs;
        }

    }



/*

  /**
   * Rule to convert a {@link LogicalCalc} to an
   * {@link MongoCalcRel}.
   o/
  private static class MongoCalcRule
      extends MongoConverterRule {
    private MongoCalcRule(MongoConvention out) {
      super(
          LogicalCalc.class,
          Convention.NONE,
          out,
          "MongoCalcRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalCalc calc = (LogicalCalc) rel;

      // If there's a multiset, let FarragoMultisetSplitter work on it
      // first.
      if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
        return null;
      }

      return new MongoCalcRel(
          rel.getCluster(),
          rel.getTraitSet().replace(out),
          convert(
              calc.getChild(),
              calc.getTraitSet().replace(out)),
          calc.getProgram(),
          Project.Flags.Boxed);
    }
  }

  public static class MongoCalcRel extends SingleRel implements MongoRel {
    private final RexProgram program;

    /**
     * Values defined in {@link org.polypheny.db.rel.core.Project.Flags}.
     o/
    protected int flags;

    public MongoCalcRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode child,
        RexProgram program,
        int flags) {
      super(cluster, traitSet, child);
      assert getConvention() instanceof MongoConvention;
      this.flags = flags;
      this.program = program;
      this.rowType = program.getOutputRowType();
    }

    public RelOptPlanWriter explainTerms(RelOptPlanWriter pw) {
      return program.explainCalc(super.explainTerms(pw));
    }

    public double getRows() {
      return LogicalFilter.estimateFilteredRows(
          getChild(), program);
    }

    public RelOptCost computeSelfCost(RelOptPlanner planner) {
      double dRows = RelMetadataQuery.getRowCount(this);
      double dCpu =
          RelMetadataQuery.getRowCount(getChild())
              * program.getExprCount();
      double dIo = 0;
      return planner.makeCost(dRows, dCpu, dIo);
    }

    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new MongoCalcRel(
          getCluster(),
          traitSet,
          sole(inputs),
          program.copy(),
          getFlags());
    }

    public int getFlags() {
      return flags;
    }

    public RexProgram getProgram() {
      return program;
    }

    public SqlString implement(MongoImplementor implementor) {
      final SqlBuilder buf = new SqlBuilder(implementor.dialect);
      buf.append("SELECT ");
      if (isStar(program)) {
        buf.append("*");
      } else {
        for (Ord<RexLocalRef> ref : Ord.zip(program.getProjectList())) {
          buf.append(ref.i == 0 ? "" : ", ");
          expr(buf, program, ref.e);
          alias(buf, null, getRowType().getFieldNames().get(ref.i));
        }
      }
      implementor.newline(buf)
          .append("FROM ");
      implementor.subQuery(buf, 0, getChild(), "t");
      if (program.getCondition() != null) {
        implementor.newline(buf);
        buf.append("WHERE ");
        expr(buf, program, program.getCondition());
      }
      return buf.toSqlString();
    }

    private static boolean isStar(RexProgram program) {
      int i = 0;
      for (RexLocalRef ref : program.getProjectList()) {
        if (ref.getIndex() != i++) {
          return false;
        }
      }
      return i == program.getInputRowType().getFieldCount();
    }

    private static void expr(
        SqlBuilder buf, RexProgram program, RexNode rex) {
      if (rex instanceof RexLocalRef) {
        final int index = ((RexLocalRef) rex).getIndex();
        expr(buf, program, program.getExprList().get(index));
      } else if (rex instanceof RexInputRef) {
        buf.identifier(
            program.getInputRowType().getFieldNames().get(
                ((RexInputRef) rex).getIndex()));
      } else if (rex instanceof RexLiteral) {
        toSql(buf, (RexLiteral) rex);
      } else if (rex instanceof RexCall) {
        final RexCall call = (RexCall) rex;
        switch (call.getOperator().getSyntax()) {
        case Binary:
          expr(buf, program, call.getOperands().get(0));
          buf.append(' ')
              .append(call.getOperator().toString())
              .append(' ');
          expr(buf, program, call.getOperands().get(1));
          break;
        default:
          throw new AssertionError(call.getOperator());
        }
      } else {
        throw new AssertionError(rex);
      }
    }
  }

  private static SqlBuilder toSql(SqlBuilder buf, RexLiteral rex) {
    switch (rex.getTypeName()) {
    case CHAR:
    case VARCHAR:
      return buf.append(
          new NlsString(rex.getValue2().toString(), null, null)
              .asSql(false, false));
    default:
      return buf.append(rex.getValue2().toString());
    }
  }

*/


    /**
     * Rule to convert an {@link org.polypheny.db.rel.logical.LogicalAggregate}
     * to an {@link MongoAggregate}.
     */
    private static class MongoAggregateRule extends MongoConverterRule {

        public static final RelOptRule INSTANCE = new MongoAggregateRule();


        private MongoAggregateRule() {
            super( LogicalAggregate.class, Convention.NONE, MongoRel.CONVENTION,
                    "MongoAggregateRule" );
        }


        @Override
        public RelNode convert( RelNode rel ) {
            final LogicalAggregate agg = (LogicalAggregate) rel;
            final RelTraitSet traitSet =
                    agg.getTraitSet().replace( out );
            try {
                return new MongoAggregate(
                        rel.getCluster(),
                        traitSet,
                        convert( agg.getInput(), traitSet.simplify() ),
                        agg.indicator,
                        agg.getGroupSet(),
                        agg.getGroupSets(),
                        agg.getAggCallList() );
            } catch ( InvalidRelException e ) {
                LOGGER.warn( e.toString() );
                return null;
            }
        }

    }

/*
  /**
   * Rule to convert an {@link org.polypheny.db.rel.logical.Union} to a
   * {@link MongoUnionRel}.
   o/
  private static class MongoUnionRule
      extends MongoConverterRule {
    private MongoUnionRule(MongoConvention out) {
      super(
          Union.class,
          Convention.NONE,
          out,
          "MongoUnionRule");
    }

    public RelNode convert(RelNode rel) {
      final Union union = (Union) rel;
      final RelTraitSet traitSet =
          union.getTraitSet().replace(out);
      return new MongoUnionRel(
          rel.getCluster(),
          traitSet,
          convertList(union.getInputs(), traitSet),
          union.all);
    }
  }

  public static class MongoUnionRel
      extends Union
      implements MongoRel {
    public MongoUnionRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
    }

    public MongoUnionRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoUnionRel(getCluster(), traitSet, inputs, all);
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
      return super.computeSelfCost(planner).multiplyBy(.1);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, "UNION");
    }
  }

  private static SqlString setOpSql(
      SetOp setOpRel, MongoImplementor implementor, String op) {
    final SqlBuilder buf = new SqlBuilder(implementor.dialect);
    for (Ord<RelNode> input : Ord.zip(setOpRel.getInputs())) {
      if (input.i > 0) {
        implementor.newline(buf)
            .append(op + (setOpRel.all ? " ALL " : ""));
        implementor.newline(buf);
      }
      buf.append(implementor.visitChild(input.i, input.e));
    }
    return buf.toSqlString();
  }

  /**
   * Rule to convert an {@link org.polypheny.db.rel.logical.LogicalIntersect}
   * to an {@link MongoIntersectRel}.
   o/
  private static class MongoIntersectRule
      extends MongoConverterRule {
    private MongoIntersectRule(MongoConvention out) {
      super(
          LogicalIntersect.class,
          Convention.NONE,
          out,
          "MongoIntersectRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalIntersect intersect = (LogicalIntersect) rel;
      if (intersect.all) {
        return null; // INTERSECT ALL not implemented
      }
      final RelTraitSet traitSet =
          intersect.getTraitSet().replace(out);
      return new MongoIntersectRel(
          rel.getCluster(),
          traitSet,
          convertList(intersect.getInputs(), traitSet),
          intersect.all);
    }
  }

  public static class MongoIntersectRel
      extends Intersect
      implements MongoRel {
    public MongoIntersectRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public MongoIntersectRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoIntersectRel(getCluster(), traitSet, inputs, all);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, " intersect ");
    }
  }

  /**
   * Rule to convert an {@link org.polypheny.db.rel.logical.LogicalMinus}
   * to an {@link MongoMinusRel}.
   o/
  private static class MongoMinusRule
      extends MongoConverterRule {
    private MongoMinusRule(MongoConvention out) {
      super(
          LogicalMinus.class,
          Convention.NONE,
          out,
          "MongoMinusRule");
    }

    public RelNode convert(RelNode rel) {
      final LogicalMinus minus = (LogicalMinus) rel;
      if (minus.all) {
        return null; // EXCEPT ALL not implemented
      }
      final RelTraitSet traitSet =
          rel.getTraitSet().replace(out);
      return new MongoMinusRel(
          rel.getCluster(),
          traitSet,
          convertList(minus.getInputs(), traitSet),
          minus.all);
    }
  }

  public static class MongoMinusRel
      extends Minus
      implements MongoRel {
    public MongoMinusRel(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        List<RelNode> inputs,
        boolean all) {
      super(cluster, traitSet, inputs, all);
      assert !all;
    }

    public MongoMinusRel copy(
        RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
      return new MongoMinusRel(getCluster(), traitSet, inputs, all);
    }

    public SqlString implement(MongoImplementor implementor) {
      return setOpSql(this, implementor, " minus ");
    }
  }

  public static class MongoValuesRule extends MongoConverterRule {
    private MongoValuesRule(MongoConvention out) {
      super(
          LogicalValues.class,
          Convention.NONE,
          out,
          "MongoValuesRule");
    }

    @Override public RelNode convert(RelNode rel) {
      LogicalValues valuesRel = (LogicalValues) rel;
      return new MongoValuesRel(
          valuesRel.getCluster(),
          valuesRel.getRowType(),
          valuesRel.getTuples(),
          valuesRel.getTraitSet().plus(out));
    }
  }

  public static class MongoValuesRel
      extends Values
      implements MongoRel {
    MongoValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples,
        RelTraitSet traitSet) {
      super(cluster, rowType, tuples, traitSet);
    }

    @Override public RelNode copy(
        RelTraitSet traitSet, List<RelNode> inputs) {
      assert inputs.isEmpty();
      return new MongoValuesRel(
          getCluster(), rowType, tuples, traitSet);
    }

    public SqlString implement(MongoImplementor implementor) {
      throw new AssertionError(); // TODO:
    }
  }
*/
}
