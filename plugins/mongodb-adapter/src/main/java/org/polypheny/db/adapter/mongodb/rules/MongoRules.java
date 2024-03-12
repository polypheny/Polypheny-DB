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

import java.util.AbstractList;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import lombok.Getter;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoConvention;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgFieldCollation.Direction;
import org.polypheny.db.algebra.AlgFieldCollation.NullDirection;
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
import org.polypheny.db.algebra.core.document.DocumentAggregate;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.algebra.core.document.DocumentSort;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.relational.RelModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgTrait;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
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
    public static String isItem( RexCall call ) {
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


    public static String maybeQuote( String s ) {
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


    public static String translateDocValueAsKey( AlgDataType rowType, RexNameRef call ) {
        BsonValue value = translateDocValue( rowType, call );
        return value.isString() ? value.asString().getValue() : value.asDocument().toJson();
    }


    public static BsonValue translateDocValue( AlgDataType rowType, RexNameRef ref ) {
        return new BsonString( ref.getIndex()
                .map( i -> rowType.getFieldNames().get( i ) + "." + ref.getName() )
                .orElse( ref.getName() ) );
    }


    /**
     * Base class for planner rules that convert a relational expression to MongoDB calling convention.
     */
    abstract static class MongoConverterRule extends ConverterRule {

        protected final Convention out;


        <R extends AlgNode> MongoConverterRule( Class<R> clazz, Predicate<? super R> supports, AlgTrait<?> in, Convention out, String description ) {
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
            super( Sort.class, MongoSortRule::supports, Convention.NONE, MongoAlg.CONVENTION, "MongoSortRule" );
        }


        public static boolean supports( Sort sort ) {
            // null is always less in mongodb, so we leave that to Polypheny
            return sort.collation.getFieldCollations().stream().noneMatch( c ->
                    (c.direction == Direction.ASCENDING && c.nullDirection == NullDirection.FIRST)
                            || (c.direction == Direction.DESCENDING && c.nullDirection == NullDirection.LAST) );

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
     * Rule to convert a {@link LogicalRelFilter} to a {@link MongoFilter}.
     */
    private static class MongoFilterRule extends MongoConverterRule {

        private static final MongoFilterRule INSTANCE = new MongoFilterRule();


        private MongoFilterRule() {
            super(
                    LogicalRelFilter.class,
                    MongoFilterRule::supports,
                    Convention.NONE,
                    MongoAlg.CONVENTION,
                    MongoFilterRule.class.getSimpleName() );
        }


        private static boolean supports( LogicalRelFilter filter ) {
            return (MongoConvention.mapsDocuments || !DocumentRules.containsDocument( filter )) && !containsIncompatible( filter );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelFilter filter = (LogicalRelFilter) alg;
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
                    MongoDocumentFilterRule.class.getSimpleName() );
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
     * Rule to convert a {@link LogicalRelProject} to a {@link MongoProject}.
     */
    private static class MongoProjectRule extends MongoConverterRule {

        private static final MongoProjectRule INSTANCE = new MongoProjectRule();


        private MongoProjectRule() {
            super(
                    LogicalRelProject.class,
                    project -> (MongoConvention.mapsDocuments || !DocumentRules.containsDocument( project ))
                            && !containsIncompatible( project )
                            && !UnsupportedRexCallVisitor.containsModelItem( project.getProjects() ),
                    Convention.NONE,
                    MongoAlg.CONVENTION,
                    MongoProjectRule.class.getSimpleName() );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelProject project = (LogicalRelProject) alg;
            final AlgTraitSet traitSet = project.getTraitSet().replace( out );
            return new MongoProject(
                    project.getCluster(),
                    traitSet,
                    convert( project.getInput(), out ),
                    project.getProjects(),
                    project.getTupleType() );
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
                    MongoDocumentProjectRule.class.getSimpleName() );
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
                    || operator.getOperatorName() == OperatorName.TRIM
                    || operator.getOperatorName() == OperatorName.INITCAP
                    || operator.getOperatorName() == OperatorName.SUBSTRING
                    || operator.getOperatorName() == OperatorName.FLOOR
                    || operator.getOperatorName() == OperatorName.DISTANCE
                    || (operator.getOperatorName() == OperatorName.CAST && call.operands.get( 0 ).getType().getPolyType() == PolyType.DATE)
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
            super( Values.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, MongoValuesRule.class.getSimpleName() );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            Values values = (Values) alg;

            return new MongoValues(
                    values.getCluster(),
                    values.getTupleType(),
                    values.getTuples(),
                    values.getTraitSet().replace( out ) );
        }

    }


    public static class MongoDocumentsRule extends MongoConverterRule {

        private static final MongoDocumentsRule INSTANCE = new MongoDocumentsRule();


        private MongoDocumentsRule() {
            super( DocumentValues.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, MongoDocumentsRule.class.getSimpleName() );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            DocumentValues documentValues = (DocumentValues) alg;
            return new MongoDocuments(
                    alg.getCluster(),
                    documentValues.documents,
                    documentValues.dynamicDocuments,
                    alg.getTraitSet().replace( out )
            );


        }

    }


    public static class MongoDocuments extends DocumentValues implements MongoAlg {


        public MongoDocuments( AlgCluster cluster, List<PolyDocument> documentTuples, List<RexDynamicParam> dynamicParams, AlgTraitSet traitSet ) {
            super( cluster, traitSet, documentTuples, dynamicParams );
        }


        @Override
        public void implement( Implementor implementor ) {
            // empty on purpose
        }


        @Override
        public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
            return new MongoDocuments( getCluster(), documents, dynamicDocuments, traitSet );
        }

    }


    private static class MongoTableModificationRule extends MongoConverterRule {

        private static final MongoTableModificationRule INSTANCE = new MongoTableModificationRule();


        MongoTableModificationRule() {
            super( RelModify.class, MongoTableModificationRule::mongoSupported, Convention.NONE, MongoAlg.CONVENTION, MongoTableModificationRule.class.getSimpleName() );
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
            public AlgNode visit( LogicalRelScan scan ) {
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
            Optional<ModifiableTable> oModifiableTable = modify.getEntity().unwrap( ModifiableTable.class );
            if ( oModifiableTable.isEmpty() ) {
                return null;
            }
            Optional<MongoEntity> oMongo = modify.getEntity().unwrap( MongoEntity.class );
            if ( oMongo.isEmpty() ) {
                return null;
            }

            final AlgTraitSet traitSet = modify.getTraitSet().replace( out );
            return new MongoTableModify(
                    modify.getCluster(),
                    traitSet,
                    oMongo.get(),
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
            super( DocumentModify.class, r -> true, Convention.NONE, MongoAlg.CONVENTION, MongoDocumentModificationRule.class.getSimpleName() + "." + MongoAlg.CONVENTION );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final DocumentModify<?> modify = (DocumentModify<?>) alg;
            Optional<ModifiableTable> oModifiableCollection = modify.entity.unwrap( ModifiableTable.class );
            if ( oModifiableCollection.isEmpty() ) {
                return null;
            }
            if ( modify.entity.unwrap( MongoEntity.class ).isEmpty() ) {
                return null;
            }

            final AlgTraitSet traitSet = modify.getTraitSet().replace( out );
            return new MongoDocumentModify(
                    traitSet,
                    modify.entity.unwrap( MongoEntity.class ).get(),
                    AlgOptRule.convert( modify.getInput(), traitSet ),
                    modify.operation,
                    modify.updates,
                    modify.removes,
                    modify.renames );
        }

    }


    /**
     * Rule to convert an {@link LogicalRelAggregate}
     * to an {@link MongoAggregate}.
     */
    private static class MongoAggregateRule extends MongoConverterRule {

        public static final AlgOptRule INSTANCE = new MongoAggregateRule();


        private MongoAggregateRule() {
            super( LogicalRelAggregate.class, MongoAggregateRule::supported, Convention.NONE, MongoAlg.CONVENTION,
                    MongoAggregateRule.class.getSimpleName() );
        }


        private static boolean supported( LogicalRelAggregate aggregate ) {
            return aggregate.getAggCallList().stream().noneMatch( AggregateCall::isDistinct )
                    && aggregate.getModel() != DataModel.DOCUMENT
                    && aggregate.getAggCallList().stream().noneMatch( a -> a.getAggregation().getKind() == Kind.SINGLE_VALUE )
                    && aggregate.getAggCallList().stream().noneMatch( a -> a.getAggregation().getKind() == Kind.COUNT ); // mongodb returns no result when we count and there is nothing in the collection...
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalRelAggregate agg = (LogicalRelAggregate) alg;
            final AlgTraitSet traitSet = agg.getTraitSet().replace( out );
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
            super( DocumentAggregate.class, r -> true, Convention.NONE, MongoAlg.CONVENTION,
                    MongoDocumentAggregateRule.class.getSimpleName() );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            final LogicalDocumentAggregate agg = (LogicalDocumentAggregate) alg;
            final AlgTraitSet traitSet = agg.getTraitSet().replace( out );
            return new MongoDocumentAggregate(
                    alg.getCluster(),
                    traitSet,
                    convert( agg.getInput(), traitSet.simplify() ),
                    agg.getGroup().orElse( null ),
                    agg.aggCalls );
        }

    }

}
