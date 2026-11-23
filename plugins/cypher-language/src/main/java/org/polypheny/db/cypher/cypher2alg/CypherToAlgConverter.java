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

package org.polypheny.db.cypher.cypher2alg;

import static org.polypheny.db.cypher.CypherPathMatchUtil.isRelationshipList;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.PlanKey;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.ResultKind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.common.ConditionalExecute.Condition;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgCall;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgForeach;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgJoin;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgShortestPathJoin;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMergeRows;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSetRows;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnion;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgWriteRow;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalGraph.SubstitutionGraph;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.cypher.CypherCallResultItem;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.CypherParameter;
import org.polypheny.db.cypher.CypherNode.CypherFamily;
import org.polypheny.db.cypher.CypherNode.CypherKind;
import org.polypheny.db.cypher.MergeActionType;
import org.polypheny.db.cypher.clause.CypherCall;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherDelete;
import org.polypheny.db.cypher.clause.CypherForeach;
import org.polypheny.db.cypher.clause.CypherMatch;
import org.polypheny.db.cypher.clause.CypherMerge;
import org.polypheny.db.cypher.clause.CypherOrderItem;
import org.polypheny.db.cypher.clause.CypherQuery;
import org.polypheny.db.cypher.clause.CypherRemove;
import org.polypheny.db.cypher.clause.CypherReturn;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherReturnItem;
import org.polypheny.db.cypher.clause.CypherSetClause;
import org.polypheny.db.cypher.clause.CypherSubQuery;
import org.polypheny.db.cypher.clause.CypherUnwind;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.clause.CypherWith;
import org.polypheny.db.cypher.expression.CypherAggregate;
import org.polypheny.db.cypher.expression.CypherExpression;
import org.polypheny.db.cypher.expression.CypherFunctionInvocation;
import org.polypheny.db.cypher.expression.CypherLiteral;
import org.polypheny.db.cypher.expression.CypherProperty;
import org.polypheny.db.cypher.expression.CypherVariable;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.cypher.pattern.CypherNamedPattern;
import org.polypheny.db.cypher.pattern.CypherNodePattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.pattern.CypherRelPattern;
import org.polypheny.db.cypher.pattern.CypherShortestPathPattern;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.cypher.query.CypherUnion;
import org.polypheny.db.cypher.set.CypherSetItem;
import org.polypheny.db.functions.CypherFunctions;
import org.polypheny.db.functions.CypherMergeRows;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptPredicateList;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexSimplify;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.util.ImmutableBitSet;
import org.polypheny.db.util.Pair;

@Slf4j
public class CypherToAlgConverter {

    private final Snapshot snapshot;
    private final AlgBuilder algBuilder;
    private final Statement statement;
    private final RexBuilder rexBuilder;
    private final AlgCluster cluster;


    public CypherToAlgConverter( Statement statement, AlgBuilder builder, RexBuilder rexBuilder, AlgCluster cluster ) {
        this.snapshot = statement.getTransaction().getSnapshot();
        this.statement = statement;
        this.algBuilder = builder;
        this.rexBuilder = rexBuilder;
        this.cluster = cluster;
    }


    public AlgRoot convert( CypherNode query, ParsedQueryContext parsedContext, AlgCluster cluster ) {
        long namespaceId = parsedContext.getNamespaceId();
        if ( query instanceof CypherUnion union ) {
            return convertUnion( union, namespaceId, cluster );
        }
        return convertBranch( query, namespaceId, cluster, false );
    }


    private AlgRoot convertBranch( CypherNode query, long namespaceId, AlgCluster cluster, boolean unionBranch ) {
        LogicalEntity entity = getEntity( namespaceId, query );

        if ( query.isFullScan() ) {
            // simple full graph relScan
            return AlgRoot.of( buildFullScan( (LogicalGraph) entity ), Kind.SELECT ).withResultKind( ResultKind.GRAPH_ELEMENTS );
        }

        if ( !CypherFamily.QUERY.contains( query.getCypherKind() ) ) {
            throw new GenericRuntimeException( "Used a unsupported query." );
        }

        CypherContext context = new CypherContext( query, entity, cluster, algBuilder, rexBuilder, snapshot );

        convertQuery( query, context, !unionBranch );

        CypherGraphInputs graphInputs = new CypherGraphInputs( entity, cluster );
        AlgNode output = context.build().accept( graphInputs );
        if ( context.returningInsert != null && graphInputs.usesGraphInput() ) {
            // Read the graph after each CREATE row is written, using the returning write pipeline.
            context = new CypherContext( query, entity, cluster, algBuilder, rexBuilder, snapshot );
            convertQuery( query, context, false );
            output = context.build().accept( graphInputs );
        }
        if ( context.returningInsert != null ) {
            // Execute the write to completion even when the RETURN projection produces no rows.
            output = LogicalConditionalExecute.create( context.returningInsert, output, Condition.GREATER_ZERO,
                    GenericRuntimeException.class, "Cypher CREATE did not produce an update result" );
        }
        AlgRoot root = AlgRoot.of( output, context.kind != null ? context.kind : Kind.SELECT );
        ResultKind result = ResultKind.forStatement( root.kind, DataModel.GRAPH, root.validatedRowType );
        boolean writeCount = output instanceof LogicalLpgAggregate aggregate && aggregate.isWriteCount()
                || output instanceof LogicalLpgForeach foreach && foreach.isCountOnly();
        if ( result == ResultKind.UPDATE_COUNT && (context.returningInsert != null
                || !writeCount && LogicalLpgModify.containsReturningWrite( output )) ) {
            result = ResultKind.forGraphProjection( root.validatedRowType );
        }
        return root.withResultKind( result );
    }


    private AlgRoot convertUnion( CypherUnion union, long namespaceId, AlgCluster cluster ) {
        // The parser builds a left-deep tree. Flatten equal operators and preserve source order.
        List<CypherQuery> branches = new ArrayList<>();
        CypherQuery first = union;
        while ( first instanceof CypherUnion part ) {
            if ( part.isAll() != union.isAll() ) {
                throw new CypherSyntaxException( "InvalidClauseComposition: UNION and UNION ALL cannot be mixed." );
            }
            branches.add( part.getRight() );
            first = part.getLeft();
        }
        branches.add( first );

        List<AlgNode> inputs = new ArrayList<>( branches.size() );
        List<String> names = null;
        Kind kind = Kind.SELECT;
        for ( int i = branches.size() - 1; i >= 0; i-- ) {
            CypherQuery branch = branches.get( i );
            if ( !(branch instanceof CypherSingleQuery single) || single.isFullScan() || single.isDdl()
                    || !(single.getClauses().get( single.getClauses().size() - 1 ) instanceof CypherReturnClause) ) {
                throw new CypherSyntaxException( "InvalidClauseComposition: Each UNION branch must end in a RETURN clause." );
            }

            AlgRoot converted = convertBranch( branch, namespaceId, cluster, true );
            if ( Kind.DML.contains( converted.kind ) ) {
                kind = converted.kind;
            }
            List<String> branchNames = converted.validatedRowType.getFieldNames();
            if ( names == null ) {
                names = branchNames;
            }
            if ( names.size() != branchNames.size() || !branchNames.containsAll( names ) ) {
                throw new CypherSyntaxException( "DifferentColumnsInUnion: All UNION branches must return the same column names." );
            }

            AlgNode input = converted.alg;
            if ( !names.equals( branchNames ) ) {
                List<RexNode> fields = new ArrayList<>( names.size() );
                for ( String name : names ) {
                    fields.add( rexBuilder.makeInputRef( input, branchNames.indexOf( name ) ) );
                }
                input = LogicalLpgProject.create( input, fields, names.stream().map( PolyString::of ).toList() );
            }
            inputs.add( input );
        }

        AlgRoot root = AlgRoot.of( LogicalLpgUnion.create( inputs, union.isAll() ), kind );
        return root.withResultKind( ResultKind.forGraphProjection( root.validatedRowType ) );
    }


    @NotNull
    private LogicalEntity getEntity( long namespaceId, CypherNode query ) {
        Optional<LogicalGraph> optionalGraph = this.snapshot.graph().getGraph( namespaceId );
        if ( optionalGraph.isPresent() ) {
            return optionalGraph.get();
        }

        Optional<LogicalNamespace> optionalNamespace = this.snapshot.getNamespace( namespaceId );
        if ( optionalNamespace.isPresent() ) {
            return new SubstitutionGraph( namespaceId, "sub", false, optionalNamespace.get().caseSensitive, query.getUnderlyingLabels() );
        }

        throw new GenericRuntimeException( "Could not find namespace with id " + namespaceId );
    }


    private AlgNode buildFullScan( LogicalGraph graph ) {
        return new LogicalLpgScan(
                cluster,
                cluster.traitSet(),
                graph,
                GraphType.of() );
    }


    private void convertQuery( CypherNode node, CypherContext context, boolean allowStandaloneCreateReturning ) {
        switch ( node.getCypherKind() ) {
            case SINGLE:
                CypherSingleQuery query = (CypherSingleQuery) node;
                List<CypherClause> clauses = query.getClauses().stream()
                        .filter( clause -> clause.getCypherKind() != CypherKind.USE )
                        .toList();
                context.createReturning = allowStandaloneCreateReturning && clauses.size() == 2
                        && clauses.get( 0 ).getCypherKind() == CypherKind.CREATE
                        && clauses.get( 1 ).getCypherKind() == CypherKind.RETURN;

                for ( int i = 0; i < clauses.size(); i++ ) {
                    context.writeHasConsumer = i + 1 < clauses.size();
                    convertClauses( clauses.get( i ), context );
                }

                break;
            case PERIODIC_COMMIT:
            case UNION:
                throw new UnsupportedOperationException();
        }
    }


    private void convertClauses( CypherClause clause, CypherContext context ) {
        switch ( clause.getCypherKind() ) {
            case MATCH:
                convertMatch( (CypherMatch) clause, context );
                break;
            case RETURN:
                convertProjection( (CypherReturnClause) clause, null, context, false );
                break;
            case CREATE:
                materializePathBindings( context );
                convertCreate( (CypherCreate) clause, context );
                break;
            case MERGE:
                materializePathBindings( context );
                convertMerge( (CypherMerge) clause, context );
                break;
            case USE:
                break;
            case WITH:
                convertWith( (CypherWith) clause, context );
                break;
            case UNWIND:
                convertUnwind( (CypherUnwind) clause, context );
                break;
            case FOR_EACH:
                materializePathBindings( context );
                convertForeach( (CypherForeach) clause, context );
                break;
            case QUERY:
                materializePathBindings( context );
                convertCallSubquery( (CypherSubQuery) clause, context );
                break;
            case CALL:
                materializePathBindings( context );
                convertProcedureCall( (CypherCall) clause, context );
                break;
            case SET:
                materializePathBindings( context );
                convertSet( (CypherSetClause) clause, context );
                break;
            case DELETE:
                materializePathBindings( context );
                convertDelete( (CypherDelete) clause, context );
                break;
            case REMOVE:
                materializePathBindings( context );
                convertRemove( (CypherRemove) clause, context );
                break;
            default:
                throw new UnsupportedOperationException();
        }

    }


    private void convertRemove( CypherRemove clause, CypherContext context ) {
        clause.getRemove( context );
    }


    private void convertDelete( CypherDelete clause, CypherContext context ) {
        clause.getDelete( context );
    }


    private void convertSet( CypherSetClause clause, CypherContext context ) {
        clause.getSet( context );
    }


    private void convertUnwind( CypherUnwind clause, CypherContext context ) {
        materializePathBindings( context );
        clause.getUnwind( context );
    }


    private void convertForeach( CypherForeach clause, CypherContext context ) {
        if ( !context.hasInput() ) {
            context.add( context.asValues( List.of() ) );
        }
        RexNode list = clause.getExpression().getRex( context, RexType.PROJECT ).right;
        if ( context.containsAggs() ) {
            throw new CypherSyntaxException( "An aggregate used by FOREACH must be evaluated in a preceding WITH clause." );
        }
        AlgNode input = context.pop();
        AlgDataType outerType = input.getTupleType();
        String item = clause.getVariable().getName();
        String listName = CypherContext.privateName( outerType.getFieldNames(), "$foreach$list" );
        List<RexNode> outerValues = context.refs( input );
        List<PolyString> outerNames = CypherContext.fieldNames( input );
        outerValues.add( list );
        outerNames.add( PolyString.of( listName ) );
        AlgNode rows = LogicalLpgProject.create( input, outerValues, outerNames );

        AlgDataType component = list.getType().getComponentType();
        AlgDataType itemType = context.typeFactory.createTypeWithNullability(
                component == null ? context.typeFactory.createPolyType( PolyType.ANY ) : component, true );
        AlgDataTypeFactory.Builder iterationFields = context.typeFactory.builder();
        for ( AlgDataTypeField field : outerType.getFields() ) {
            String name = field.getName() == null
                    ? CypherContext.privateName( outerType.getFieldNames(), "$foreach$anonymous" + field.getIndex() )
                    : field.getName();
            iterationFields.add( name, null, field.getType() );
        }
        AlgDataType iterationType = iterationFields.add( listName, null, itemType ).build();
        CorrelationId correlation = cluster.createCorrel();
        RexNode binding = rexBuilder.makeCorrel( iterationType, correlation );
        List<RexNode> bodyValues = new ArrayList<>();
        List<PolyString> bodyNames = new ArrayList<>();
        for ( AlgDataTypeField field : outerType.getFields() ) {
            String name = field.getName();
            if ( name != null && !name.equals( item ) ) {
                bodyValues.add( rexBuilder.makeFieldAccess( binding, field.getIndex() ) );
                bodyNames.add( PolyString.of( name ) );
            }
        }
        bodyValues.add( rexBuilder.makeFieldAccess( binding, outerType.getFieldCount() ) );
        bodyNames.add( PolyString.of( item ) );

        boolean countOnly = !context.writeHasConsumer;
        CypherContext body = new CypherContext( clause, context.graph, cluster, algBuilder, rexBuilder, snapshot );
        body.add( LogicalLpgProject.create( body.asValues( List.of() ), bodyValues, bodyNames ) );
        List<CypherClause> updates = clause.getClauses();
        for ( int i = 0; i < updates.size(); i++ ) {
            CypherClause update = updates.get( i );
            switch ( update.getCypherKind() ) {
                case CREATE, MERGE, SET, REMOVE, DELETE, FOR_EACH -> {
                    body.writeHasConsumer = !countOnly || i < updates.size() - 1;
                    convertClauses( update, body );
                }
                default -> throw new CypherSyntaxException( "FOREACH only accepts updating clauses." );
            }
        }
        AlgDataType resultType = countOnly ? AlgOptUtil.createDmlRowType( Kind.UPDATE, context.typeFactory ) : outerType;
        context.add( new LogicalLpgForeach( rows, body.build(), correlation, iterationType, resultType, countOnly ) );
        context.kind = Kind.UPDATE;
    }


    private void convertCallSubquery( CypherSubQuery call, CypherContext context ) {
        if ( call.getInTransactionsParams() != null ) {
            throw new CypherSyntaxException( "CALL subqueries cannot start independent transactions." );
        }
        if ( !( call.getQuery() instanceof CypherSingleQuery query ) || query.isFullScan() || query.hasUpdates()
                || query.getClauses().stream().anyMatch( clause -> clause.isDdl() || clause.getCypherKind() == CypherKind.USE ) ) {
            throw new CypherSyntaxException( "CALL requires a single read-only subquery on the enclosing graph." );
        }
        List<CypherClause> clauses = query.getClauses();
        if ( !( clauses.get( clauses.size() - 1 ) instanceof CypherReturnClause ) ) {
            throw new CypherSyntaxException( "A CALL subquery must end in RETURN." );
        }
        AlgNode outer = popCallInput( context );
        CorrelationId correlation = cluster.createCorrel();
        ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
        CypherContext body = new CypherContext( query, context.graph, cluster, algBuilder, rexBuilder, snapshot );
        int start = 0;
        if ( clauses.get( 0 ) instanceof CypherWith with && isImportingWith( with ) ) {
            List<String> names = outer == null ? List.of() : outer.getTupleType().getFieldNames();
            List<RexNode> values = new ArrayList<>();
            List<PolyString> bindings = new ArrayList<>();
            for ( CypherReturn item : with.getReturnClause().getReturnItems() ) {
                String name = ( (CypherVariable) ( (CypherReturnItem) item ).getExpression() ).getName();
                int index = names.indexOf( name );
                if ( index < 0 ) {
                    throw new CypherSyntaxException( "Unknown variable imported by CALL subquery: " + name );
                }
                if ( bindings.contains( PolyString.of( name ) ) ) {
                    throw new CypherSyntaxException( "Variable imported more than once by CALL subquery: " + name );
                }
                requiredColumns.set( index );
                values.add( rexBuilder.makeFieldAccess( rexBuilder.makeCorrel( outer.getTupleType(), correlation ), index ) );
                bindings.add( PolyString.of( name ) );
            }
            body.add( LogicalLpgProject.create( body.asValues( List.of() ), values, bindings ) );
            start = 1;
        }
        for ( int i = start; i < clauses.size(); i++ ) {
            body.writeHasConsumer = i + 1 < clauses.size();
            convertClauses( clauses.get( i ), body );
        }
        appendCallRows( context, outer, body.build(), correlation, requiredColumns.build() );
    }


    private static boolean isImportingWith( CypherWith with ) {
        CypherReturnClause projection = with.getReturnClause();
        return with.getWhere() == null && !projection.isDistinct() && projection.getOrder().isEmpty()
                && projection.getSkip() == null && projection.getLimit() == null
                && projection.getReturnItems().stream().allMatch( item -> item instanceof CypherReturnItem value
                        && value.getVariable() == null && value.getExpression() instanceof CypherVariable );
    }


    private void convertProcedureCall( CypherCall call, CypherContext context ) {
        if ( !call.getNamespace().equals( List.of( "db" ) ) || !call.getName().equals( "labels" ) ) {
            throw new CypherSyntaxException( "Unknown Cypher procedure: " + String.join( ".", call.getNamespace() )
                    + (call.getNamespace().isEmpty() ? "" : ".") + call.getName() );
        }
        if ( call.getArguments() != null && !call.getArguments().isEmpty() ) {
            throw new CypherSyntaxException( "db.labels() takes no arguments." );
        }
        AlgNode outer = popCallInput( context );
        boolean standalone = outer == null && !context.writeHasConsumer;
        if ( !standalone && (call.isYieldAll() || call.getItems() == null) ) {
            throw new CypherSyntaxException( "CALL db.labels() in a query requires an explicit YIELD clause." );
        }

        List<PolyString> names = new ArrayList<>();
        if ( call.getItems() == null ) {
            names.add( PolyString.of( "label" ) );
        } else {
            for ( CypherCallResultItem item : call.getItems() ) {
                if ( !item.getExpression().equals( "label" ) ) {
                    throw new CypherSyntaxException( "Unknown output of db.labels(): " + item.getExpression() );
                }
                String name = item.getInput() instanceof CypherVariable variable ? variable.getName() : item.getExpression();
                names.add( PolyString.of( name ) );
            }
        }

        AlgNode scan = new LogicalLpgScan( cluster, cluster.traitSet(), context.graph, GraphType.of() );
        QueryLanguage cypher = QueryLanguage.from( "cypher" );
        RexCall node = (RexCall) rexBuilder.makeCall( context.nodeType, OperatorRegistry.get( cypher, OperatorName.CYPHER_NODE_EXTRACT ),
                List.of( rexBuilder.makeInputRef( context.nodeType, 0 ) ) );
        AlgNode nodes = LogicalLpgMatch.create( scan, List.of( node ), List.of( PolyString.of( "$call$node" ) ) );
        AlgDataType stringType = context.typeFactory.createPolyType( PolyType.VARCHAR );
        AlgDataType listType = context.typeFactory.createArrayType( stringType, -1 );
        RexNode labels = rexBuilder.makeCall( listType, OperatorRegistry.get( cypher, OperatorName.CYPHER_LABELS ),
                List.of( rexBuilder.makeInputRef( nodes, 0 ) ) );
        AlgNode arrays = LogicalLpgProject.create( nodes, List.of( labels ), List.of( PolyString.of( "$call$labels" ) ) );
        AlgNode values = LogicalLpgUnwind.create( arrays, 0, "label" );
        AlgNode distinct = LogicalLpgAggregate.create( values, List.of( RexNameRef.create( "label", 0, stringType ) ), List.of() );
        RexNode label = rexBuilder.makeInputRef( distinct, 0 );
        AlgNode result = LogicalLpgProject.create( distinct, names.stream().map( name -> label ).toList(), names );
        appendCallRows( context, outer, result, cluster.createCorrel(), ImmutableBitSet.of() );
        if ( call.getWhere() != null ) {
            convertWhere( call.getWhere(), context );
            context.combineFilter();
        }
    }


    private AlgNode popCallInput( CypherContext context ) {
        AlgNode input = context.pop();
        if ( input != null && input.getTupleType().getFieldNames().stream().anyMatch( Objects::isNull ) ) {
            return projectBindings( input, matchBindings( new MatchRows( input, 0 ), 0, Map.of() ) );
        }
        return input;
    }


    private void appendCallRows( CypherContext context, @Nullable AlgNode outer, AlgNode body,
            CorrelationId correlation, ImmutableBitSet requiredColumns ) {
        Set<String> names = new HashSet<>( outer == null ? List.of() : outer.getTupleType().getFieldNames() );
        for ( String name : body.getTupleType().getFieldNames() ) {
            if ( !names.add( name ) ) {
                throw new CypherSyntaxException( "CALL cannot return the already bound variable: " + name );
            }
        }
        context.add( outer == null ? body : new LogicalLpgCall( outer, body, correlation, requiredColumns ) );
        setKindIfNull( context, Kind.SELECT );
    }


    private void convertWith( CypherWith clause, CypherContext context ) {
        for ( CypherReturn item : clause.getReturnClause().getReturnItems() ) {
            if ( item instanceof CypherReturnItem expression && expression.getVariable() == null
                    && !(expression.getExpression() instanceof CypherVariable) ) {
                throw new CypherSyntaxException( "An expression in WITH must have an alias (use AS)." );
            }
        }
        convertProjection( clause.getReturnClause(), clause.getWhere(), context, true );
        // A projected path is a value. Its original node and relationship variables are out of scope.
        context.implicitPathBindings.clear();
    }


    private void convertProjection( CypherReturnClause clause, @Nullable CypherWhere where, CypherContext context, boolean withClause ) {
        if ( clause.getReturnItems().stream().anyMatch( CypherReturn::isStar ) ) {
            materializePathBindings( context );
        }
        AlgNode source = context.stack.isEmpty() ? null : context.peek();
        AlgNode projection = clause.getGraphProject( context, withClause );
        List<CypherOrderItem> order = clause.getOrder() == null ? List.of() : clause.getOrder();
        boolean sort = !order.isEmpty() || clause.getLimit() != null || clause.getSkip() != null;
        if ( order.isEmpty() && where == null ) {
            context.add( sort ? clause.getGraphSort( projection, List.of() ) : projection );
            return;
        }

        // Only a regular projection can look up unprojected input variables. DISTINCT and
        // aggregation expose their outputs, including expressions already projected.
        LogicalLpgProject project = projection instanceof LogicalLpgProject output && output.getInput() == source ? output : null;
        AlgNode input = project == null ? projection : project.getInput();
        Map<String, RexNode> sourceBindings = new LinkedHashMap<>();
        if ( source != null ) {
            matchBindings( new MatchRows( source, 0 ), 0, context.implicitPathBindings )
                    .forEach( ( name, value ) -> sourceBindings.put( name.value, value ) );
        }
        Map<String, RexNode> original = input == source ? sourceBindings : Map.of();
        List<AlgDataTypeField> fields = projection.getTupleType().getFields();
        List<RexNode> values = new ArrayList<>( project == null
                ? fields.stream().map( field -> (RexNode) rexBuilder.makeInputRef( projection, field.getIndex() ) ).toList()
                : project.getProjects() );
        List<PolyString> names = projection.getTupleType().getFieldNames().stream()
                .map( PolyString::of ).collect( Collectors.toCollection( ArrayList::new ) );
        int width = fields.size();
        Map<String, RexNode> outputValues = new LinkedHashMap<>();
        Map<String, RexNode> outputRefs = new LinkedHashMap<>();
        for ( int i = 0; i < width; i++ ) {
            String name = names.get( i ).value;
            if ( projection != source || context.isVisibleVariable( name ) ) {
                outputValues.putIfAbsent( name, values.get( i ) );
                outputRefs.putIfAbsent( name, rexBuilder.makeInputRef( fields.get( i ).getType(), i ) );
            }
        }

        Map<String, RexNode> unshadowedBindings = new HashMap<>( sourceBindings );
        outputValues.keySet().forEach( unshadowedBindings::remove );
        Map<PlanKey, String> projectedExpressions = projectedExpressions( clause, sourceBindings, context );

        for ( CypherReturn item : clause.getReturnItems() ) {
            if ( item instanceof CypherReturnItem projected && projected.getExpression() instanceof CypherVariable variable ) {
                String column = projected.columnName();
                if ( outputValues.containsKey( column ) ) {
                    outputValues.putIfAbsent( variable.getName(), outputValues.get( column ) );
                    outputRefs.putIfAbsent( variable.getName(), outputRefs.get( column ) );
                }
            }
        }

        List<Integer> sortFields = new ArrayList<>();
        if ( !order.isEmpty() ) {
            boolean grouped = clause.getReturnItems().stream().anyMatch(
                    item -> item instanceof CypherReturnItem projected && projected.getExpression() instanceof CypherAggregate );
            Function<String, RexNode> sortBindings = name -> {
                RexNode binding = outputValues.getOrDefault( name, original.get( name ) );
                if ( binding == null && grouped && sourceBindings.containsKey( name ) ) {
                    throw new CypherSyntaxException( "AmbiguousAggregationExpression: '%s' is not available as a grouping key in ORDER BY.".formatted( name ) );
                }
                return binding;
            };
            for ( CypherOrderItem item : order ) {
                CypherExpression expression = item.getExpression();
                RexNode projectedKey = projectedExpression( expression, projectedExpressions, sourceBindings, outputValues, context );
                RexNode value;
                try {
                    // An exact projected expression is already a sort key. Inside another
                    // expression, a new output alias still shadows the original binding.
                    if ( projectedKey != null ) {
                        value = projectedKey;
                    } else {
                        value = context.resolveInScope( expression, RexType.PROJECT, sortBindings, part -> {
                            boolean aggregate = part instanceof CypherAggregate;
                            // A compound grouping key must be named by its alias or recomputed from simple keys.
                            if ( grouped && !aggregate && !(part instanceof CypherProperty property && property.getSubject() instanceof CypherVariable) ) {
                                return null;
                            }
                            RexNode projected = projectedExpression( part, projectedExpressions,
                                    aggregate ? sourceBindings : unshadowedBindings, outputValues, context );
                            if ( aggregate && projected == null ) {
                                throw new CypherSyntaxException( "The aggregation used to order rows must also be projected." );
                            }
                            return projected;
                        } );
                    }
                } catch ( CypherSyntaxException ex ) {
                    if ( grouped ) {
                        throw ex;
                    }
                    String name = expression instanceof CypherVariable || expression instanceof CypherProperty property && property.getSubject() instanceof CypherVariable
                            ? expression.getName() : expression.getClass().getSimpleName();
                    throw new CypherSyntaxException( "Unknown sort expression: " + name );
                }
                int index = values.indexOf( value );
                sortFields.add( index >= 0 ? index : appendHiddenColumn( value, values, names ) );
            }
        }

        RexNode predicate = null;
        if ( where != null ) {
            Map<String, RexNode> filterInputs = new HashMap<>( outputRefs );
            RexNode condition = context.resolveInScope( where.getExpression(), RexType.FILTER, name -> filterInputs.computeIfAbsent( name, key -> {
                RexNode originalValue = original.get( key );
                if ( originalValue == null ) {
                    return null;
                }
                int index = appendHiddenColumn( originalValue, values, names );
                return rexBuilder.makeInputRef( originalValue.getType(), index );
            } ), part -> projectedExpression( part, projectedExpressions, unshadowedBindings, outputRefs, context ) );
            predicate = context.booleanPredicate( condition );
        }
        AlgNode result = values.size() == width ? projection
                : LogicalLpgProject.create( input, values, names );
        if ( sort ) {
            result = clause.getGraphSort( result, sortFields );
        }
        if ( predicate != null ) {
            result = new LogicalLpgFilter( result.getCluster(), result.getTraitSet(), result, predicate );
            context.clearVariables();
        }
        if ( values.size() > width ) {
            List<RexNode> visible = result.getTupleType().getFields().subList( 0, width ).stream()
                    .map( field -> (RexNode) rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ).toList();
            // The sort still orders the rows, but its keys can refer to columns removed here.
            result = new LogicalLpgProject( cluster, result.getTraitSet().replace( AlgCollations.EMPTY ),
                    result, visible, names.subList( 0, width ) );
        }
        context.add( result );
    }


    private static int appendHiddenColumn( RexNode value, List<RexNode> values, List<PolyString> names ) {
        int index = values.size();
        PolyString name = PolyString.of( "$projection" + index );
        while ( names.contains( name ) ) {
            name = PolyString.of( name.value + "_" );
        }
        values.add( value );
        names.add( name );
        return index;
    }


    private static Map<PlanKey, String> projectedExpressions( CypherReturnClause clause, Map<String, RexNode> sourceBindings, CypherContext context ) {
        Map<PlanKey, String> expressions = new HashMap<>();
        for ( CypherReturn item : clause.getReturnItems() ) {
            if ( item instanceof CypherReturnItem projection && !(projection.getExpression() instanceof CypherVariable) ) {
                String name = projection.columnName() == null && projection.getExpression() instanceof CypherAggregate aggregate
                        ? aggregate.op.name() : projection.columnName();
                if ( name != null ) {
                    expressionKey( projection.getExpression(), sourceBindings, context ).ifPresent( key -> expressions.putIfAbsent( key, name ) );
                }
            }
        }
        return expressions;
    }


    @Nullable
    private static RexNode projectedExpression( CypherExpression expression, Map<PlanKey, String> projections,
            Map<String, RexNode> sourceBindings, Map<String, RexNode> output, CypherContext context ) {
        if ( projections.isEmpty() || expression instanceof CypherVariable ) {
            return null;
        }
        return expressionKey( expression, sourceBindings, context ).map( projections::get ).map( output::get ).orElse( null );
    }


    private static Optional<PlanKey> expressionKey( CypherExpression expression, Map<String, RexNode> sourceBindings, CypherContext context ) {
        try {
            // Use the existing structural Rex representation in the input scope. Aggregate
            // keys include their operation and argument without adding another aggregate call.
            if ( expression instanceof CypherAggregate aggregate ) {
                RexNode argument = aggregate.getTarget() == null ? null : context.resolveInScope( aggregate.getTarget(), RexType.PROJECT, sourceBindings::get, null );
                return PlanKey.of( aggregate.op, aggregate.isDistinct(), argument );
            }
            return PlanKey.of( context.resolveInScope( expression, RexType.PROJECT, sourceBindings::get, null ) );
        } catch ( GenericRuntimeException | UnsupportedOperationException ex ) {
            // A failed probe is not a query error. Resolve the expression normally to apply
            // output aliases and report any errors in the resulting scope.
            return Optional.empty();
        }
    }


    private void materializePathBindings( CypherContext context ) {
        if ( context.implicitPathBindings.isEmpty() ) {
            return;
        }
        AlgNode input = context.pop();
        context.add( projectBindings( input, matchBindings( new MatchRows( input, 0 ), 0, context.implicitPathBindings ) ) );
        context.implicitPathBindings.clear();
    }


    private void convertMerge( CypherMerge clause, CypherContext context ) {
        if ( !context.hasInput() ) {
            context.add( context.asValues( List.of() ) );
        }
        AlgNode input = context.peek();
        List<AlgDataTypeField> fields = new ArrayList<>( input.getTupleType().getFields() );
        Map<String, Integer> names = new HashMap<>();
        for ( AlgDataTypeField field : fields ) {
            names.put( field.getName(), field.getIndex() );
        }

        CypherPattern pattern = clause.getPattern();
        CypherVariable pathVariable = null;
        if ( pattern instanceof CypherNamedPattern named ) {
            pathVariable = named.getVariable();
            pattern = named.getAnonymousPattern();
        }
        if ( !(pattern instanceof CypherEveryPathPattern path) || !path.getPredicates().isEmpty() ) {
            throw new CypherSyntaxException( "MERGE requires a path pattern without inline WHERE predicates." );
        }

        List<RexNode> properties = new ArrayList<>();
        List<CypherMergeRows.Node> nodes = new ArrayList<>();
        int inputCount = fields.size();
        for ( CypherNodePattern node : path.getNodes() ) {
            CypherVariable variable = node.getVariable();
            Integer existing = variable == null ? null : names.get( variable.getName() );
            boolean bound = existing != null && existing < inputCount;
            if ( existing != null ) {
                if ( path.getEdges().isEmpty() || !node.getLabels().isEmpty() || node.getProperties() != null ) {
                    throw mergeVariableAlreadyBound( variable.getName() );
                }
                PolyType type = fields.get( existing ).getType().getPolyType();
                if ( type != PolyType.NODE && !isDynamicBinding( type ) ) {
                    throw new CypherSyntaxException( "MERGE requires a node binding: " + variable.getName() );
                }
            }
            int slot = existing == null ? addMergeBinding( variable, context.nodeType, names, fields ) : existing;
            nodes.add( new CypherMergeRows.Node( slot, bound, mergeProperties( context, node.getProperties(), properties ), node.getLabels() ) );
        }

        List<CypherMergeRows.Relationship> relationships = new ArrayList<>();
        for ( CypherRelPattern edge : path.getEdges() ) {
            int slot = addMergeBinding( edge.getVariable(), context.edgeType, names, fields );
            if ( edge.getPathLength() != null ) {
                throw new CypherSyntaxException( "CreatingVarLength: MERGE does not allow variable-length relationships." );
            }
            if ( edge.getLabels().size() != 1 ) {
                throw new CypherSyntaxException( "NoSingleRelationshipType: MERGE requires exactly one relationship type." );
            }
            EdgeDirection direction = edge.isLeft() == edge.isRight() ? EdgeDirection.NONE
                    : edge.isRight() ? EdgeDirection.LEFT_TO_RIGHT : EdgeDirection.RIGHT_TO_LEFT;
            relationships.add( new CypherMergeRows.Relationship( slot, mergeProperties( context, edge.getProperties(), properties ),
                    PolyString.of( edge.getLabels().get( 0 ) ), direction ) );
        }

        int pathSlot = addMergeBinding( pathVariable, context.pathType, names, fields );
        AlgDataType rowType = new AlgRecordType( fields );
        List<LogicalLpgMergeRows.Action> onCreate = new ArrayList<>();
        List<LogicalLpgMergeRows.Action> onMatch = new ArrayList<>();
        AlgNode actionScope = LogicalLpgValues.create( cluster, cluster.traitSet(), rowType, ImmutableList.of() );
        context.add( actionScope );
        try {
            for ( int i = 0; i < clause.getClauses().size(); i++ ) {
                List<LogicalLpgMergeRows.Action> actions = clause.getActionTypes().get( i ) == MergeActionType.ON_CREATE ? onCreate : onMatch;
                for ( CypherSetItem item : clause.getClauses().get( i ).getInput() ) {
                    item.convertItem( context );
                    for ( Pair<PolyString, RexNode> action : context.popNodes() ) {
                        actions.add( new LogicalLpgMergeRows.Action( names.get( action.left.value ), action.right ) );
                    }
                }
            }
        } finally {
            context.pop();
        }

        context.pop();
        LogicalLpgScan scan = new LogicalLpgScan( cluster, cluster.traitSet(), context.graph, GraphType.of() );
        List<PolyString> outputNames = fields.stream().map( field -> PolyString.of( field.getName() ) ).toList();
        CypherMergeRows.Pattern specification = new CypherMergeRows.Pattern( nodes, relationships, pathSlot, inputCount, outputNames );
        AlgNode rows = new LogicalLpgMergeRows( input, scan, rowType, specification, properties, onCreate, onMatch, !context.writeHasConsumer );
        AlgNode write = new LogicalLpgModify( cluster, cluster.traitSet(), context.graph, rows, Modify.Operation.MERGE, null, null, 1 );
        context.add( context.writeHasConsumer ? write : LogicalLpgAggregate.countWrites( write ) );
        context.clearVariables();
        // The graph-write result and monitoring pipeline uses INSERT for a possible creation.
        context.kind = Kind.INSERT;
    }


    private static int mergeProperties( CypherContext context, @Nullable CypherExpression expression, List<RexNode> properties ) {
        if ( expression == null ) {
            return -1;
        }
        if ( !(expression instanceof CypherLiteral literal) || literal.getLiteralType() != CypherLiteral.Literal.MAP ) {
            throw new CypherSyntaxException( "InvalidParameterUse: MERGE pattern properties require a literal map of expressions." );
        }
        int slot = properties.size();
        properties.add( literal.getRex( context, RexType.PROJECT ).right );
        return slot;
    }


    private static int addMergeBinding( @Nullable CypherVariable variable, AlgDataType type, Map<String, Integer> names, List<AlgDataTypeField> fields ) {
        if ( variable == null ) {
            return -1;
        }
        String name = variable.getName();
        int index = fields.size();
        if ( names.putIfAbsent( name, index ) != null ) {
            throw mergeVariableAlreadyBound( name );
        }
        fields.add( new AlgDataTypeFieldImpl( -1L, name, index, type ) );
        return index;
    }


    private static CypherSyntaxException mergeVariableAlreadyBound( String name ) {
        return new CypherSyntaxException( "VariableAlreadyBound: Cannot rebind '" + name + "' in MERGE." );
    }


    private void convertCreate( CypherCreate clause, CypherContext context ) {
        validateCreatePatterns( clause.getPatterns(), context.stack.isEmpty() ? null : context.peek() );
        context.kind = Kind.INSERT;

        if ( context.stack.isEmpty() && (context.usesCreateExpressions() || clause.getPatterns().stream().anyMatch( CypherToAlgConverter::hasComputedCreateProperties )) ) {
            context.add( context.asValues( List.of() ) );
        }

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }
        context.combineValues();
    }


    private static boolean hasComputedCreateProperties( CypherPattern pattern ) {
        if ( pattern instanceof CypherNamedPattern named ) {
            pattern = named.getAnonymousPattern();
        }
        return pattern instanceof CypherEveryPathPattern path
                && (path.getNodes().stream().anyMatch( node -> isComputedProperty( node.getProperties() ) )
                || path.getEdges().stream().anyMatch( edge -> isComputedProperty( edge.getProperties() ) ));
    }


    private static boolean isComputedProperty( @Nullable CypherExpression property ) {
        if ( property == null || property instanceof CypherParameter ) {
            return false;
        }
        if ( property instanceof CypherLiteral literal ) {
            return switch ( literal.getLiteralType() ) {
                case MAP -> literal.getMapValue().values().stream().anyMatch( CypherToAlgConverter::isComputedProperty );
                case LIST -> literal.getListValue().stream().anyMatch( CypherToAlgConverter::isComputedProperty );
                default -> false;
            };
        }
        // The literal path already handles point maps with constant coordinates.
        return !(property instanceof CypherFunctionInvocation function
                && function.getOperatorName() == OperatorName.CYPHER_POINT
                && function.getArguments().size() == 1
                && function.getArguments().get( 0 ) instanceof CypherLiteral literal
                && literal.getLiteralType() == CypherLiteral.Literal.MAP
                && !isComputedProperty( literal ));
    }


    private static void validateCreatePatterns( List<CypherPattern> patterns, @Nullable AlgNode input ) {
        Map<String, PolyType> bindings = new HashMap<>();
        if ( input != null ) {
            for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
                bindings.put( field.getName(), field.getType().getPolyType() );
            }
        }
        for ( CypherPattern pattern : patterns ) {
            if ( !pattern.getPredicates().isEmpty() ) {
                throw new CypherSyntaxException( "Inline WHERE predicates are not allowed in CREATE patterns." );
            }
            if ( pattern instanceof CypherNamedPattern named ) {
                registerCreatedVariable( bindings, named.getVariable(), PolyType.PATH );
                pattern = named.getAnonymousPattern();
            }
            if ( !(pattern instanceof CypherEveryPathPattern path) ) {
                continue;
            }
            for ( CypherNodePattern node : path.getNodes() ) {
                if ( node.getVariable() == null ) {
                    continue;
                }
                String name = node.getVariable().getName();
                PolyType boundType = bindings.putIfAbsent( name, PolyType.NODE );
                if ( boundType == null ) {
                    continue;
                }
                // An existing node can only be reused as a bare relationship endpoint.
                if ( path.getEdges().isEmpty() || !node.getLabels().isEmpty() || node.getProperties() != null ) {
                    throw createVariableAlreadyBound( name );
                }
                if ( boundType != PolyType.NODE && !isDynamicBinding( boundType ) ) {
                    throw new CypherSyntaxException( "CREATE requires a node binding: " + name );
                }
            }
            for ( CypherRelPattern edge : path.getEdges() ) {
                if ( edge.getPathLength() != null ) {
                    throw new CypherSyntaxException( "Variable-length relationships are not supported in CREATE" );
                }
                if ( edge.isLeft() == edge.isRight() ) {
                    throw new CypherSyntaxException( "RequiresDirectedRelationship: CREATE requires directed relationships" );
                }
                if ( edge.getRelTypes().size() != 1 ) {
                    throw new CypherSyntaxException( "NoSingleRelationshipType: CREATE relationships require exactly one type" );
                }
                if ( edge.getVariable() != null ) {
                    registerCreatedVariable( bindings, edge.getVariable(), PolyType.EDGE );
                }
            }
        }
    }


    private static void registerCreatedVariable( Map<String, PolyType> bindings, CypherVariable variable, PolyType type ) {
        if ( bindings.putIfAbsent( variable.getName(), type ) != null ) {
            throw createVariableAlreadyBound( variable.getName() );
        }
    }


    private static CypherSyntaxException createVariableAlreadyBound( String name ) {
        return new CypherSyntaxException( "VariableAlreadyBound: Cannot rebind '" + name + "' in CREATE." );
    }


    private void convertMatch( CypherMatch clause, CypherContext context ) {
        List<CypherPattern> patterns = new ArrayList<>();
        Set<Integer> shortest = new HashSet<>();
        for ( CypherPattern pattern : clause.getPatterns() ) {
            CypherPattern anonymous = pattern instanceof CypherNamedPattern named ? named.getAnonymousPattern() : pattern;
            if ( anonymous instanceof CypherShortestPathPattern shortestPath ) {
                shortest.add( patterns.size() );
                CypherEveryPathPattern segment = shortestPath.getSingleSegment();
                pattern = pattern instanceof CypherNamedPattern named ? new CypherNamedPattern( named.getVariable(), segment ) : segment;
            }
            patterns.add( pattern );
        }
        Set<String> occupiedBindings = validateMatchBindings( patterns );
        setKindIfNull( context, Kind.SELECT );
        context.active = clause;
        MatchPropertyMaps propertyMaps = prepareMatchPropertyMaps( patterns, occupiedBindings, context );
        List<CypherExpression> inlinePredicates = patterns.stream().flatMap( pattern -> pattern.getPredicates().stream() ).toList();
        if ( clause.isOptional() || patterns.size() > 1 || !context.stack.isEmpty() || !inlinePredicates.isEmpty()
                || !propertyMaps.checks().isEmpty() || !shortest.isEmpty() ) {
            convertJoinedMatch( clause, context, inlinePredicates, propertyMaps, shortest );
            return;
        }

        for ( CypherPattern pattern : patterns ) {
            convertPattern( pattern, context );
        }
        // Row-dependent writes extract their bindings from the full path in the engine.
        boolean usesPathInWrite = !context.implicitPathBindings.isEmpty()
                && context.original instanceof CypherSingleQuery query && query.hasUpdates();
        context.combineMatch( usesPathInWrite );

        if ( clause.getWhere() == null ) {
            return;
        }

        convertWhere( clause.getWhere(), context );

        context.combineFilter();
    }


    private static Set<String> validateMatchBindings( List<CypherPattern> patterns ) {
        Map<String, PolyType> bindings = new HashMap<>();
        Set<String> fixedRelationships = new HashSet<>();
        for ( CypherPattern pattern : patterns ) {
            if ( pattern instanceof CypherNamedPattern named ) {
                registerMatchBinding( bindings, named.getVariable(), PolyType.PATH );
                pattern = named.getAnonymousPattern();
            }
            if ( pattern instanceof CypherEveryPathPattern path ) {
                for ( CypherNodePattern node : path.getNodes() ) {
                    registerMatchBinding( bindings, node.getVariable(), PolyType.NODE );
                }
                for ( CypherRelPattern edge : path.getEdges() ) {
                    CypherVariable variable = edge.getVariable();
                    PolyType type = edge.getPathLength() == null ? PolyType.EDGE : PolyType.ARRAY;
                    registerMatchBinding( bindings, variable, type );
                    if ( variable != null && type == PolyType.EDGE && !fixedRelationships.add( variable.getName() ) ) {
                        throw new CypherSyntaxException( "Relationship variable '" + variable.getName() + "' is repeated in the same MATCH." );
                    }
                }
            }
        }
        return new HashSet<>( bindings.keySet() );
    }


    private static void registerMatchBinding( Map<String, PolyType> bindings, @Nullable CypherVariable variable, PolyType type ) {
        if ( variable == null ) {
            return;
        }
        String name = variable.getName();
        PolyType previous = bindings.putIfAbsent( name, type );
        if ( previous != null && previous != type ) {
            throw new CypherSyntaxException( "Variable '" + name + "' has conflicting types in MATCH: " + previous + " and " + type + "." );
        }
        if ( previous == PolyType.PATH ) {
            throw new CypherSyntaxException( "Path variable '" + name + "' is already bound." );
        }
    }


    private static MatchPropertyMaps prepareMatchPropertyMaps( List<CypherPattern> patterns, Set<String> occupiedBindings, CypherContext context ) {
        if ( context.hasInput() ) {
            for ( AlgDataTypeField field : context.peek().getTupleType().getFields() ) {
                occupiedBindings.add( field.getName() );
                if ( context.isImplicitPathBinding( field.getName() ) ) {
                    occupiedBindings.addAll( field.getType().getFieldNames() );
                }
            }
        }
        List<CypherPattern> candidates = new ArrayList<>();
        List<MatchPropertyCheck> checks = new ArrayList<>();
        Set<PolyString> internalBindings = new HashSet<>();
        for ( CypherPattern pattern : patterns ) {
            CypherPattern anonymous = pattern instanceof CypherNamedPattern named ? named.getAnonymousPattern() : pattern;
            if ( !(anonymous instanceof CypherEveryPathPattern path) ) {
                candidates.add( pattern );
                continue;
            }
            int previousChecks = checks.size();
            List<CypherNodePattern> nodes = new ArrayList<>();
            List<CypherRelPattern> edges = new ArrayList<>();
            for ( CypherNodePattern node : path.getNodes() ) {
                MatchPropertySplit split = splitMatchProperties( node.getProperties(), node.getVariable(), occupiedBindings, internalBindings, checks );
                nodes.add( split == null ? node : node.withMatchProperties( split.binding(), split.literalProperties() ) );
            }
            for ( CypherRelPattern edge : path.getEdges() ) {
                MatchPropertySplit split = splitMatchProperties( edge.getProperties(), edge.getVariable(), occupiedBindings, internalBindings, checks );
                edges.add( split == null ? edge : edge.withMatchProperties( split.binding(), split.literalProperties() ) );
            }
            if ( previousChecks == checks.size() ) {
                candidates.add( pattern );
            } else {
                CypherEveryPathPattern candidate = new CypherEveryPathPattern( nodes, edges );
                candidates.add( pattern instanceof CypherNamedPattern named ? new CypherNamedPattern( named.getVariable(), candidate ) : candidate );
            }
        }
        return new MatchPropertyMaps( candidates, checks, internalBindings );
    }


    private static @Nullable MatchPropertySplit splitMatchProperties( @Nullable CypherExpression properties, @Nullable CypherVariable binding,
            Set<String> occupiedBindings, Set<PolyString> internalBindings, List<MatchPropertyCheck> checks ) {
        if ( !(properties instanceof CypherLiteral literal) || literal.getLiteralType() != CypherLiteral.Literal.MAP || !isComputedProperty( literal ) ) {
            return null;
        }
        List<StringPos> literalKeys = new ArrayList<>();
        List<CypherExpression> literalValues = new ArrayList<>();
        List<StringPos> expressionKeys = new ArrayList<>();
        List<CypherExpression> expressions = new ArrayList<>();
        literal.getMapValue().forEach( ( key, value ) -> {
            StringPos position = new StringPos( key, value.pos );
            if ( isComputedProperty( value ) ) {
                expressionKeys.add( position );
                expressions.add( value );
            } else {
                literalKeys.add( position );
                literalValues.add( value );
            }
        } );
        if ( binding == null ) {
            int suffix = internalBindings.size();
            String name = "$matchProperty" + suffix;
            while ( !occupiedBindings.add( name ) ) {
                name = "$matchProperty" + ++suffix;
            }
            binding = new CypherVariable( literal.pos, name );
            internalBindings.add( PolyString.of( name ) );
        }
        CypherLiteral expressionMap = new CypherLiteral( literal.pos, CypherLiteral.Literal.MAP, expressionKeys, expressions );
        checks.add( new MatchPropertyCheck( PolyString.of( binding.getName() ), expressionMap ) );
        return new MatchPropertySplit( binding, new CypherLiteral( literal.pos, CypherLiteral.Literal.MAP, literalKeys, literalValues ) );
    }


    private record MatchPropertyMaps( List<CypherPattern> candidates, List<MatchPropertyCheck> checks, Set<PolyString> internalBindings ) {

        private static final MatchPropertyMaps NONE = new MatchPropertyMaps( List.of(), List.of(), Set.of() );

    }


    private record MatchPropertyCheck( PolyString binding, CypherLiteral properties ) {

    }


    private record MatchPropertySplit( CypherVariable binding, CypherLiteral literalProperties ) {

    }


    private void convertJoinedMatch( CypherMatch clause, CypherContext context, List<CypherExpression> inlinePredicates,
            MatchPropertyMaps propertyMaps, Set<Integer> shortest ) {
        MatchRows right = null;
        boolean checkRelationshipUniqueness = propertyMaps.candidates().size() > 1;
        boolean selectAfterPredicates = !shortest.isEmpty();
        boolean independentPath = shortest.size() == 1 && hasOnlyNodeSiblings( propertyMaps, shortest.iterator().next() );
        LogicalLpgMatch shortestTraversal = null;
        List<Integer> shortestPathOffsets = new ArrayList<>();
        Set<String> shortestBindings = new HashSet<>();
        int nextHiddenOffset = 0;
        for ( int i = 0; i < propertyMaps.candidates().size(); i++ ) {
            CypherPattern pattern = propertyMaps.candidates().get( i );
            CypherContext candidate = new CypherContext( context.original, context.graph, cluster, algBuilder, rexBuilder, snapshot );
            candidate.active = clause;
            Pair<PolyString, RexNode> namedMatch = pattern.getPatternMatch( candidate );
            boolean shortestPattern = shortest.contains( i );
            candidate.add( namedMatch );
            // Candidate rows need complete graph values, including unnamed nodes and paths.
            candidate.combineMatch( true );
            AlgNode match = candidate.build();
            if ( shortestPattern && independentPath ) {
                shortestTraversal = (LogicalLpgMatch) match;
            }
            RexIndexRef matchedValue = rexBuilder.makeInputRef( match, 0 );
            boolean isPath = matchedValue.getType().getPolyType() == PolyType.PATH;
            // Anonymous sibling nodes also identify separate matches when selecting a shortest path.
            List<RexNode> hidden = selectAfterPredicates || (isPath && checkRelationshipUniqueness) ? List.of( matchedValue ) : List.of();
            if ( shortestPattern && selectAfterPredicates ) {
                shortestPathOffsets.add( nextHiddenOffset );
                if ( pattern instanceof CypherNamedPattern named ) {
                    shortestBindings.add( named.getVariable().getName() );
                }
                CypherVariable relationship = everyPath( pattern ).getEdges().get( 0 ).getVariable();
                if ( relationship != null ) {
                    shortestBindings.add( relationship.getName() );
                }
            }
            nextHiddenOffset += hidden.size();
            Map<String, Boolean> pathBindings = isPath ? Map.of( namedMatch.left.value, pattern instanceof CypherNamedPattern ) : Map.of();
            MatchRows bindings = projectMatchRows( match, matchBindings( new MatchRows( match, 0 ), 0, pathBindings ), hidden );
            right = right == null ? bindings : joinBindings( right, bindings, JoinAlgType.INNER, List.of(), MatchPropertyMaps.NONE, null, context, true, null );
        }

        materializePathBindings( context );
        AlgNode left = context.pop();
        if ( left == null ) {
            // The first OPTIONAL MATCH has one input row with no variables.
            left = LogicalLpgValues.create( cluster, cluster.traitSet(), new AlgRecordType( List.of() ), ImmutableList.of( ImmutableList.of() ) );
        }
        MatchRows rightRows = Objects.requireNonNull( right );
        ShortestMatch selection = selectAfterPredicates ? shortestMatch( rightRows, shortestPathOffsets, shortestBindings, shortestTraversal ) : null;
        JoinAlgType joinType = clause.isOptional() ? JoinAlgType.LEFT : JoinAlgType.INNER;
        context.add( joinBindings( new MatchRows( left, 0 ), rightRows, joinType, inlinePredicates, propertyMaps, clause.getWhere(), context, false, selection ).node() );
    }


    private static boolean hasOnlyNodeSiblings( MatchPropertyMaps maps, int shortestIndex ) {
        // Another relationship pattern could rule out a shortest trail by relationship uniqueness.
        for ( int i = 0; i < maps.candidates().size(); i++ ) {
            if ( i != shortestIndex && !everyPath( maps.candidates().get( i ) ).getEdges().isEmpty() ) {
                return false;
            }
        }
        return true;
    }


    private static CypherEveryPathPattern everyPath( CypherPattern pattern ) {
        return (CypherEveryPathPattern) (pattern instanceof CypherNamedPattern named ? named.getAnonymousPattern() : pattern);
    }


    private static ShortestMatch shortestMatch( MatchRows rows, List<Integer> pathOffsets, Set<String> shortestBindings,
            @Nullable LogicalLpgMatch traversal ) {
        List<Integer> pathColumns = pathOffsets.stream().map( i -> rows.pathFields().get( i ).getIndex() ).toList();
        List<Integer> groupColumns = new ArrayList<>();
        List<Integer> pathDependentColumns = new ArrayList<>( pathColumns );
        for ( AlgDataTypeField field : rows.visibleFields() ) {
            if ( shortestBindings.contains( field.getName() ) ) {
                pathDependentColumns.add( field.getIndex() );
            } else {
                groupColumns.add( field.getIndex() );
            }
        }
        for ( AlgDataTypeField field : rows.pathFields() ) {
            if ( !pathColumns.contains( field.getIndex() ) ) {
                groupColumns.add( field.getIndex() );
            }
        }
        return new ShortestMatch( pathColumns, groupColumns, pathDependentColumns, traversal );
    }


    private record ShortestMatch( List<Integer> pathColumns, List<Integer> groupColumns, List<Integer> pathDependentColumns,
                                  @Nullable LogicalLpgMatch traversal ) {

        private boolean canSelectDuringTraversal( RexNode condition, int offset ) {
            if ( traversal == null || !RexUtil.isDeterministic( condition ) ) {
                return false;
            }
            ImmutableBitSet columns = AlgOptUtil.InputFinder.bits( condition );
            // A bound relationship or list contributes its own equality predicate on a right-hand path column.
            return pathDependentColumns.stream().noneMatch( index -> columns.get( offset + index ) );
        }


        private AlgNode selectDuringTraversal( AlgNode right ) {
            return right.accept( new AlgShuttleImpl() {
                @Override
                public AlgNode visit( LogicalLpgMatch match ) {
                    if ( match != traversal ) {
                        return super.visit( match );
                    }
                    RexCall path = match.getMatches().get( 0 );
                    RexCall shortest = new RexCall( path.getType(),
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_SHORTEST_PATH_MATCH ), path.operands );
                    return new LogicalLpgMatch( match.getCluster(), match.getTraitSet(), match.getInput(), List.of( shortest ), match.getNames(), true );
                }
            } );
        }
    }


    private MatchRows joinBindings( MatchRows leftRows, MatchRows rightRows, JoinAlgType type, List<CypherExpression> inlinePredicates, MatchPropertyMaps propertyMaps,
            @Nullable CypherWhere where, CypherContext context, boolean clausePatterns, @Nullable ShortestMatch selection ) {
        AlgNode left = leftRows.node();
        AlgNode right = rightRows.node();
        int offset = left.getTupleType().getFieldCount();
        Map<PolyString, RexNode> leftBindings = matchBindings( leftRows, 0, Map.of() );
        Map<PolyString, RexNode> rightBindings = matchBindings( rightRows, offset, Map.of() );
        List<MatchBindingCheck> bindingChecks = new ArrayList<>();
        List<RexNode> conditions = new ArrayList<>();
        Map<PolyString, RexNode> scope = new LinkedHashMap<>( leftBindings );
        rightBindings.forEach( ( name, value ) -> {
            RexNode current = scope.putIfAbsent( name, value );
            if ( current != null ) {
                OperatorName check = matchBindingValidator( current.getType(), value.getType() );
                if ( check != null ) {
                    bindingChecks.add( new MatchBindingCheck( check, current ) );
                }
                // Multiple patterns in one MATCH cannot share a relationship binding, even an empty list.
                boolean repeatedRelationship = clausePatterns && isRelationshipList( current.getType() ) && isRelationshipList( value.getType() );
                conditions.add( repeatedRelationship ? rexBuilder.makeLiteral( false ) : sameBinding( name, current, value ) );
            }
        } );
        // Patterns in the same clause cannot reuse a relationship, even when that relationship is unnamed.
        List<RexNode> paths = new ArrayList<>();
        if ( clausePatterns ) {
            List<RexNode> leftHidden = matchElements( leftRows, 0 );
            List<RexNode> rightHidden = matchElements( rightRows, offset );
            List<RexNode> leftPaths = leftHidden.stream().filter( value -> value.getType().getPolyType() == PolyType.PATH ).toList();
            List<RexNode> rightPaths = rightHidden.stream().filter( value -> value.getType().getPolyType() == PolyType.PATH ).toList();
            for ( RexNode leftPath : leftPaths ) {
                for ( RexNode rightPath : rightPaths ) {
                    conditions.add( rexBuilder.makeCall( context.booleanType,
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_PATHS_DISJOINT ),
                            List.of( leftPath, rightPath ) ) );
                }
            }
            paths.addAll( leftHidden );
            paths.addAll( rightHidden );
        }
        RexNode whereCondition = null;
        List<RexNode> inlineConditions = new ArrayList<>();
        if ( where != null || !inlinePredicates.isEmpty() || !propertyMaps.checks().isEmpty() ) {
            // Resolve WHERE against the visible bindings, then rewrite its references to the join inputs.
            propertyMaps.internalBindings().forEach( scope::remove );
            AlgNode combined = LogicalLpgJoin.create( left, right, rexBuilder.makeLiteral( true ), JoinAlgType.INNER );
            context.add( projectBindings( combined, scope ) );
            List<RexNode> arguments = new ArrayList<>( scope.values() );
            RexShuttle joinRefs = new RexShuttle() {
                @Override
                public RexNode visitIndexRef( RexIndexRef ref ) {
                    return arguments.get( ref.getIndex() );
                }
            };
            // Literal entries have already constrained the structural match. Evaluate the remaining
            // values with both sets of bindings, before OPTIONAL MATCH can introduce null rows.
            for ( MatchPropertyCheck check : propertyMaps.checks() ) {
                RexNode candidate = Objects.requireNonNull( rightBindings.get( check.binding() ) );
                RexNode properties = check.properties().getRex( context, RexType.PROJECT ).right.accept( joinRefs );
                inlineConditions.add( rexBuilder.makeCall( context.booleanType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_MATCH_PROPERTIES ), List.of( candidate, properties ) ) );
            }
            for ( CypherExpression inline : inlinePredicates ) {
                inlineConditions.add( context.booleanPredicate( inline.getRex( context, RexType.FILTER ).right ).accept( joinRefs ) );
            }
            if ( where != null ) {
                whereCondition = simplifyWhere( context.booleanPredicate( where.getExpression().getRex( context, RexType.FILTER ).right ).accept( joinRefs ) );
            }
            context.pop();
        }

        boolean whereRejectsMatch = whereCondition != null && (RexUtil.isNull( whereCondition ) || whereCondition.isAlwaysFalse());
        if ( !inlineConditions.isEmpty() ) {
            RexNode inline = combineInlineMatchPredicates( inlineConditions, context );
            // Inline predicates can raise an error before the clause WHERE rejects the match.
            conditions.add( whereRejectsMatch ? requireBothMatchPredicates( inline, rexBuilder.makeLiteral( false ), context ) : inline );
        }
        if ( whereCondition != null && (inlineConditions.isEmpty() || !whereRejectsMatch) ) {
            conditions.add( whereCondition );
        }
        boolean nullPredicate = conditions.removeIf( RexUtil::isNull );
        RexNode condition = RexUtil.composeConjunction( rexBuilder, conditions, false );
        // A false predicate rules out matching the pattern. Otherwise validate the bindings
        // even if the traversal supplies no candidate for the join predicate.
        boolean validate = !bindingChecks.isEmpty() && !condition.isAlwaysFalse();
        boolean rejectInput = nullPredicate && type == JoinAlgType.INNER && validate;
        if ( validate ) {
            RexNode validation = matchBindingChecks( bindingChecks, context, true );
            // A bare null WHERE cannot match. For an inner join, reject input via the validator,
            // which returns true or throws, so the planner cannot discard the validation.
            if ( rejectInput ) {
                validation = RexUtil.not( validation );
            } else if ( whereCondition != null && !RexUtil.isNull( whereCondition )
                    && RexUtil.isDeterministic( whereCondition ) && AlgOptUtil.InputFinder.bits( whereCondition ).length() <= offset ) {
                // Only consult a left-only guard to decide whether an incompatible binding
                // should raise. Evaluate WHERE on candidates alone when the binding is valid.
                RexNode isFalse = rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_FALSE ), whereCondition );
                RexNode compatible = matchBindingChecks( bindingChecks, context, false );
                validation = rexBuilder.makeCall( context.booleanType,
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_MATCH_BINDING_GUARD ),
                        List.of( compatible, isFalse, validation ) );
            }
            left = LogicalLpgFilter.create( left, validation );
        }
        RexNode joinCondition = nullPredicate && !rejectInput ? rexBuilder.makeLiteral( false ) : condition;
        AlgNode join;
        if ( selection == null ) {
            join = LogicalLpgJoin.create( left, right, joinCondition, type );
        } else if ( selection.canSelectDuringTraversal( joinCondition, offset ) ) {
            join = LogicalLpgJoin.create( left, selection.selectDuringTraversal( right ), joinCondition, type );
        } else {
            join = new LogicalLpgShortestPathJoin( left, right, joinCondition, type == JoinAlgType.LEFT, selection.pathColumns(), selection.groupColumns() );
        }
        Map<PolyString, RexNode> output = new LinkedHashMap<>( leftBindings );
        RexNode hiddenBindings = propertyMaps.internalBindings().isEmpty() ? null : rexBuilder.makeArray(
                context.typeFactory.createArrayType( context.typeFactory.createPolyType( PolyType.VARCHAR ), -1 ),
                propertyMaps.internalBindings().stream().sorted().map( name -> (PolyValue) name ).toList() );
        // Derive new bindings from the join's row type, which makes its optional side nullable.
        for ( AlgDataTypeField field : rightRows.visibleFields() ) {
            if ( field.getName() != null && !propertyMaps.internalBindings().contains( PolyString.of( field.getName() ) ) ) {
                int index = offset + field.getIndex();
                RexNode value = rexBuilder.makeInputRef( join, index );
                if ( hiddenBindings != null && field.getType().getPolyType() == PolyType.PATH ) {
                    value = rexBuilder.makeCall( value.getType(),
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_MATCH_VISIBLE_PATH ), List.of( value, hiddenBindings ) );
                }
                output.putIfAbsent( PolyString.of( field.getName() ), value );
            }
        }
        return projectMatchRows( join, output, paths );
    }


    private RexNode combineInlineMatchPredicates( List<RexNode> predicates, CypherContext context ) {
        if ( predicates.size() == 1 || predicates.stream().noneMatch( RexUtil::isNull ) ) {
            return RexUtil.composeConjunction( rexBuilder, predicates );
        }
        // A bare null makes the conjunction unable to match. Keep the other inline
        // predicates in the plan, since evaluating them for a candidate can still fail.
        RexNode result = predicates.get( 0 );
        for ( int i = 1; i < predicates.size(); i++ ) {
            result = requireBothMatchPredicates( result, predicates.get( i ), context );
        }
        return result;
    }


    private RexNode requireBothMatchPredicates( RexNode left, RexNode right, CypherContext context ) {
        return rexBuilder.makeCall( context.booleanType,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_MATCH_WITH_WHERE ),
                List.of( left, right ) );
    }


    private RexNode matchBindingChecks( List<MatchBindingCheck> checks, CypherContext context, boolean reportError ) {
        RexNode report = rexBuilder.makeLiteral( reportError );
        return RexUtil.composeConjunction( rexBuilder, checks.stream().map( check -> rexBuilder.makeCall( context.booleanType,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), check.operator() ), List.of( check.value(), report ) ) ).toList() );
    }


    private record MatchBindingCheck( OperatorName operator, RexNode value ) {

    }


    private Map<PolyString, RexNode> matchBindings( MatchRows input, int offset, Map<String, Boolean> pathBindings ) {
        Map<PolyString, RexNode> bindings = new LinkedHashMap<>();
        for ( AlgDataTypeField field : input.visibleFields() ) {
            AlgDataType type = field.getType();
            RexNode ref = rexBuilder.makeInputRef( type, offset + field.getIndex() );
            if ( field.getName() != null && pathBindings.getOrDefault( field.getName(), true ) ) {
                bindings.put( PolyString.of( field.getName() ), ref );
            }
            if ( field.getName() != null && type.getPolyType() == PolyType.PATH && pathBindings.containsKey( field.getName() ) ) {
                for ( AlgDataTypeField variable : type.getFields() ) {
                    bindings.putIfAbsent( PolyString.of( variable.getName() ), rexBuilder.makeCall(
                            contextPathFieldType( type, variable ),
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXTRACT_FROM_PATH ),
                            List.of( ref, rexBuilder.makeLiteral( variable.getName() ) ) ) );
                }
            }
        }
        return bindings;
    }


    private List<RexNode> matchElements( MatchRows rows, int offset ) {
        return rows.pathFields().stream()
                .map( field -> (RexNode) rexBuilder.makeInputRef( field.getType(), offset + field.getIndex() ) )
                .toList();
    }


    private AlgDataType contextPathFieldType( AlgDataType path, AlgDataTypeField field ) {
        return cluster.getTypeFactory().createTypeWithNullability( field.getType(), path.isNullable() || field.getType().isNullable() );
    }


    private RexNode sameBinding( PolyString name, RexNode left, RexNode right ) {
        PolyType leftType = left.getType().getPolyType();
        PolyType rightType = right.getType().getPolyType();
        boolean element = rightType == PolyType.NODE || rightType == PolyType.EDGE;
        boolean relationshipList = isRelationshipList( right.getType() );
        if ( element && (leftType == rightType || isDynamicBinding( leftType )) ) {
            RexNode rightId = elementId( right );
            RexNode leftId = leftType == rightType ? elementId( left )
                    : rexBuilder.makeCall( cluster.getTypeFactory().createTypeWithNullability( rightId.getType(), true ),
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_MATCH_ELEMENT_ID ),
                            List.of( left, rexBuilder.makeLiteral( rightType.name() ) ) );
            return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), leftId, rightId );
        }
        if ( relationshipList && (isDynamicBinding( leftType ) || isRelationshipList( left.getType() )) ) {
            // Cypher equality compares relationship lists in traversal order, and a null result
            // cannot satisfy the join.
            AlgDataTypeFactory types = cluster.getTypeFactory();
            AlgDataType result = types.createTypeWithNullability( types.createPolyType( PolyType.BOOLEAN ), true );
            return rexBuilder.makeCall( result, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EQUALS ), List.of( left, right ) );
        }
        throw new CypherSyntaxException( "Variable '" + name.value + "' has incompatible bindings in MATCH: "
                + left.getType() + " and " + right.getType() + "." );
    }


    private static boolean isDynamicBinding( PolyType type ) {
        return type == PolyType.ANY || type == PolyType.NULL;
    }


    @Nullable
    private static OperatorName matchBindingValidator( AlgDataType boundType, AlgDataType patternType ) {
        if ( boundType.getPolyType() != PolyType.ANY ) {
            return null;
        }
        if ( patternType.getPolyType() == PolyType.NODE ) {
            return OperatorName.CYPHER_VALIDATE_MATCH_NODE;
        }
        return isRelationshipList( patternType ) ? OperatorName.CYPHER_VALIDATE_MATCH_RELATIONSHIP_LIST : null;
    }


    private RexNode elementId( RexNode element ) {
        AlgDataType idType = cluster.getTypeFactory().createTypeWithNullability(
                cluster.getTypeFactory().createPolyType( PolyType.VARCHAR ), element.getType().isNullable() );
        return rexBuilder.makeCall( idType,
                OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXTRACT_ID ), List.of( element ) );
    }


    private AlgNode projectBindings( AlgNode node, Map<PolyString, RexNode> bindings ) {
        return projectMatchRows( node, bindings, List.of() ).node();
    }


    private MatchRows projectMatchRows( AlgNode node, Map<PolyString, RexNode> bindings, List<RexNode> hiddenPaths ) {
        List<PolyString> names = new ArrayList<>( bindings.keySet() );
        List<RexNode> outputs = new ArrayList<>( bindings.values() );
        for ( int i = 0; i < hiddenPaths.size(); i++ ) {
            String name = "$matchPath" + i;
            while ( bindings.containsKey( PolyString.of( name ) ) ) {
                name += "_";
            }
            names.add( PolyString.of( name ) );
        }
        outputs.addAll( hiddenPaths );
        return new MatchRows( LogicalLpgProject.create( node, outputs, names ), hiddenPaths.size() );
    }


    private record MatchRows( AlgNode node, int pathCount ) {

        private List<AlgDataTypeField> visibleFields() {
            List<AlgDataTypeField> fields = node.getTupleType().getFields();
            return fields.subList( 0, fields.size() - pathCount );
        }


        private List<AlgDataTypeField> pathFields() {
            List<AlgDataTypeField> fields = node.getTupleType().getFields();
            return fields.subList( fields.size() - pathCount, fields.size() );
        }

    }


    private void convertWhere( CypherWhere where, CypherContext context ) {
        Pair<PolyString, RexNode> predicate = where.getExpression().getRex( context, RexType.FILTER );
        RexNode condition = simplifyWhere( context.booleanPredicate( predicate.right ) );
        context.add( predicate.left, RexUtil.isNull( condition ) ? rexBuilder.makeLiteral( false ) : condition );
    }


    private RexNode simplifyWhere( RexNode predicate ) {
        RexNode folded = predicate.accept( new RexShuttle() {
            @Override
            public RexNode visitCall( RexCall call ) {
                RexCall rewritten = (RexCall) super.visitCall( call );
                OperatorName op = rewritten.getOperator().getOperatorName();
                if ( (op == OperatorName.CYPHER_EQUALS || op == OperatorName.CYPHER_NOT_EQUALS)
                        && rewritten.operands.get( 0 ) instanceof RexLiteral left && rewritten.operands.get( 1 ) instanceof RexLiteral right ) {
                    PolyBoolean equal = CypherFunctions.equal( left.value, right.value );
                    return equal == null ? rexBuilder.makeNullLiteral( rewritten.getType() )
                            : rexBuilder.makeLiteral( equal.value == (op == OperatorName.CYPHER_EQUALS) );
                }
                return rewritten;
            }
        } );
        return new RexSimplify( rexBuilder, AlgOptPredicateList.EMPTY, RexUtil.EXECUTOR ).simplify( folded );
    }


    private void convertPattern( CypherPattern pattern, CypherContext context ) {
        if ( context.kind == Kind.INSERT ) {
            // convert "values" pattern (LogicalGraphValues AlgNode)
            pattern.getPatternValues( context );
        } else {
            // convert filter pattern ( RexNode CYPHER_PATTERN_MATCH )
            Pair<PolyString, RexNode> match = pattern.getPatternMatch( context );
            if ( match.right.getType().getPolyType() == PolyType.PATH ) {
                context.implicitPathBindings.put( match.left.value, pattern instanceof CypherNamedPattern );
            }
            context.add( match );
        }
    }


    private void setKindIfNull( CypherContext context, Kind kind ) {
        if ( context.kind == null ) {
            context.kind = kind;
        }
    }


    public enum RexType {
        PROJECT,
        FILTER
    }


    public static class CypherContext {

        public final AlgCluster cluster;
        public final AlgBuilder algBuilder;
        public final RexBuilder rexBuilder;
        public final CypherNode original;
        public final LogicalEntity graph;
        public final AlgDataType graphType;
        public final AlgDataType booleanType;
        public final AlgDataType geometryType;
        public final AlgDataType nodeType;
        public final AlgDataType edgeType;
        public final AlgDataType pathType;
        public final AlgDataType numberType;
        public final Snapshot snapshot;
        public final AlgDataTypeFactory typeFactory;
        private final Stack<AlgNode> stack = new Stack<>();
        // named projects, null if no name provided
        private final Queue<Pair<PolyString, RexNode>> rexQueue = new LinkedList<>();
        private final Queue<Pair<String, AggregateCall>> rexAggQueue = new LinkedList<>();
        private final Map<PolyString, PolyNode> nodes = new HashMap<>();
        // Paths still represented by a single match column. The value says whether the path itself has a user alias.
        private final Map<String, Boolean> implicitPathBindings = new HashMap<>();
        private final Map<String, RexNameRef> quantifiedBindings = new HashMap<>();
        private int quantifiedDepth;
        private Function<String, RexNode> scopedVariables;
        private Function<CypherExpression, RexNode> scopedExpressions;
        public CypherNode active;
        public Kind kind;
        private boolean writeHasConsumer;
        private boolean createReturning;
        private LogicalLpgModify returningInsert;
        private List<Pair<PolyString, EdgeVariableHolder>> edges = new LinkedList<>();
        private final Map<PolyString, PolyPath> createdPaths = new LinkedHashMap<>();
        private final Map<PolyString, List<Pair<PolyString, RexNode>>> createProperties = new HashMap<>();


        private CypherContext(
                CypherNode original,
                LogicalEntity graph,
                AlgCluster cluster,
                AlgBuilder algBuilder,
                RexBuilder rexBuilder,
                Snapshot snapshot ) {
            this.original = original;
            this.graph = graph;
            this.cluster = cluster;
            this.algBuilder = algBuilder;
            this.rexBuilder = rexBuilder;
            this.graphType = cluster.getTypeFactory().createPolyType( PolyType.GRAPH );
            this.booleanType = cluster.getTypeFactory().createPolyType( PolyType.BOOLEAN );
            this.geometryType = cluster.getTypeFactory().createPolyType( PolyType.GEOMETRY );
            this.nodeType = cluster.getTypeFactory().createPolyType( PolyType.NODE );
            this.edgeType = cluster.getTypeFactory().createPolyType( PolyType.EDGE );
            this.pathType = cluster.getTypeFactory().createPolyType( PolyType.PATH );
            this.numberType = cluster.getTypeFactory().createPolyType( PolyType.INTEGER );
            this.typeFactory = cluster.getTypeFactory();
            this.snapshot = snapshot;
        }


        public RexNode graphValue() {
            return CypherGraphInputs.graphReference( graphType );
        }


        public void addDefaultScanIfNecessary() {
            if ( !stack.isEmpty() ) {
                return;
            }
            LogicalLpgScan lps = new LogicalLpgScan(
                    cluster,
                    cluster.traitSet(),
                    graph,
                    GraphType.of() );
            stack.add( lps );
        }


        public boolean isImplicitPathBinding( String name ) {
            return implicitPathBindings.containsKey( name );
        }


        public boolean isVisibleVariable( String name ) {
            return !Boolean.FALSE.equals( implicitPathBindings.get( name ) );
        }


        @Nullable
        public RexNode scopedVariable( String name ) {
            RexNameRef quantified = quantifiedBindings.get( name );
            if ( quantified != null ) {
                return quantified;
            }
            if ( scopedVariables == null ) {
                return null;
            }
            RexNode variable = scopedVariables.apply( name );
            if ( variable == null ) {
                throw new CypherSyntaxException( "The used variable is not known: " + name );
            }
            return variable;
        }


        @Nullable
        public RexNode projectedExpression( CypherExpression expression ) {
            return scopedExpressions == null || !quantifiedBindings.isEmpty() ? null : scopedExpressions.apply( expression );
        }


        public Pair<RexNameRef, RexNode> quantifiedPredicate( String variable, AlgDataType component, CypherExpression predicate ) {
            RexNameRef element = RexNameRef.scoped( "$cypherListItem" + quantifiedDepth++, typeFactory.createTypeWithNullability( component, true ) );
            RexNameRef previous = quantifiedBindings.put( variable, element );
            try {
                return Pair.of( element, booleanPredicate( predicate.getRex( this, RexType.FILTER ).right ) );
            } finally {
                quantifiedDepth--;
                if ( previous == null ) {
                    quantifiedBindings.remove( variable );
                } else {
                    quantifiedBindings.put( variable, previous );
                }
            }
        }


        private RexNode resolveInScope( CypherExpression expression, RexType type, Function<String, RexNode> variables,
                @Nullable Function<CypherExpression, RexNode> expressions ) {
            Function<String, RexNode> previousVariables = scopedVariables;
            Function<CypherExpression, RexNode> previousExpressions = scopedExpressions;
            scopedVariables = variables;
            scopedExpressions = expressions;
            try {
                return expression.getRex( this, type ).right;
            } finally {
                scopedVariables = previousVariables;
                scopedExpressions = previousExpressions;
            }
        }


        private void combineMatch( boolean enumerableOnly ) {
            addDefaultScanIfNecessary();
            List<Pair<PolyString, RexNode>> matches = getMatches();

            List<RexCall> calls = Pair.right( matches ).stream().map( r -> (RexCall) r ).toList();

            stack.add( new LogicalLpgMatch( cluster, cluster.traitSet(), stack.pop(), calls, Pair.left( matches ), enumerableOnly ) );
            clearVariables();
        }


        private void clearVariables() {
            nodes.clear();
            edges.clear();
            createdPaths.clear();
            createProperties.clear();
        }


        private List<Pair<PolyString, RexNode>> getMatches() {
            ArrayList<Pair<PolyString, RexNode>> namedMatch = new ArrayList<>();

            while ( !rexQueue.isEmpty() ) {
                namedMatch.add( rexQueue.remove() );
            }
            return namedMatch;
        }


        public RexNode booleanPredicate( RexNode predicate ) {
            if ( RexUtil.isNull( predicate ) ) {
                return rexBuilder.makeNullLiteral( booleanType );
            }
            return switch ( predicate.getType().getPolyType() ) {
                case BOOLEAN -> predicate;
                case ANY, NULL -> rexBuilder.makeCall( typeFactory.createTypeWithNullability( booleanType, true ),
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_BOOLEAN_PREDICATE ), List.of( predicate ) );
                default -> throw new CypherSyntaxException( "Cypher predicates require a BOOLEAN, got " + predicate.getType() + "." );
            };
        }


        public void combineFilter() {
            addDefaultScanIfNecessary();
            assert stack.size() == 1 : "Error while combining Cypher filter";

            addProjectIfNecessary();

            RexNode condition = getCondition();

            stack.add( new LogicalLpgFilter( cluster, cluster.traitSet(), stack.pop(), condition ) );
            clearVariables();
        }


        private void addProjectIfNecessary() {
            if ( stack.size() >= 1 && rexQueue.size() > 0 ) {
                AlgNode node = stack.peek();
                if ( node.getTupleType().getFields().size() == 1
                        && node.getTupleType().getFields().get( 0 ).getType().getPolyType() == PolyType.GRAPH ) {
                    node = stack.pop();

                    List<Pair<PolyString, RexNode>> rex = new ArrayList<>();
                    rexQueue.iterator().forEachRemaining( rex::add );
                    stack.add( new LogicalLpgProject( node.getCluster(), node.getTraitSet(), node, Pair.right( rex ), Pair.left( rex ) ) );
                }
            }
        }


        private RexNode getCondition() {
            List<RexNode> nodes = new ArrayList<>();
            while ( !rexQueue.isEmpty() ) {
                Pair<PolyString, RexNode> popped = rexQueue.remove();
                nodes.add( popped.right );
            }

            return nodes.size() == 1 ? nodes.get( 0 ) : algBuilder.and( nodes );
        }


        public void add( AlgNode node ) {
            this.stack.add( node );
        }


        public void add( PolyString name, RexNode node ) {
            this.rexQueue.add( Pair.of( name, node ) );
        }


        public void add( Pair<PolyString, RexNode> namedNode ) {
            this.rexQueue.add( namedNode );
        }


        @Nullable
        public RexNode getRexNode( String key ) {
            List<Pair<PolyString, RexNode>> nodes = popNodes();
            List<PolyString> names = Pair.left( nodes );
            int i = names.indexOf( PolyString.of( key ) );
            if ( i >= 0 ) {
                Pair<PolyString, RexNode> node = nodes.get( i );
                nodes.remove( i );

                for ( Pair<PolyString, RexNode> pair : nodes ) {
                    add( pair );
                }

                return node.right;
            }
            for ( Pair<PolyString, RexNode> pair : nodes ) {
                add( pair );
            }

            return null;
        }


        public void addAgg( Pair<String, AggregateCall> agg ) {
            this.rexAggQueue.add( agg );
        }


        public AlgNode build() {
            assert stack.size() == 1;
            return stack.pop();
        }


        public boolean hasInput() {
            return !stack.isEmpty();
        }


        public AlgNode peek() {
            return stack.peek();
        }


        public AlgNode pop() {
            if ( !stack.isEmpty() ) {
                return stack.pop();
            }
            return null;
        }


        public List<Pair<PolyString, RexNode>> popNodes() {
            List<Pair<PolyString, RexNode>> namedNodes = new ArrayList<>( rexQueue );
            rexQueue.clear();
            return namedNodes;
        }


        public List<Pair<String, AggregateCall>> popAggNodes() {
            List<Pair<String, AggregateCall>> namedNodes = new ArrayList<>( rexAggQueue );
            rexAggQueue.clear();
            return namedNodes;
        }


        public Pair<PolyString, RexNode> getPropertyExtract( String key, String subject, RexNode field ) {
            RexNode extractedProperty = rexBuilder.makeCall(
                    typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true ),
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXTRACT_PROPERTY ),
                    List.of( field, rexBuilder.makeLiteral( key ) ) );

            return Pair.of( PolyString.of( subject + "." + key ), extractedProperty );
        }


        public RexNode getBinaryOperation( OperatorName op, RexNode left, RexNode right ) {
            return switch ( op ) {
                case STARTS_WITH, ENDS_WITH, CYPHER_CONTAINS -> rexBuilder.makeCall(
                        typeFactory.createTypeWithNullability( booleanType, true ),
                        OperatorRegistry.get( QueryLanguage.from( "cypher" ), op ),
                        List.of( left, right ) );
                default -> rexBuilder.makeCall(
                        typeFactory.createTypeWithNullability( booleanType, left.getType().isNullable() || right.getType().isNullable() ),
                        OperatorRegistry.get( op ),
                        List.of( left, right ) );
            };
        }


        public boolean containsAggs() {
            return !rexAggQueue.isEmpty();
        }


        public AlgNode asValues( List<Pair<PolyString, RexNode>> nameAndValues ) {
            List<AlgDataTypeField> fields = new ArrayList<>();
            ImmutableList.Builder<RexLiteral> values = ImmutableList.builder();
            for ( Pair<PolyString, RexNode> nameAndValue : nameAndValues ) {
                fields.add( new AlgDataTypeFieldImpl( -1L, nameAndValue.left.value, fields.size(), nameAndValue.right.getType() ) );
                values.add( (RexLiteral) nameAndValue.right );
            }
            return LogicalLpgValues.create( cluster, cluster.traitSet(),
                    new AlgRecordType( fields ), ImmutableList.of( values.build() ) );
        }


        public void addNodes( List<Pair<PolyString, PolyNode>> nodes ) {
            for ( Pair<PolyString, PolyNode> node : nodes ) {
                if ( !this.nodes.containsKey( node.left ) ) {
                    this.nodes.put( node.left, node.right );
                } else if ( node.left == null || node.left.isNull() ) {
                    this.nodes.put( node.right.id, node.right );
                }
                // we don't add the variable again
            }
        }


        public void addEdges( List<Pair<PolyString, EdgeVariableHolder>> edges ) {
            this.edges.addAll( edges );
        }


        public void addCreatedPath( PolyString name, List<Pair<PolyString, PolyNode>> nodes, List<Pair<PolyString, EdgeVariableHolder>> edges ) {
            PolyPath path = PolyPath.create( nodes, edges.stream().map( edge -> Pair.of( edge.left, orientCreatedEdge( edge.right.edge ) ) ).toList() );
            if ( createdPaths.putIfAbsent( name, path ) != null ) {
                throw new GenericRuntimeException( "Cypher path variable is already bound: " + name.value );
            }
        }


        @Nullable
        public Pair<PolyString, PolyNode> getNodeVariable( PolyString name ) {
            if ( name != null && this.nodes.containsKey( name ) ) {
                return Pair.of( name, this.nodes.get( name ) );
            }
            return null;
        }


        public void combineValues() {
            List<Pair<PolyString, PolyNode>> nodes = popNodeValues();
            List<Pair<PolyString, EdgeVariableHolder>> edges = popEdgeValues();

            if ( usesCreateExpressions() ) {
                createRows( nodes, edges );
            } else {
                Stream.<GraphPropertyHolder>concat( nodes.stream().map( binding -> binding.right ), edges.stream().map( binding -> binding.right.edge ) )
                        .forEach( element -> {
                            element.properties.values().forEach( CypherFunctions::validatePropertyValue );
                            element.properties.values().removeIf( value -> value == null || value.isNull() );
                        } );
                LogicalLpgValues values = LogicalLpgValues.create(
                        cluster,
                        cluster.traitSet(),
                        nodes,
                        nodeType,
                        edges.stream().map( edge -> Pair.of( edge.left, orientCreatedEdge( edge.right.edge ) ) ).toList(),
                        edgeType );
                LogicalLpgModify insert = new LogicalLpgModify( cluster, cluster.traitSet(), graph, values, Modify.Operation.INSERT, null, null );
                if ( createReturning ) {
                    returningInsert = insert;
                    add( buildCreatedBindings( values ) );
                } else {
                    add( insert );
                }
            }
            clearVariables();
        }


        public boolean usesCreateExpressions() {
            return kind == Kind.INSERT && (!stack.isEmpty() || writeHasConsumer && !createReturning);
        }


        public void addCreateProperties( PolyString id, @Nullable CypherExpression expression ) {
            if ( expression == null ) {
                return;
            }
            if ( !(expression instanceof CypherLiteral literal) || literal.getLiteralType() != CypherLiteral.Literal.MAP ) {
                throw new GenericRuntimeException( "CREATE graph properties require a map of expressions" );
            }
            createProperties.put( id, literal.getMapValue().entrySet().stream().map( entry -> Pair.of(
                    PolyString.of( entry.getKey() ), entry.getValue().getRex( this, RexType.PROJECT ).right ) ).toList() );
        }


        private void createRows( List<Pair<PolyString, PolyNode>> nodes, List<Pair<PolyString, EdgeVariableHolder>> edges ) {
            AlgNode input = pop();
            List<String> inputNames = input.getTupleType().getFieldNames();
            String rowIdName = privateName( inputNames, "$create$row" );
            AlgNode withRowId = new LogicalLpgWriteRow( input, rowIdName );

            List<PolyString> bindings = inputNames.stream().map( PolyString::of )
                    .collect( Collectors.toCollection( ArrayList::new ) );
            Map<PolyString, Integer> graphSlots = new HashMap<>();
            List<Integer> writeSlots = new ArrayList<>();
            List<PolyString> graphNames = fieldNames( withRowId );
            List<RexNode> graphValues = refs( withRowId );
            RexNode rowId = graphValues.get( inputNames.size() );
            for ( Pair<PolyString, PolyNode> binding : nodes ) {
                PolyNode node = binding.right;
                PolyString name = node.getVariableName();
                int slot = name == null ? -1 : inputNames.indexOf( name.value );
                if ( slot < 0 ) {
                    slot = graphValues.size();
                    List<RexNode> args = new ArrayList<>( List.of( new RexLiteral( node, nodeType, PolyType.NODE ), rowId ) );
                    args.addAll( createPropertyArrays( node.id ) );
                    graphValues.add( createExpression( nodeType, OperatorName.CYPHER_CREATE_NODE, args ) );
                    graphNames.add( addCreateBinding( bindings, graphNames, name, slot ) );
                    writeSlots.add( slot );
                }
                graphSlots.put( node.id, slot );
            }

            AlgNode withNodes = new LogicalLpgProject( cluster, cluster.traitSet(), withRowId, graphValues, graphNames );
            graphValues = refs( withNodes );
            graphNames = fieldNames( withNodes );
            rowId = graphValues.get( inputNames.size() );
            for ( Pair<PolyString, EdgeVariableHolder> binding : edges ) {
                PolyEdge edge = orientCreatedEdge( binding.right.edge );
                int slot = graphValues.size();
                RexNode source = graphValues.get( graphSlots.get( edge.left ) );
                RexNode target = graphValues.get( graphSlots.get( edge.right ) );
                List<RexNode> args = new ArrayList<>( List.of( new RexLiteral( edge, edgeType, PolyType.EDGE ), rowId, source, target ) );
                args.addAll( createPropertyArrays( edge.id ) );
                graphValues.add( createExpression( edgeType, OperatorName.CYPHER_CREATE_EDGE, args ) );
                graphNames.add( addCreateBinding( bindings, graphNames, edge.getVariableName(), slot ) );
                graphSlots.put( edge.id, slot );
                writeSlots.add( slot );
            }
            AlgNode withEdges = new LogicalLpgProject( cluster, cluster.traitSet(), withNodes, graphValues, graphNames );
            graphValues = refs( withEdges );
            graphNames = fieldNames( withEdges );
            for ( Map.Entry<PolyString, PolyPath> entry : createdPaths.entrySet() ) {
                List<RexNode> parts = new ArrayList<>();
                for ( GraphPropertyHolder element : entry.getValue().getPath() ) {
                    parts.add( graphValues.get( graphSlots.get( element.id ) ) );
                }
                graphValues.add( createExpression( pathType, OperatorName.CYPHER_CREATE_PATH, List.of( expressionArray( parts ) ) ) );
                graphNames.add( addCreateBinding( bindings, graphNames, entry.getKey(), graphValues.size() - 1 ) );
            }
            AlgNode withPaths = new LogicalLpgProject( cluster, cluster.traitSet(), withEdges, graphValues, graphNames );
            projectCreateResult( withPaths, writeSlots, bindings );
        }


        private void projectCreateResult( AlgNode created, List<Integer> writeSlots, List<PolyString> bindings ) {
            List<String> fields = created.getTupleType().getFieldNames();
            if ( writeSlots.isEmpty() ) {
                throw new GenericRuntimeException( "CREATE must add a new node or relationship" );
            }
            List<RexNode> values = new ArrayList<>();
            List<PolyString> names = new ArrayList<>();
            for ( int slot : writeSlots ) {
                values.add( rexBuilder.makeInputRef( created, slot ) );
                names.add( PolyString.of( privateName( fields, "$create$write" + slot ) ) );
            }
            if ( !writeHasConsumer ) {
                AlgNode provider = new LogicalLpgProject( cluster, cluster.traitSet(), created, values, names );
                AlgNode write = new LogicalLpgModify( cluster, cluster.traitSet(), graph, provider, Modify.Operation.INSERT, null, null, writeSlots.size() );
                add( LogicalLpgAggregate.countWrites( write ) );
                return;
            }
            for ( PolyString binding : bindings ) {
                values.add( rexBuilder.makeInputRef( created, fields.indexOf( binding.value ) ) );
                names.add( binding );
            }
            AlgNode provider = new LogicalLpgProject( cluster, cluster.traitSet(), created, values, names );
            AlgNode write = new LogicalLpgModify( cluster, cluster.traitSet(), graph, provider, Modify.Operation.INSERT, null, null, writeSlots.size() );
            List<RexNode> result = new ArrayList<>();
            for ( int i = 0; i < bindings.size(); i++ ) {
                result.add( rexBuilder.makeInputRef( write, writeSlots.size() + i ) );
            }
            add( new LogicalLpgProject( cluster, cluster.traitSet(), write, result, bindings ) );
        }


        private List<RexNode> createPropertyArrays( PolyString id ) {
            List<Pair<PolyString, RexNode>> properties = createProperties.getOrDefault( id, List.of() );
            RexNode keys = rexBuilder.makeArray( typeFactory.createArrayType( typeFactory.createPolyType( PolyType.VARCHAR ), -1 ),
                    new ArrayList<>( Pair.left( properties ) ) );
            return List.of( keys, expressionArray( Pair.right( properties ) ) );
        }


        private RexNode expressionArray( List<RexNode> values ) {
            AlgDataType type = typeFactory.createArrayType( typeFactory.createPolyType( PolyType.ANY ), -1 );
            return rexBuilder.makeCall( type, OperatorRegistry.get( OperatorName.ARRAY_VALUE_CONSTRUCTOR ), values );
        }


        private RexNode createExpression( AlgDataType type, OperatorName name, List<RexNode> arguments ) {
            return rexBuilder.makeCall( type, OperatorRegistry.get( QueryLanguage.from( "cypher" ), name ), arguments );
        }


        private List<RexNode> refs( AlgNode input ) {
            return input.getTupleType().getFields().stream().map( field -> (RexNode) rexBuilder.makeInputRef( input, field.getIndex() ) )
                    .collect( Collectors.toCollection( ArrayList::new ) );
        }


        private static List<PolyString> fieldNames( AlgNode input ) {
            return input.getTupleType().getFieldNames().stream().map( PolyString::of ).collect( Collectors.toCollection( ArrayList::new ) );
        }


        private static PolyString addCreateBinding( List<PolyString> bindings, List<PolyString> fields, PolyString name, int slot ) {
            if ( name == null || name.isNull() ) {
                return PolyString.of( privateName( fields.stream().map( field -> field.value ).toList(), "$create$" + slot ) );
            }
            if ( bindings.contains( name ) ) {
                throw new GenericRuntimeException( "Cypher variable is already bound: " + name.value );
            }
            bindings.add( name );
            return name;
        }


        private static String privateName( List<String> fields, String base ) {
            String name = base;
            while ( fields.contains( name ) ) {
                name += "$";
            }
            return name;
        }


        private static PolyEdge orientCreatedEdge( PolyEdge edge ) {
            if ( edge.direction != EdgeDirection.RIGHT_TO_LEFT ) {
                return edge;
            }
            // Store the source and target explicitly regardless of the direction used to spell the pattern.
            return new PolyEdge( edge.id, edge.properties, edge.labels, edge.right, edge.left, EdgeDirection.LEFT_TO_RIGHT, edge.getVariableName() )
                    .fromTo( edge.fromTo() );
        }


        private LogicalLpgValues buildCreatedBindings( LogicalLpgValues values ) {
            List<AlgDataTypeField> fields = new ArrayList<>();
            ImmutableList.Builder<RexLiteral> bindings = ImmutableList.builder();
            Stream.<GraphPropertyHolder>concat( values.getNodes().stream(), values.getEdges().stream() )
                    .filter( element -> element.getVariableName() != null && !element.getVariableName().isNull() )
                    .forEach( element -> {
                        AlgDataType type = element.isNode() ? nodeType : edgeType;
                        fields.add( new AlgDataTypeFieldImpl( -1L, element.getVariableName().value, fields.size(), type ) );
                        bindings.add( new RexLiteral( element, type, element.type ) );
                    } );
            createdPaths.forEach( (name, path) -> {
                if ( fields.stream().anyMatch( field -> field.getName().equals( name.value ) ) ) {
                    throw new GenericRuntimeException( "Cypher path variable is already bound: " + name.value );
                }
                fields.add( new AlgDataTypeFieldImpl( -1L, name.value, fields.size(), pathType ) );
                bindings.add( new RexLiteral( path, pathType, PolyType.PATH ) );
            } );
            return LogicalLpgValues.create( cluster, cluster.traitSet(), new AlgRecordType( fields ), ImmutableList.of( bindings.build() ) );
        }


        private List<Pair<PolyString, EdgeVariableHolder>> popEdgeValues() {
            List<Pair<PolyString, EdgeVariableHolder>> edges = this.edges;
            this.edges = new LinkedList<>();
            return edges;
        }


        private List<Pair<PolyString, PolyNode>> popNodeValues() {
            List<Pair<PolyString, PolyNode>> nodes = this.nodes
                    .entrySet()
                    .stream()
                    .map( n -> Pair.of( n.getKey(), n.getValue() ) )
                    .toList();
            this.nodes.clear();
            return nodes;
        }


        public void combineUpdate( List<Pair<PolyString, RexNode>> actions ) {
            if ( actions.isEmpty() ) {
                throw new GenericRuntimeException( "Empty UPDATE is not possible" );
            }

            Map<PolyString, RexNode> targets = new LinkedHashMap<>();
            actions.forEach( action -> targets.putIfAbsent( action.left, action.right ) );
            List<Pair<PolyString, RexNode>> updates = targets.entrySet().stream().map( entry -> Pair.of( entry.getKey(), entry.getValue() ) ).toList();
            modify( Modify.Operation.UPDATE, updates, false, actions );
            kind = Kind.UPDATE;
        }


        public void combineDelete( boolean detach ) {
            if ( rexQueue.isEmpty() ) {
                throw new GenericRuntimeException( "Empty DELETE is not possible" );
            }

            List<Pair<PolyString, RexNode>> deletes = popNodes();

            modify( Modify.Operation.DELETE, deletes, detach );
            kind = Kind.DELETE;
        }


        private void modify( Modify.Operation operation, List<Pair<PolyString, RexNode>> updates ) {
            modify( operation, updates, false );
        }


        private void modify( Modify.Operation operation, List<Pair<PolyString, RexNode>> updates, boolean detachDelete ) {
            modify( operation, updates, detachDelete, List.of() );
        }


        private void modify( Modify.Operation operation, List<Pair<PolyString, RexNode>> updates,
                boolean detachDelete, List<Pair<PolyString, RexNode>> actions ) {
            AlgNode input = pop();
            if ( operation == Modify.Operation.UPDATE ) {
                // WITH values must be evaluated before the update expressions mutate their graph bindings.
                input = new LogicalLpgWriteRow( input );
            }
            List<RexNode> parameters = new ArrayList<>( Pair.right( updates ) );
            List<PolyString> names = new ArrayList<>();
            for ( int i = 0; i < updates.size(); i++ ) {
                names.add( PolyString.of( privateName( input.getTupleType().getFieldNames(), "polypheny_modify_" + i ) ) );
            }
            List<AlgDataTypeField> fields = input.getTupleType().getFields();
            if ( writeHasConsumer ) {
                for ( AlgDataTypeField field : fields ) {
                    parameters.add( rexBuilder.makeInputRef( input, field.getIndex() ) );
                    names.add( PolyString.of( field.getName() ) );
                }
            }
            List<PolyString> aliases = Pair.left( updates );
            AlgNode provider;
            if ( actions.isEmpty() ) {
                provider = new LogicalLpgProject( cluster, cluster.traitSet(), input, parameters, names );
            } else {
                List<AlgDataTypeField> output = new ArrayList<>();
                for ( int i = 0; i < names.size(); i++ ) {
                    output.add( new AlgDataTypeFieldImpl( -1L, names.get( i ).value, i, parameters.get( i ).getType() ) );
                }
                List<Integer> actionSlots = actions.stream().map( action -> aliases.indexOf( action.left ) ).toList();
                provider = new LogicalLpgSetRows( input, new AlgRecordType( output ), Pair.right( actions ), actionSlots, updates.size() );
            }
            if ( !writeHasConsumer ) {
                List<RexNode> targets = refs( provider ).stream()
                        .map( ref -> (RexNode) rexBuilder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), ref ) )
                        .toList();
                provider = new LogicalLpgFilter( cluster, cluster.traitSet(), provider, RexUtil.composeDisjunction( rexBuilder, targets, false ) );
            }
            AlgNode modify = new LogicalLpgModify( cluster, cluster.traitSet(), graph, provider, operation, null, null, updates.size(), detachDelete );
            if ( !writeHasConsumer ) {
                add( LogicalLpgAggregate.countWrites( modify ) );
                clearVariables();
                return;
            }

            List<RexNode> values = new ArrayList<>();
            for ( AlgDataTypeField field : fields ) {
                int updatedIndex = operation == Modify.Operation.UPDATE ? aliases.indexOf( PolyString.of( field.getName() ) ) : -1;
                values.add( rexBuilder.makeInputRef( modify, updatedIndex < 0 ? updates.size() + field.getIndex() : updatedIndex ) );
            }
            add( new LogicalLpgProject( cluster, cluster.traitSet(), modify, values, fields.stream().map( field -> PolyString.of( field.getName() ) ).toList() ) );
            clearVariables();
        }


        public RexNode getLabelUpdate( List<String> labels, String variable, boolean replace ) {
            AlgNode node = peek();
            int index = node.getTupleType().getFieldNames().indexOf( variable );
            if ( index < 0 ) {
                throw new GenericRuntimeException( String.format( "Unknown variable with name %s", variable ) );
            }
            AlgDataTypeField field = node.getTupleType().getFields().get( index );

            PolyType type = field.getType().getPolyType();
            if ( type != PolyType.NODE && !isDynamicBinding( type ) ) {
                throw new GenericRuntimeException( "SET label assignment requires a node binding" );
            }

            RexNode ref = getRexNode( variable );
            if ( ref == null ) {
                ref = rexBuilder.makeInputRef( field.getType(), index );
            }

            return rexBuilder.makeCall(
                    field.getType(),
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_SET_LABELS ),
                    List.of(
                            ref,
                            rexBuilder.makeArray(
                                    typeFactory.createArrayType( typeFactory.createPolyType( PolyType.VARCHAR, 255 ), -1 ),
                                    labels.stream().map( PolyString::of ).collect( Collectors.toList() ) ),
                            rexBuilder.makeLiteral( replace ) ) );
        }


        public void combineSet() {
            if ( rexQueue.isEmpty() ) {
                throw new GenericRuntimeException( "Empty SET is not possible" );
            }

            List<Pair<PolyString, RexNode>> updates = popNodes();

            modify( Modify.Operation.UPDATE, updates );
            kind = Kind.UPDATE;
        }

    }


    public static class EdgeVariableHolder {

        public final PolyEdge edge;
        public final PolyString identifier;
        public final PolyString left;
        public final PolyString right;


        public EdgeVariableHolder( PolyEdge edge, PolyString identifier, PolyString left, PolyString right ) {
            this.edge = edge;
            this.identifier = identifier;
            this.left = left;
            this.right = right;
        }


        public Pair<PolyString, PolyEdge> asNamedEdge() {
            return Pair.of( identifier, edge );
        }

    }

}
