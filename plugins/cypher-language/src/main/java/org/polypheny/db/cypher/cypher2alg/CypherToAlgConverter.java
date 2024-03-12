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

package org.polypheny.db.cypher.cypher2alg;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
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
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.CypherNode.CypherFamily;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherDelete;
import org.polypheny.db.cypher.clause.CypherMatch;
import org.polypheny.db.cypher.clause.CypherRemove;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherSetClause;
import org.polypheny.db.cypher.clause.CypherUnwind;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.clause.CypherWith;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
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

        LogicalEntity entity = getEntity( namespaceId, query );

        if ( query.isFullScan() ) {
            // simple full graph relScan
            return AlgRoot.of( buildFullScan( (LogicalGraph) entity ), Kind.SELECT );
        }

        if ( !CypherFamily.QUERY.contains( query.getCypherKind() ) ) {
            throw new GenericRuntimeException( "Used a unsupported query." );
        }

        CypherContext context = new CypherContext( query, entity, cluster, algBuilder, rexBuilder, snapshot );

        convertQuery( query, context );

        return AlgRoot.of( context.build(), context.kind != null ? context.kind : Kind.SELECT );
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


    private void convertQuery( CypherNode node, CypherContext context ) {
        switch ( node.getCypherKind() ) {
            case SINGLE:
                CypherSingleQuery query = (CypherSingleQuery) node;

                for ( CypherClause clause : query.getClauses() ) {
                    convertClauses( clause, context );
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
                convertReturn( (CypherReturnClause) clause, context );
                break;
            case CREATE:
                convertCreate( (CypherCreate) clause, context );
                break;
            case USE:
                break;
            case WITH:
                convertWith( (CypherWith) clause, context );
                break;
            case UNWIND:
                convertUnwind( (CypherUnwind) clause, context );
                break;
            case SET:
                convertSet( (CypherSetClause) clause, context );
                break;
            case DELETE:
                convertDelete( (CypherDelete) clause, context );
                break;
            case REMOVE:
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
        clause.getUnwind( context );
    }


    private void convertWith( CypherWith clause, CypherContext context ) {
        convertReturn( clause.getReturnClause(), context );

        if ( clause.getWhere() != null ) {
            convertWhere( clause.getWhere(), context );
            // the combining could potentially be moved into the convertWhere
            context.combineFilter();
        }
    }


    private void convertReturn( CypherReturnClause clause, CypherContext context ) {
        AlgNode node = clause.getGraphProject( context );

        if ( clause.getOrder() != null && !clause.getOrder().isEmpty() || clause.getLimit() != null || clause.getSkip() != null ) {
            AlgDataType rowType = node.getTupleType();
            List<String> missingNames = clause
                    .getSortFields()
                    .stream().filter( n -> !rowType.getFieldNames().contains( n ) )
                    .toList();

            if ( !missingNames.isEmpty() ) {
                // hidden names are used for sort but are not yet present
                node = addHiddenRows( context, node, missingNames );
            }

            context.add( node );
            node = clause.getGraphSort( context );

            if ( !missingNames.isEmpty() ) {
                // we have to remove the names again
                node = removeHiddenRows( context, node, missingNames );
            }

        }
        context.add( node );
    }


    private AlgNode removeHiddenRows( CypherContext context, AlgNode node, List<String> missingNames ) {
        List<RexNode> nodes = new ArrayList<>();
        List<PolyString> names = new ArrayList<>();
        node.getTupleType().getFields().forEach( f -> {
            if ( !missingNames.contains( f.getName() ) ) {
                nodes.add( context.rexBuilder.makeInputRef( f.getType(), f.getIndex() ) );
                names.add( PolyString.of( f.getName() ) );
            }
        } );
        return new LogicalLpgProject( node.getCluster(), cluster.traitSet(), node, nodes, names );
    }


    private AlgNode addHiddenRows( CypherContext context, AlgNode node, List<String> missingNames ) {

        AlgNode input;
        if ( node instanceof LogicalLpgProject ) {
            input = node.getInput( 0 );
        } else {
            input = node;
        }

        List<Pair<PolyString, RexNode>> additional = new ArrayList<>();
        for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
            for ( String name : missingNames ) {
                String[] split = name.split( "\\." );
                if ( split.length > 1 && split[0].equals( field.getName() ) ) {
                    // missing property
                    additional.add( context.getPropertyExtract( split[1], split[0], context.rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ) );
                } else if ( name.equals( field.getName() ) ) {
                    // missing field ( edge, node, string, etc )
                    additional.add( Pair.of( PolyString.of( name ), context.rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ) );
                }
            }
        }
        List<RexNode> nodes;
        List<PolyString> names;
        if ( node instanceof LogicalLpgProject ) {
            // we can extend the Project
            LogicalLpgProject project = (LogicalLpgProject) node;

            nodes = new ArrayList<>( project.getProjects() );
            names = new ArrayList<>( project.getNames() );

            nodes.addAll( Pair.right( additional ) );
            names.addAll( Pair.left( additional ) );

            node = new LogicalLpgProject( node.getCluster(), node.getTraitSet(), input, nodes, names );
        } else {
            // we can add a project
            nodes = new ArrayList<>();
            names = new ArrayList<>();

            node.getTupleType().getFields().forEach( f -> {
                nodes.add( context.rexBuilder.makeInputRef( f.getType(), f.getIndex() ) );
                names.add( PolyString.of( f.getName() ) );
            } );

            nodes.addAll( Pair.right( additional ) );
            names.addAll( Pair.left( additional ) );

            node = new LogicalLpgProject( node.getCluster(), node.getTraitSet(), input, nodes, names );
        }
        return node;
    }


    private void convertCreate( CypherCreate clause, CypherContext context ) {
        context.kind = Kind.INSERT;

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }
        context.combineValues();
    }


    private void convertMatch( CypherMatch clause, CypherContext context ) {
        setKindIfNull( context, Kind.SELECT );
        if ( clause.isOptional() ) {
            throw new UnsupportedOperationException();
        }

        context.active = clause;

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }
        context.combineMatch();

        if ( clause.getWhere() == null ) {
            return;
        }

        convertWhere( clause.getWhere(), context );

        context.combineFilter();
    }


    private void convertWhere( CypherWhere where, CypherContext context ) {
        context.add( where.getExpression().getRex( context, RexType.FILTER ) );
    }


    private void convertPattern( CypherPattern pattern, CypherContext context ) {
        if ( context.kind == Kind.INSERT ) {
            // convert "values" pattern (LogicalGraphValues AlgNode)
            pattern.getPatternValues( context );
        } else {
            // convert filter pattern ( RexNode CYPHER_PATTERN_MATCH )
            context.add( pattern.getPatternMatch( context ) );
        }
    }


    private void setKindIfNull( CypherContext context, Kind kind ) {
        if ( context.kind == null ) {
            context.kind = kind;
        }
    }


    public static class CypherContext {

        public final AlgCluster cluster;
        public final AlgBuilder algBuilder;
        public final RexBuilder rexBuilder;

        private final Stack<AlgNode> stack = new Stack<>();
        // named projects, null if no name provided
        private final Queue<Pair<PolyString, RexNode>> rexQueue = new LinkedList<>();
        private final Queue<Pair<String, AggregateCall>> rexAggQueue = new LinkedList<>();
        public final CypherNode original;
        public final LogicalEntity graph;

        public final AlgDataType graphType;
        public final AlgDataType booleanType;
        public final AlgDataType nodeType;
        public final AlgDataType edgeType;
        public final AlgDataType pathType;
        public final AlgDataType numberType;
        public final Snapshot snapshot;
        public final AlgDataTypeFactory typeFactory;
        public CypherNode active;
        public Kind kind;
        private final Map<PolyString, PolyNode> nodes = new HashMap<>();
        private List<Pair<PolyString, EdgeVariableHolder>> edges = new LinkedList<>();


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
            this.nodeType = cluster.getTypeFactory().createPolyType( PolyType.NODE );
            this.edgeType = cluster.getTypeFactory().createPolyType( PolyType.EDGE );
            this.pathType = cluster.getTypeFactory().createPolyType( PolyType.PATH );
            this.numberType = cluster.getTypeFactory().createPolyType( PolyType.INTEGER );
            this.typeFactory = cluster.getTypeFactory();
            this.snapshot = snapshot;
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


        public void combineMatch() {
            addDefaultScanIfNecessary();
            List<Pair<PolyString, RexNode>> matches = getMatches();

            List<RexCall> calls = Pair.right( matches ).stream().map( r -> (RexCall) r ).toList();

            stack.add( new LogicalLpgMatch( cluster, cluster.traitSet(), stack.pop(), calls, Pair.left( matches ) ) );
            clearVariables();
        }


        private void clearVariables() {
            nodes.clear();
            edges.clear();
        }


        private List<Pair<PolyString, RexNode>> getMatches() {
            ArrayList<Pair<PolyString, RexNode>> namedMatch = new ArrayList<>();

            while ( !rexQueue.isEmpty() ) {
                namedMatch.add( rexQueue.remove() );
            }
            return namedMatch;
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
                    typeFactory.createPolyType( PolyType.ANY ),
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_EXTRACT_PROPERTY ),
                    List.of( field, rexBuilder.makeLiteral( key ) ) );

            return Pair.of( PolyString.of( subject + "." + key ), extractedProperty );
        }


        public RexNode getBinaryOperation( OperatorName op, RexNode left, RexNode right ) {
            switch ( op ) {
                case STARTS_WITH:
                    return getLikeOperator( left, right, ( i ) -> i + "%" );
                case ENDS_WITH:
                    return getLikeOperator( left, right, ( i ) -> "%" + i );
                case CONTAINS:
                    return getLikeOperator( left, right, ( i ) -> "%" + i + "%" );
                default:
                    return rexBuilder.makeCall(
                            booleanType,
                            OperatorRegistry.get( op ),
                            List.of( left, right ) );
            }
        }


        private RexNode getLikeOperator( RexNode left, RexNode right, Function<String, String> adjustingFunction ) {
            assert right.isA( Kind.LITERAL );
            String adjustedRight = adjustingFunction.apply( ((RexLiteral) right).value.asString().value );
            return rexBuilder.makeCall(
                    booleanType,
                    OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_LIKE ),
                    List.of( left, rexBuilder.makeLiteral( adjustedRight ) ) );
        }


        public boolean containsAggs() {
            return !rexAggQueue.isEmpty();
        }


        public AlgNode asValues( List<Pair<PolyString, RexNode>> nameAndValues ) {
            if ( nameAndValues.stream().allMatch( v -> v.right.isA( Kind.LITERAL ) ) ) {
                List<AlgDataTypeField> fields = new ArrayList<>();
                int i = 0;
                for ( Pair<PolyString, RexNode> nameAndValue : nameAndValues ) {
                    fields.add( new AlgDataTypeFieldImpl( -1L, nameAndValue.left.value, i, nameAndValue.right.getType() ) );
                    i++;
                }

                return new LogicalLpgValues(
                        cluster,
                        cluster.traitSet(),
                        List.of(),
                        List.of(),
                        ImmutableList.of( ImmutableList.copyOf( nameAndValues.stream().map( e -> (RexLiteral) e.getValue() ).toList() ) ),
                        new AlgRecordType( fields ) );
            } else {
                throw new UnsupportedOperationException();
            }
        }


        public void addNodes( List<Pair<PolyString, PolyNode>> nodes ) {
            for ( Pair<PolyString, PolyNode> node : nodes ) {
                if ( !this.nodes.containsKey( node.left ) ) {
                    this.nodes.put( node.left, node.right );
                } else if ( node.left == null ) {
                    this.nodes.put( node.right.id, node.right );
                }
                // we don't add the variable again
            }
        }


        public void addEdges( List<Pair<PolyString, EdgeVariableHolder>> edges ) {
            this.edges.addAll( edges );
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

            if ( stack.isEmpty() ) {
                // simple unfiltered insert
                add( LogicalLpgValues.create(
                        cluster,
                        cluster.traitSet(),
                        nodes,
                        nodeType,
                        edges.stream().map( t -> Pair.of( t.left, t.right.edge ) ).toList(),
                        edgeType ) );

                add( new LogicalLpgModify( cluster, cluster.traitSet(), graph, pop(), Modify.Operation.INSERT, null, null ) );
            } else {
                // filtered DML
                List<Pair<PolyString, RexNode>> newNodes = new LinkedList<>();

                List<Pair<PolyString, RexNode>> newEdges = new LinkedList<>();

                AlgNode node = stack.peek();
                List<String> names = node.getTupleType().getFieldNames();
                List<AlgDataTypeField> fields = node.getTupleType().getFields();

                // nodes can be added as literals
                for ( Pair<PolyString, PolyNode> namedNode : nodes ) {
                    if ( !names.contains( namedNode.left.value ) ) {
                        newNodes.add( Pair.of( namedNode.left, rexBuilder.makeLiteral( namedNode.right, nodeType, false ) ) );
                    }
                }

                // edges need to be adjusted depending on the previous stage
                for ( Pair<PolyString, EdgeVariableHolder> namedEdge : edges ) {
                    RexNode ref = rexBuilder.makeLiteral( namedEdge.right.edge, edgeType, false );
                    int leftIndex = names.indexOf( namedEdge.right.left.value );
                    int rightIndex = names.indexOf( namedEdge.right.right.value );

                    if ( leftIndex != -1 && rightIndex != -1 ) {
                        // both sides are retrieved from the previous stage (inputRef)-[]-(inputRef)
                        RexNode left = rexBuilder.makeInputRef( fields.get( leftIndex ).getType(), leftIndex );
                        RexNode right = rexBuilder.makeInputRef( fields.get( rightIndex ).getType(), rightIndex );
                        newEdges.add( Pair.of( namedEdge.left, rexBuilder.makeCall( edgeType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_ADJUST_EDGE ), List.of( ref, left, right ) ) ) );
                    } else if ( leftIndex == -1 && rightIndex == -1 ) {
                        // both sides are part of this stage (literal)
                        RexNode stubL = rexBuilder.makeLiteral( new PolyNode( new PolyDictionary(), PolyList.of(), namedEdge.right.left ).isVariable( true ), nodeType, false );
                        RexNode stubR = rexBuilder.makeLiteral( new PolyNode( new PolyDictionary(), PolyList.of(), namedEdge.right.right ).isVariable( true ), nodeType, false );
                        newEdges.add( Pair.of( namedEdge.left, rexBuilder.makeCall( edgeType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_ADJUST_EDGE ), List.of( ref, stubL, stubR ) ) ) );
                    } else if ( leftIndex != -1 ) {
                        // left is from previous stage, right is not (inputRef)-[]-()
                        RexNode left = rexBuilder.makeInputRef( fields.get( leftIndex ).getType(), leftIndex );
                        RexNode stub = rexBuilder.makeLiteral( new PolyNode( new PolyDictionary(), PolyList.of(), namedEdge.right.right ).isVariable( true ), nodeType, false );
                        newEdges.add( Pair.of( namedEdge.left, rexBuilder.makeCall( edgeType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_ADJUST_EDGE ), List.of( ref, left, stub ) ) ) );
                    } else {
                        // right is from previous stage, left is not ()-[]-(inputRef)
                        RexNode right = rexBuilder.makeInputRef( fields.get( rightIndex ).getType(), rightIndex );
                        RexNode stub = rexBuilder.makeLiteral( new PolyNode( new PolyDictionary(), PolyList.of(), namedEdge.right.left ).isVariable( true ), nodeType, false );
                        newEdges.add( Pair.of( namedEdge.left, rexBuilder.makeCall( edgeType, OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_ADJUST_EDGE ), List.of( ref, stub, right ) ) ) );
                    }
                }

                List<Pair<PolyString, RexNode>> nodesAndEdges = Stream.concat( newNodes.stream(), newEdges.stream() ).collect( Collectors.toList() );

                AtomicLong id = new AtomicLong();
                List<PolyString> adjustedNames = Pair.left( nodesAndEdges ).stream()
                        .map( n -> Objects.requireNonNullElseGet( n.value, () -> "EXPR$" + id.getAndIncrement() ) )
                        .map( Object::toString )
                        .map( PolyString::of )
                        .collect( Collectors.toList() );

                add( new LogicalLpgProject( node.getCluster(), node.getTraitSet(), pop(), Pair.right( nodesAndEdges ), adjustedNames ) );

                add( new LogicalLpgModify( cluster, cluster.traitSet(), graph, pop(), Modify.Operation.INSERT, null, null ) );
            }
            clearVariables();
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
                    .collect( Collectors.toList() );
            this.nodes.clear();
            return nodes;
        }


        public void combineUpdate() {
            if ( rexQueue.isEmpty() ) {
                throw new GenericRuntimeException( "Empty UPDATE is not possible" );
            }

            List<Pair<PolyString, RexNode>> updates = popNodes();

            add( new LogicalLpgModify( cluster, cluster.traitSet(), graph, pop(), Modify.Operation.UPDATE, Pair.left( updates ), Pair.right( updates ) ) );
            clearVariables();
        }


        public void combineDelete() {
            if ( rexQueue.isEmpty() ) {
                throw new GenericRuntimeException( "Empty DELETE is not possible" );
            }

            List<Pair<PolyString, RexNode>> deletes = popNodes();

            add( new LogicalLpgModify( cluster, cluster.traitSet(), graph, pop(), Modify.Operation.DELETE, Pair.left( deletes ), Pair.right( deletes ) ) );
            clearVariables();
        }


        public RexNode getLabelUpdate( List<String> labels, String variable, boolean replace ) {
            AlgNode node = peek();
            int index = node.getTupleType().getFieldNames().indexOf( variable );
            if ( index < 0 ) {
                throw new GenericRuntimeException( String.format( "Unknown variable with name %s", variable ) );
            }
            AlgDataTypeField field = node.getTupleType().getFields().get( index );

            if ( field.getType().getPolyType() == PolyType.EDGE && labels.size() != 1 ) {
                throw new GenericRuntimeException( "Edges require exactly one label" );
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
                throw new GenericRuntimeException( "Empty DELETE is not possible" );
            }

            List<Pair<PolyString, RexNode>> updates = popNodes();

            add( new LogicalLpgModify( cluster, cluster.traitSet(), graph, pop(), Modify.Operation.UPDATE, Pair.left( updates ), Pair.right( updates ) ) );
            clearVariables();
        }

    }


    public enum RexType {
        PROJECT,
        FILTER
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
