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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
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
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.core.common.ConditionalExecute.Condition;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgJoin;
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
import org.polypheny.db.cypher.CypherNode.CypherKind;
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
import org.polypheny.db.cypher.pattern.CypherNamedPattern;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
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

        AlgNode output = context.build();
        if ( context.returningInsert != null ) {
            // Execute the write to completion even when the RETURN projection produces no rows.
            output = LogicalConditionalExecute.create( context.returningInsert, output, Condition.GREATER_ZERO,
                    GenericRuntimeException.class, "Cypher CREATE did not produce an update result" );
        }
        return AlgRoot.of( output, context.kind != null ? context.kind : Kind.SELECT );
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
                List<CypherKind> clauses = query.getClauses().stream()
                        .map( CypherClause::getCypherKind )
                        .filter( kind -> kind != CypherKind.USE )
                        .toList();
                if ( !clauses.isEmpty() && clauses.get( clauses.size() - 1 ) == CypherKind.RETURN && query.hasUpdates() ) {
                    if ( !clauses.equals( List.of( CypherKind.CREATE, CypherKind.RETURN ) ) ) {
                        throw new GenericRuntimeException( "Cypher write results support a single CREATE clause followed by RETURN" );
                    }
                    context.createReturning = true;
                }

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
        context.active = clause;
        if ( clause.isOptional() || !context.stack.isEmpty() ) {
            convertJoinedMatch( clause, context );
            return;
        }

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }
        context.combineMatch( false );

        if ( clause.getWhere() == null ) {
            return;
        }

        convertWhere( clause.getWhere(), context );

        context.combineFilter();
    }


    private void convertJoinedMatch( CypherMatch clause, CypherContext context ) {
        MatchRows right = null;
        boolean checkRelationshipUniqueness = clause.getPatterns().size() > 1;
        for ( CypherPattern pattern : clause.getPatterns() ) {
            CypherContext candidate = new CypherContext( context.original, context.graph, cluster, algBuilder, rexBuilder, snapshot );
            candidate.active = clause;
            candidate.add( pattern.getPatternMatch( candidate ) );
            // Candidate rows need complete graph values, including unnamed nodes and paths.
            candidate.combineMatch( true );
            AlgNode match = candidate.build();
            RexIndexRef matchedValue = rexBuilder.makeInputRef( match, 0 );
            List<RexNode> paths = checkRelationshipUniqueness && matchedValue.getType().getPolyType() == PolyType.PATH ? List.of( matchedValue ) : List.of();
            MatchRows bindings = projectMatchRows( match, matchBindings( new MatchRows( match, 0 ), 0, pattern instanceof CypherNamedPattern ), paths );
            right = right == null ? bindings : joinBindings( right, bindings, JoinAlgType.INNER, null, context, true );
        }

        AlgNode left = context.pop();
        if ( left == null ) {
            // The first OPTIONAL MATCH has one input row with no variables.
            left = LogicalLpgValues.create( cluster, cluster.traitSet(), new AlgRecordType( List.of() ), ImmutableList.of( ImmutableList.of() ) );
        }
        JoinAlgType joinType = clause.isOptional() ? JoinAlgType.LEFT : JoinAlgType.INNER;
        context.add( joinBindings( new MatchRows( left, 0 ), Objects.requireNonNull( right ), joinType, clause.getWhere(), context, false ).node() );
    }


    private MatchRows joinBindings( MatchRows leftRows, MatchRows rightRows, JoinAlgType type, CypherWhere where, CypherContext context, boolean clausePatterns ) {
        AlgNode left = leftRows.node();
        AlgNode right = rightRows.node();
        int offset = left.getTupleType().getFieldCount();
        Map<PolyString, RexNode> leftBindings = matchBindings( leftRows, 0, true );
        Map<PolyString, RexNode> rightBindings = matchBindings( rightRows, offset, true );
        List<RexNode> conditions = new ArrayList<>();
        Map<PolyString, RexNode> scope = new LinkedHashMap<>( leftBindings );
        rightBindings.forEach( ( name, value ) -> {
            RexNode current = scope.putIfAbsent( name, value );
            if ( current != null ) {
                conditions.add( sameBinding( name, current, value ) );
            }
        } );
        // Patterns in the same clause cannot reuse a relationship, even when that relationship is unnamed.
        List<RexNode> paths = new ArrayList<>();
        if ( clausePatterns ) {
            List<RexNode> leftPaths = matchPaths( leftRows, 0 );
            List<RexNode> rightPaths = matchPaths( rightRows, offset );
            for ( RexNode leftPath : leftPaths ) {
                for ( RexNode rightPath : rightPaths ) {
                    conditions.add( rexBuilder.makeCall( context.booleanType,
                            OperatorRegistry.get( QueryLanguage.from( "cypher" ), OperatorName.CYPHER_PATHS_DISJOINT ),
                            List.of( leftPath, rightPath ) ) );
                }
            }
            paths.addAll( leftPaths );
            paths.addAll( rightPaths );
        }
        if ( where != null ) {
            // Resolve WHERE against the visible bindings, then rewrite its references to the join inputs.
            AlgNode combined = LogicalLpgJoin.create( left, right, rexBuilder.makeLiteral( true ), JoinAlgType.INNER );
            context.add( projectBindings( combined, scope ) );
            RexNode predicate = where.getExpression().getRex( context, RexType.PROJECT ).right;
            List<RexNode> arguments = new ArrayList<>( scope.values() );
            conditions.add( predicate.accept( new RexShuttle() {
                @Override
                public RexNode visitIndexRef( RexIndexRef ref ) {
                    return arguments.get( ref.getIndex() );
                }
            } ) );
            context.pop();
        }

        AlgNode join = LogicalLpgJoin.create( left, right, RexUtil.composeConjunction( rexBuilder, conditions, false ), type );
        Map<PolyString, RexNode> output = new LinkedHashMap<>( leftBindings );
        // Derive new bindings from the join's row type, which makes its optional side nullable.
        for ( AlgDataTypeField field : rightRows.visibleFields() ) {
            if ( field.getName() != null ) {
                int index = offset + field.getIndex();
                output.putIfAbsent( PolyString.of( field.getName() ), rexBuilder.makeInputRef( join, index ) );
            }
        }
        return projectMatchRows( join, output, paths );
    }


    private Map<PolyString, RexNode> matchBindings( MatchRows input, int offset, boolean includePathColumn ) {
        Map<PolyString, RexNode> bindings = new LinkedHashMap<>();
        for ( AlgDataTypeField field : input.visibleFields() ) {
            AlgDataType type = field.getType();
            RexNode ref = rexBuilder.makeInputRef( type, offset + field.getIndex() );
            if ( field.getName() != null && (includePathColumn || type.getPolyType() != PolyType.PATH) ) {
                bindings.put( PolyString.of( field.getName() ), ref );
            }
            if ( type.getPolyType() == PolyType.PATH ) {
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


    private List<RexNode> matchPaths( MatchRows rows, int offset ) {
        return rows.pathFields().stream()
                .map( field -> (RexNode) rexBuilder.makeInputRef( field.getType(), offset + field.getIndex() ) )
                .toList();
    }


    private AlgDataType contextPathFieldType( AlgDataType path, AlgDataTypeField field ) {
        return cluster.getTypeFactory().createTypeWithNullability( field.getType(), path.isNullable() || field.getType().isNullable() );
    }


    private RexNode sameBinding( PolyString name, RexNode left, RexNode right ) {
        PolyType type = left.getType().getPolyType();
        if ( type != right.getType().getPolyType() || (type != PolyType.NODE && type != PolyType.EDGE) ) {
            throw new GenericRuntimeException( "Variable " + name + " must refer to the same type of node or relationship in each pattern." );
        }
        return rexBuilder.makeCall( OperatorRegistry.get( OperatorName.EQUALS ), elementId( left ), elementId( right ) );
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
                name += "$";
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
        public CypherNode active;
        public Kind kind;
        private boolean createReturning;
        private LogicalLpgModify returningInsert;
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
            this.geometryType = cluster.getTypeFactory().createPolyType( PolyType.GEOMETRY );
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
                    typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), true ),
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
                            typeFactory.createTypeWithNullability( booleanType, left.getType().isNullable() || right.getType().isNullable() ),
                            OperatorRegistry.get( op ),
                            List.of( left, right ) );
            }
        }


        private RexNode getLikeOperator( RexNode left, RexNode right, Function<String, String> adjustingFunction ) {
            assert right.isA( Kind.LITERAL );
            String adjustedRight = adjustingFunction.apply( ((RexLiteral) right).value.asString().value );
            return rexBuilder.makeCall(
                    typeFactory.createTypeWithNullability( booleanType, left.getType().isNullable() ),
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
            } else if ( nameAndValues.stream().allMatch( v -> v.right.isA( Kind.CYPHER_FUNCTION ) ) ) {
                throw new NotImplementedException( "RETURN statement with function call without MATCH currently not supported" );
            } else {
                throw new UnsupportedOperationException();
            }
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
