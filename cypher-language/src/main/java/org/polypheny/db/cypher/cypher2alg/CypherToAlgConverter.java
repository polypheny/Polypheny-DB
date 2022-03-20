/*
 * Copyright 2019-2022 The Polypheny Project
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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Stack;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.logical.graph.LogicalGraph;
import org.polypheny.db.algebra.logical.graph.LogicalGraphFilter;
import org.polypheny.db.algebra.logical.graph.LogicalGraphMatch;
import org.polypheny.db.algebra.logical.graph.LogicalGraphModify;
import org.polypheny.db.algebra.logical.graph.LogicalGraphProject;
import org.polypheny.db.algebra.logical.graph.LogicalGraphScan;
import org.polypheny.db.algebra.logical.graph.LogicalGraphValues;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.catalog.exceptions.UnknownNamespaceException;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.CypherNode.CypherFamily;
import org.polypheny.db.cypher.clause.CypherClause;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.clause.CypherMatch;
import org.polypheny.db.cypher.clause.CypherReturnClause;
import org.polypheny.db.cypher.clause.CypherWhere;
import org.polypheny.db.cypher.pattern.CypherPattern;
import org.polypheny.db.cypher.query.CypherSingleQuery;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class CypherToAlgConverter {

    private final PolyphenyDbCatalogReader catalogReader;
    private final AlgBuilder algBuilder;
    private final Statement statement;
    private final RexBuilder rexBuilder;
    private final AlgOptCluster cluster;


    public CypherToAlgConverter( Statement statement, AlgBuilder builder, RexBuilder rexBuilder, AlgOptCluster cluster ) {
        this.catalogReader = statement.getTransaction().getCatalogReader();
        this.statement = statement;
        this.algBuilder = builder;
        this.rexBuilder = rexBuilder;
        this.cluster = cluster;
    }


    public AlgRoot convert( CypherNode query, CypherQueryParameters parameters, AlgOptCluster cluster ) {
        long databaseId;
        if ( parameters.databaseId == null ) {
            databaseId = getDatabaseId( parameters );
        } else {
            databaseId = parameters.databaseId;
        }

        LogicalGraph graph = new LogicalGraph( databaseId );

        if ( !CypherFamily.QUERY.contains( query.getCypherKind() ) ) {
            throw new RuntimeException( "Used a unsupported query." );
        }

        CypherContext context = new CypherContext( query, graph, cluster, algBuilder, rexBuilder, catalogReader );

        convertQuery( query, context );

        return AlgRoot.of( context.build(), context.kind );
    }


    private long getDatabaseId( CypherQueryParameters parameters ) {
        long databaseId;
        try {
            databaseId = Catalog.getInstance().getNamespace( Catalog.defaultDatabaseId, parameters.databaseName ).id;
        } catch ( UnknownNamespaceException e ) {
            throw new RuntimeException( "Error on retrieving the used namespace" );
        }
        return databaseId;
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
            default:
                throw new UnsupportedOperationException();
        }

    }


    private void convertReturn( CypherReturnClause clause, CypherContext context ) {
        AlgNode node = clause.getGraphProject( context );

        if ( clause.getOrder() != null && !clause.getOrder().isEmpty() || clause.getLimit() != null || clause.getSkip() != null ) {
            AlgDataType rowType = node.getRowType();
            List<String> missingNames = clause
                    .getSortFields()
                    .stream().filter( n -> !rowType.getFieldNames().contains( n ) )
                    .collect( Collectors.toList() );

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
        List<String> names = new ArrayList<>();
        node.getRowType().getFieldList().forEach( f -> {
            if ( !missingNames.contains( f.getName() ) ) {
                nodes.add( context.rexBuilder.makeInputRef( f.getType(), f.getIndex() ) );
                names.add( f.getName() );
            }
        } );
        return new LogicalGraphProject( node.getCluster(), cluster.traitSet(), node, nodes, names );
    }


    private AlgNode addHiddenRows( CypherContext context, AlgNode node, List<String> missingNames ) {

        AlgNode input;
        if ( node instanceof LogicalGraphProject ) {
            input = node.getInput( 0 );
        } else {
            input = node;
        }

        List<Pair<String, RexNode>> additional = new ArrayList<>();
        for ( AlgDataTypeField field : input.getRowType().getFieldList() ) {
            for ( String name : missingNames ) {
                String[] split = name.split( "\\." );
                if ( split.length > 1 && split[0].equals( field.getName() ) ) {
                    // missing property
                    additional.add( context.getPropertyExtract( split[1], split[0], context.rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ) );
                } else if ( name.equals( field.getName() ) ) {
                    // missing field ( edge, node, string, etc )
                    additional.add( Pair.of( name, context.rexBuilder.makeInputRef( field.getType(), field.getIndex() ) ) );
                }
            }
        }
        List<RexNode> nodes;
        List<String> names;
        if ( node instanceof LogicalGraphProject ) {
            // we can extend the Project
            LogicalGraphProject project = (LogicalGraphProject) node;

            nodes = new ArrayList<>( project.getProjects() );
            names = new ArrayList<>( project.getNames() );

            nodes.addAll( Pair.right( additional ) );
            names.addAll( Pair.left( additional ) );

            node = new LogicalGraphProject( node.getCluster(), node.getTraitSet(), input, nodes, names );
        } else {
            // we can add a project
            nodes = new ArrayList<>();
            names = new ArrayList<>();

            node.getRowType().getFieldList().forEach( f -> {
                nodes.add( context.rexBuilder.makeInputRef( f.getType(), f.getIndex() ) );
                names.add( f.getName() );
            } );

            nodes.addAll( Pair.right( additional ) );
            names.addAll( Pair.left( additional ) );

            node = new LogicalGraphProject( node.getCluster(), node.getTraitSet(), input, nodes, names );
        }
        return node;
    }


    private void convertCreate( CypherCreate clause, CypherContext context ) {
        context.kind = Kind.INSERT;

        for ( CypherPattern pattern : clause.getPatterns() ) {
            convertPattern( pattern, context );
        }

        AlgNode node = context.pop();
        if ( !context.stack.isEmpty() ) {
            // multiple patternValues, which need to be merged into one "graph"
            List<AlgNode> nodes = new ArrayList<>();
            nodes.add( node );
            while ( !context.stack.isEmpty() ) {
                nodes.add( context.pop() );
            }
            assert nodes.stream().allMatch( n -> n instanceof LogicalGraphValues );
            node = LogicalGraphValues.merge( nodes.stream().map( n -> (LogicalGraphValues) n ).collect( Collectors.toList() ) );
        }

        context.add( new LogicalGraphModify( cluster, cluster.traitSet(), context.graph, catalogReader, node, Operation.INSERT, null, null ) );
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
        context.add( where.getExpression().getRexNode( context ) );
    }


    private void convertPattern( CypherPattern pattern, CypherContext context ) {
        if ( context.kind == Kind.INSERT ) {
            // convert "values" pattern (LogicalGraphValues AlgNode)
            context.add( pattern.getPatternValues( context ) );
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

        public final AlgOptCluster cluster;
        public final AlgBuilder algBuilder;
        public final RexBuilder rexBuilder;

        private final Stack<AlgNode> stack = new Stack<>();
        // named projects, null if no name provided
        private final Queue<Pair<String, RexNode>> rexQueue = new LinkedList<>();
        public final CypherNode original;
        public final LogicalGraph graph;

        public final AlgDataType graphType;
        public final AlgDataType booleanType;
        public final AlgDataType nodeType;
        public final AlgDataType edgeType;
        public final AlgDataType pathType;
        public final AlgDataType numberType;
        public final PolyphenyDbCatalogReader catalogReader;
        public final AlgDataTypeFactory typeFactory;
        public CypherNode active;
        public Kind kind;


        private CypherContext( CypherNode original, LogicalGraph graph, AlgOptCluster cluster, AlgBuilder algBuilder, RexBuilder rexBuilder, PolyphenyDbCatalogReader catalogReader ) {
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
            this.catalogReader = catalogReader;
        }


        public void addDefaultScanIfNecessary() {
            if ( !stack.isEmpty() ) {
                return;
            }
            stack.add( new LogicalGraphScan( cluster, catalogReader, cluster.traitSet(), graph, new AlgRecordType(
                    List.of( new AlgDataTypeFieldImpl( "g", 0, graphType ) ) ) ) );

        }


        public void combineMatch() {
            addDefaultScanIfNecessary();
            List<Pair<String, RexNode>> matches = getMatches();

            stack.add( new LogicalGraphMatch( cluster, cluster.traitSet(), stack.pop(), Pair.right( matches ), Pair.left( matches ) ) );
        }


        private List<Pair<String, RexNode>> getMatches() {
            ArrayList<Pair<String, RexNode>> namedMatch = new ArrayList<>();

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

            stack.add( new LogicalGraphFilter( cluster, cluster.traitSet(), stack.pop(), condition ) );
        }


        private void addProjectIfNecessary() {
            if ( stack.size() >= 1 && rexQueue.size() > 0 ) {
                AlgNode node = stack.peek();
                if ( node.getRowType().getFieldList().size() == 1
                        && node.getRowType().getFieldList().get( 0 ).getType().getPolyType() == PolyType.GRAPH ) {
                    node = stack.pop();

                    List<Pair<String, RexNode>> rex = new ArrayList<>();
                    rexQueue.iterator().forEachRemaining( rex::add );
                    stack.add( new LogicalGraphProject( node.getCluster(), node.getTraitSet(), node, Pair.right( rex ), Pair.left( rex ) ) );
                }
            }
        }


        private RexNode getCondition() {
            List<RexNode> nodes = new ArrayList<>();
            while ( !rexQueue.isEmpty() ) {
                Pair<String, RexNode> popped = rexQueue.remove();
                nodes.add( popped.right );
            }

            return nodes.size() == 1 ? nodes.get( 0 ) : algBuilder.and( nodes );
        }


        public void add( AlgNode node ) {
            this.stack.add( node );
        }


        public void add( String name, RexNode node ) {
            this.rexQueue.add( Pair.of( name, node ) );
        }


        public void add( Pair<String, RexNode> namedNode ) {
            this.rexQueue.add( namedNode );
        }


        public AlgNode build() {
            assert stack.size() == 1;
            return stack.pop();
        }


        public AlgNode peek() {
            return stack.peek();
        }


        public AlgNode pop() {
            return stack.pop();
        }


        public void addScanIfNecessary( List<Pair<String, RexNode>> nameAndProject ) {
            if ( !stack.isEmpty() ) {
                return;
            }
            if ( nameAndProject.size() == 1 ) {
                assert nameAndProject.get( 0 ).right.getType().getPolyType() == PolyType.NODE;
                stack.add( new LogicalGraphScan( cluster, catalogReader, cluster.traitSet(), graph, new AlgRecordType(
                        List.of(
                                new AlgDataTypeFieldImpl( nameAndProject.get( 0 ).left, 0, nodeType ) ) ) ) );
                return;
            } else if ( nameAndProject.size() == 2 ) {
                assert nameAndProject.get( 0 ).right.getType().getPolyType() == PolyType.NODE;
                assert nameAndProject.get( 1 ).right.getType().getPolyType() == PolyType.EDGE;

                addDefaultScanIfNecessary();
                return;
            }
            throw new UnsupportedOperationException();
        }


        public RexInputRef createRefToVar( String name, PolyType fieldType ) {
            AlgNode node = stack.peek();
            List<AlgDataTypeField> candidates = node
                    .getRowType()
                    .getFieldList()
                    .stream()
                    .filter( f -> f.getType().getPolyType() == fieldType
                            && f.getName().equals( name ) ).collect( Collectors.toList() );
            if ( candidates.isEmpty()
                    && node.getRowType().getFieldList().size() == 1
                    && node.getRowType().getFieldList().get( 0 ).getType().getPolyType() == PolyType.GRAPH ) {
                // we have not yet done a projection,
                // but there is still the possibility that we match against a graph
                return rexBuilder.makeInputRef( graphType, 0 );

            } else if ( candidates.size() == 1 ) {
                return rexBuilder.makeInputRef( candidates.get( 0 ).getType(), candidates.get( 0 ).getIndex() );
            } else {
                throw new RuntimeException( "There seems to be a problem with the provided algebra node." );
            }

        }


        public void insertMissingProjects( List<String> missingFields ) {
            AlgNode node = stack.pop();

            MissingFieldInserter inserter = new MissingFieldInserter( missingFields, this );
            node = node.accept( inserter );
            stack.add( node );
        }


        public Pair<String, RexNode> getPropertyExtract( String key, String subject, RexNode field ) {
            RexNode extractedProperty = rexBuilder.makeCall(
                    typeFactory.createPolyType( PolyType.VARCHAR, 255 ),
                    OperatorRegistry.get( QueryLanguage.CYPHER, OperatorName.CYPHER_EXTRACT_PROPERTY ),
                    List.of( field, rexBuilder.makeLiteral( key ) ) );

            return Pair.of( subject + "." + key, extractedProperty );
        }


        public static class MissingFieldInserter extends AlgShuttleImpl {

            private final List<String> names;

            private final List<String> adjustedNames = new ArrayList<>();
            private final CypherContext context;


            public MissingFieldInserter( List<String> missingFields, CypherContext context ) {
                this.names = missingFields;
                this.context = context;
            }


            @Override
            public AlgNode visit( LogicalGraphProject project ) {
                AlgNode node = insertProjectIfNecessary( project );

                return adjustFieldsIfNecessary( node );
            }


            private AlgNode adjustFieldsIfNecessary( AlgNode node ) {
                if ( adjustedNames.isEmpty() ) {
                    return node;
                }
                if ( node instanceof LogicalGraphProject ) {
                    List<Pair<String, Integer>> namedRef = adjustedNames.stream().map( n -> Pair.of( n, node.getRowType().getFieldNames().indexOf( n ) ) ).collect( Collectors.toList() );

                    List<RexNode> nodes = new ArrayList<>( ((LogicalGraphProject) node).getProjects() );
                    List<String> names = new ArrayList<>( ((LogicalGraphProject) node).getNames() );

                    namedRef.forEach( n -> {
                        names.add( n.left );
                        nodes.add( context.rexBuilder.makeInputRef( node.getRowType().getFieldList().get( n.right ).getType(), n.right ) );
                    } );

                    return new LogicalGraphProject( node.getCluster(), node.getTraitSet(), node.getInput( 0 ), nodes, names );
                }
                return node;
            }


            private AlgNode insertProjectIfNecessary( AlgNode node ) {
                if ( adjustedNames.size() == names.size() ) {
                    return node;
                }

                List<Pair<String, RexNode>> additionalProjects = new ArrayList<>();
                int i = 0;
                for ( AlgDataTypeField field : node.getRowType().getFieldList() ) {
                    boolean success = false;
                    for ( String name : names.stream().filter( adjustedNames::contains ).collect( Collectors.toList() ) ) {
                        if ( success ) {
                            continue;
                        }
                        if ( name.contains( "." ) ) {
                            // missing property name
                            if ( field.getName().equals( name.split( "\\." )[0] ) ) {
                                Pair<String, RexNode> propertyExtract = context.getPropertyExtract(
                                        name.split( "\\." )[1],
                                        name.split( "\\." )[0],
                                        context.rexBuilder.makeInputRef( field.getType(), i ) );
                                adjustedNames.add( propertyExtract.left );
                                success = true;
                                additionalProjects.add( propertyExtract );
                            }
                        } else {
                            // missing node or edge
                            if ( field.getName().equals( name ) ) {
                                adjustedNames.add( name );
                                success = true;
                            }
                        }
                    }

                    i++;
                }

                // we don't need to adjust the field explicitly, only signal to the parent to look for it
                if ( !additionalProjects.isEmpty() ) {
                    // need to explicitly extract the underlying field
                    List<Pair<String, RexNode>> namedRefs = node.getRowType().getFieldList().stream().map( f -> Pair.of( f.getName(), (RexNode) context.rexBuilder.makeInputRef( f.getType(), f.getIndex() ) ) ).collect( Collectors.toCollection( ArrayList::new ) );
                    namedRefs.addAll( additionalProjects );
                    return new LogicalGraphProject( node.getCluster(), node.getTraitSet(), node, Pair.right( namedRefs ), Pair.left( namedRefs ) );
                }
                return node;

            }

        }

    }

}
