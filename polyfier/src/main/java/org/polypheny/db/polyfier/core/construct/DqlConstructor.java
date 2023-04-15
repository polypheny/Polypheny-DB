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

package org.polypheny.db.polyfier.core.construct;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.RandomWalkVertexIterator;
import org.polypheny.db.polyfier.core.construct.model.Decision;
import org.polypheny.db.polyfier.core.construct.graphs.DecisionGraph;
import org.polypheny.db.polyfier.core.construct.nodes.*;
import org.polypheny.db.polyfier.core.construct.model.*;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class DqlConstructor {
    private final Transaction transaction;
    private final int complexity;
    private ConstructionGraph constructionGraph;
    private final HashMap<String, ColumnStatistic> columnStatistics;

    private Decision<?> lastDecisionStump = null;

    public DqlConstructor(Transaction transaction, int complexity, HashMap<String, ColumnStatistic> columnStatistics ) {
        this.transaction = transaction;
        this.complexity = complexity;
        this.columnStatistics = columnStatistics;
    }

    public Optional<Triple<Statement, AlgNode, Long>> construct( long seed, List<CatalogTable> tables ) {
        Statement statement = transaction.createStatement();
        this.constructionGraph = new ConstructionGraph( statement );
        if ( log.isDebugEnabled() ) {
            log.debug("Constructing Tree with seed " + seed );
        }
        try {
            constructDql( seed, tables );
        } catch ( NoSuchElementException noSuchElementException ) {
            if ( log.isDebugEnabled() ) {
                log.debug("The generation encountered a situation where the intermediate result-sets exhaust all possibilities.", noSuchElementException);
            }
            return Optional.empty();
        }
        AlgNode algNode;
        try {
            algNode = this.constructionGraph.getQueryGraph().finish( statement );
        } catch ( IllegalArgumentException illegalArgumentException ) {
            algNode = null;
            log.error("Constructor could not convert generated query: ", illegalArgumentException);
        }

        return Optional.of( Triple.of( statement, algNode , seed ) );
    }


    /**
     * Constructs a random query on the given list of tables.
     * @throws NoSuchElementException if the generation with the given seed leads to an unresolvable operator tree.
     */
    @SuppressWarnings("unchecked")
    private void constructDql( long seed, List<CatalogTable> tables ) throws NoSuchElementException {

        Weights w = Weights.defaultWeights();

        // NOTE: Changing the way tables are chosen will affect all following operations.
        Random rng = new Random( seed );

        // Get complexity for nodes.
        int leaves = rng.nextInt(1, complexity + 1);

        // Randomly Select as many tables and scan them.
        List<Scan> scans = IntStream.generate(() -> rng.nextInt(0, tables.size()))
                .limit(leaves)
                .mapToObj(tables::get)
                .map(Scan::scan)
                .map( scan -> scan.setStats( columnStatistics ) )
                .collect(Collectors.toList());

        // Add Pseudo-ResultSets to graphs.
        scans.forEach(constructionGraph.getColumnGraph()::addScanResult);
        scans.forEach( constructionGraph.getQueryGraph()::addScan );

        // Dynamic list of currently available nodes.
        LinkedList<Node> nodes = new LinkedList<>(scans);


        // We have k nodes left to place.
        int k = this.complexity;
        while (k > 0) {
            if ( log.isDebugEnabled() ) {
                log.debug("#".repeat(50) + " \t Generating Node \t " + (complexity - k) + "#".repeat(50));
                log.debug("-".repeat(150));
                nodes.forEach( node -> {
                    node.getResult().getColumns().forEach( column -> {
                        log.debug(String.format("|%-50s%-5s%-100s|", column.getName(), "->", node));
                    });
                });
            }

            // Building the Decision Graph
            constructionGraph.clearDecisionGraph( lastDecisionStump );

            // At zero nodes available we have to have one node left, ergo we can see what operations we are allowed in the iteration.
            // Each binary op allows us to reduce the number of nodes by 1, while a unary operation reduces available nodes by one.
            int n = nodes.size();
            boolean unaryAllowed = n < k || n == 1;
            boolean binaryAllowed = n > 1;

            if (unaryAllowed) {

                // Get all columns that are in the intermediate Result Sets.
                List<Column> columns = ConstructUtil.flatten(nodes
                        .stream()
                        .map(Node::getResult)
                        .map(Result::getColumns)
                        .collect(Collectors.toList())
                );

                // Assess Filtering
                // Here we will get all columns and all operations, crossing them together in the decision tree.
                // Todo use Statistics to assess filter Operations.. Pair OperatorNames with Columns for "best" Filter results.
                Decision<OperatorType> filter = constructionGraph.getDecisionGraph().addDecision( OperatorType.FILTER, w.get( OperatorType.FILTER.getName() ));
                List<Decision<Column>> filterFields = constructionGraph.getDecisionGraph().addDecisionLayer( filter, columns, ConstructUtil.defaultWeights(columns) );

                // log.debug("Filter Fields: " + Arrays.toString( (filterFields).stream().map(Decision::getVal).map( Column::getPolyType ).toArray() ) );

                if ( ! evaluateFilter( constructionGraph.getDecisionGraph(), filterFields, filterFields.stream().map( Decision::getVal).collect(Collectors.toList()) ) ) {
                    constructionGraph.getDecisionGraph().block( filter );
                }

                // List<Decision<OperatorName>> filterOperator = polyGraph.getDecisionGraph().addDecisionCross( filterFields, Operation.OPERATOR_NAMES, ConstructUtil.defaultWeights(Operation.OPERATOR_NAMES));


                // Gives us FILTER -> COLUMN -> OPERATOR at iteration.

                // Assess Projecting
                // We can do a lot when Projecting... Infinite things.. So we will simplify it by constraining.
                // First get nodes with more than x ≥ 2 columns, x can be changed here...
                List<Node> projectableNodes = nodes.stream().filter(node -> node.getResult().getColumns().size() >= 2).collect(Collectors.toList());

                // log.debug("Projectable Nodes: " + Arrays.toString( (projectableNodes).stream().map(Node::getOperatorType).toArray() ) );

                if (!projectableNodes.isEmpty()) {

                    // For choosing possible Projections while not exploding we randomly select finite subsets from the columns
                    // of the nodes / do a little fuzzing.
                    List<List<Column>> projectionTargets = ConstructUtil.flatten(nodes
                            .stream()
                            .filter(node -> node.getResult().getColumns().size() >= 2)
                            .map(node -> ConstructUtil.subsetFuzz(node.getResult().getColumns(), rng, 4, 4))
                            .collect(Collectors.toList())
                    );

                    if ( ! projectionTargets.isEmpty() ) {
                        Decision<OperatorType> project = constructionGraph.getDecisionGraph().addDecision(OperatorType.PROJECT,  w.get( OperatorType.PROJECT.getName() ));
                        List<Decision<List<Column>>> projectColumns = constructionGraph.getDecisionGraph().addDecisionLayer( project, projectionTargets, ConstructUtil.defaultWeights(projectionTargets));
                    }

                    // log.debug( "projectTargets: " + projectColumns );

                }
                // Gives us PROJECT -> PROJECTION-TARGET at iteration.

                // Assess Sorting

                Decision<OperatorType> sort = constructionGraph.getDecisionGraph().addDecision(OperatorType.SORT,  w.get( OperatorType.SORT.getName() ));

                List<Integer> sortDirections = List.of( 1, -1);

                List<Decision<Integer>> direction = constructionGraph.getDecisionGraph().addDecisionLayer(sort, sortDirections, ConstructUtil.defaultWeights(sortDirections));

                // --- RNG CAPSULE 3 --- BEGIN
                // NOTE: The result of the upstream nodes will be affected.

                // For choosing possible Projections while not exploding we randomly select finite subsets from the columns
                // of the nodes / do a little fuzzing.
                List<List<Column>> sortTargets = ConstructUtil.flatten(nodes
                        .stream()
                        .map(node -> ConstructUtil.subsetFuzz(node.getResult().getColumns(), rng, 3, 3))
                        .collect(Collectors.toList())
                );

                // --- RNG CAPSULE 3 --- END

                List<Decision<List<Column>>> sortFields = constructionGraph.getDecisionGraph().addDecisionCross(direction, sortTargets, ConstructUtil.defaultWeights(sortTargets));

                // This gives us SORT -> SORT_DIRECTION -> SORT-COLUMNS at iteration.

                // Assess Aggregates
                // For now one column aggregates...
                // Todo use Statistics to assess Aggregate Operations.. Pair OperatorNames with Columns for "best" Aggregate results.

//                decisionGraph.addDecision(OperatorType.AGGREGATE, 1.0d);
//
//                List<String> aggregations = List.of("SUM", "COUNT", "AVG", "MAX", "MIN");
//
//                decisionGraph.addDecisionLayer(OperatorType.AGGREGATE, aggregations, ConstructUtil.defaultWeights(aggregations));
//
//                List<Column> aggregateTargets = columns
//                        .stream()
//                        .filter(column -> PolyType.NUMERIC_TYPES.contains(column.getPolyType()))
//                        .collect(Collectors.toList());
//
//                decisionGraph.addDecisionCross(aggregations, aggregateTargets, ConstructUtil.defaultWeights(aggregateTargets));
//
//                // This gives us AGGREGATE -> AGGREGATION -> AGGREGATION_COLUMNS at iteration.

            }

            if (binaryAllowed) {

                // Assess Set Operations without Projections on the current ResultSets.



                constructionGraph.getColumnGraph().setOperationsWithoutProjections( nodes ).ifPresentOrElse(
                        setOperationTargets -> {
                            Decision<OperatorType> union = constructionGraph.getDecisionGraph().addDecision(OperatorType.UNION,  w.get( OperatorType.UNION.getName() ));
                            Decision<OperatorType> minus = constructionGraph.getDecisionGraph().addDecision(OperatorType.MINUS,  w.get( OperatorType.MINUS.getName() ));
                            Decision<OperatorType> intersect = constructionGraph.getDecisionGraph().addDecision(OperatorType.INTERSECT,  w.get( OperatorType.INTERSECT.getName() ));

                            List<Double> weights = ConstructUtil.defaultWeights(setOperationTargets);

                            // Left Dangling for further config...
                            List<Decision<Pair<Node, Node>>> unionTargets =  constructionGraph.getDecisionGraph().addDecisionLayer( union, setOperationTargets, weights);
                            List<Decision<Pair<Node, Node>>> minusTargets =  constructionGraph.getDecisionGraph().addDecisionLayer( minus, setOperationTargets, weights);
                            List<Decision<Pair<Node, Node>>> intersectTargets =  constructionGraph.getDecisionGraph().addDecisionLayer( intersect, setOperationTargets, weights);
                        },
                        () -> {
                            constructionGraph.getColumnGraph().allowsSetOperationWithProjection( nodes ).ifPresent(
                                    projectionCodes -> {
                                        Pair<List<Column>, List<Column>> codes = ConstructUtil.choose( projectionCodes, seed );

                                        Pair<Node, Node> setOperationTarget = Pair.of(
                                                codes.left.get( 0 ).getResult().getNode(),
                                                codes.right.get( 0 ).getResult().getNode()
                                        );

                                        Decision<OperatorType> union = constructionGraph.getDecisionGraph().addDecision(OperatorType.UNION,  w.get( OperatorType.UNION.getName() ));
                                        Decision<OperatorType> minus = constructionGraph.getDecisionGraph().addDecision(OperatorType.MINUS,  w.get( OperatorType.MINUS.getName() ));
                                        Decision<OperatorType> intersect = constructionGraph.getDecisionGraph().addDecision(OperatorType.INTERSECT,  w.get( OperatorType.INTERSECT.getName() ));

                                        // Left Dangling for further config...
                                        List<Double> weights = ConstructUtil.defaultWeights( List.of( Pair.of( null, setOperationTarget) ) );
                                        List<Decision<Pair<Pair<List<Column>, List<Column>>, Pair<Node, Node>>>> unionTargets =
                                                constructionGraph.getDecisionGraph().addDecisionLayer( union, List.of( Pair.of( codes, setOperationTarget) ) , weights);
                                        List<Decision<Pair<Pair<List<Column>, List<Column>>, Pair<Node, Node>>>> minusTargets =
                                                constructionGraph.getDecisionGraph().addDecisionLayer( minus, List.of( Pair.of( codes, setOperationTarget) ) , weights);
                                        List<Decision<Pair<Pair<List<Column>, List<Column>>, Pair<Node, Node>>>> intersectTargets =
                                                constructionGraph.getDecisionGraph().addDecisionLayer( intersect, List.of( Pair.of( codes, setOperationTarget) ) , weights);
                                    }
                            );
                        }
                );


                // Assess Join Operations without Projections on the current ResultSets.

                constructionGraph.getColumnGraph().joinOperationsWithoutProjections(nodes).ifPresent(
                        joinTargets -> {
                            Decision<OperatorType> join = constructionGraph.getDecisionGraph().addDecision(OperatorType.JOIN,  w.get( OperatorType.JOIN.getName() ));

                            List<OperatorName> joinOps = List.of(OperatorName.EQUALS);
                            List<Decision<OperatorName>> joinOperators = constructionGraph.getDecisionGraph().addDecisionLayer(join, joinOps, ConstructUtil.defaultWeights(joinOps));
                            List<JoinAlgType> joinTypes = List.of(JoinAlgType.values());
                            List<Decision<JoinAlgType>> joinAlgTypes = constructionGraph.getDecisionGraph().addDecisionCross(joinOperators, joinTypes, ConstructUtil.defaultWeights(joinTypes));
                            List<Decision<Pair<Column, Column>>> targets = constructionGraph.getDecisionGraph().addDecisionCross(joinAlgTypes, joinTargets, ConstructUtil.defaultWeights(joinTargets));
                        }
                );

            }

            // Now we build the node.
            RandomWalkVertexIterator<Decision<?>, DefaultEdge> iterator = constructionGraph.getDecisionGraph().randomWalk(seed, true);

            iterator.next();

            Decision<?> decision = null;

            decision = iterator.next();

            OperatorType operatorType = (OperatorType) decision.getVal();

            Node parent;
            Pair<Node, Node> children;
            switch ( operatorType ) {
                case UNION:
                    decision = iterator.next();
                    children = projectionSetOp( decision, nodes );
                    parent = Union.union( children );

                    break;
                case MINUS:
                    decision = iterator.next();
                    children = projectionSetOp( decision, nodes );
                    parent = Minus.minus( children );

                    break;
                case INTERSECT:
                    decision = iterator.next();
                    children = projectionSetOp( decision, nodes );
                    parent = Intersection.intersection( children );
                    break;
                case JOIN:
                    decision = iterator.next();
                    OperatorName joinOperator = (OperatorName) decision.getVal();
                    decision = iterator.next();
                    JoinAlgType joinAlgType = (JoinAlgType) decision.getVal();
                    decision = iterator.next();
                    Pair<Column, Column> joinTarget = (Pair<Column, Column>) decision.getVal();
                    parent = Join.join(joinTarget, joinAlgType, joinOperator);
                    break;

                case SORT:
                    decision = iterator.next();
                    Integer sortDirection = (Integer) decision.getVal();
                    decision = iterator.next();
                    List<Column> sortColumns = (List<Column>) decision.getVal();

                    parent = Sort.sort( sortDirection, sortColumns );

                    break;

                case AGGREGATE:
                    /*
                    decision = iterator.next();
                    String aggregation = (String) decision.getVal();
                    decision = iterator.next();
                    List<Column> aggregateFields = (List<Column>) decision.getVal();

                    parent = Aggregate.aggregate( aggregateFields, aggregation );
                    break;

                     */
                    throw new RuntimeException();
                case FILTER:
                    decision = iterator.next();
                    Column filterField = (Column) decision.getVal();
                    decision = iterator.next();
                    OperatorName filterOperator = (OperatorName) decision.getVal();
                    decision = iterator.next();
                    Object value = decision.getVal();
                    parent = Filter.filter(filterField, filterOperator, value);
                    break;
                case PROJECT:
                    decision = iterator.next();
                    List<Column> projectFields = (List<Column>) decision.getVal();

                    parent = Project.project( new LinkedList<>( projectFields ) );
                    break;
                default:
                    throw new RuntimeException();

            }

            lastDecisionStump = decision; // Visualization

            if (parent instanceof Unary) {
                Unary unary = (Unary) parent;
                constructionGraph.getQueryGraph().addUnary( unary, unary.getTarget() );


                nodes.remove(unary.getTarget());
                nodes.add(unary);
            } else {
                Binary binary = (Binary) parent;
                constructionGraph.getQueryGraph().addBinary((Binary) parent, binary.getTarget());

                nodes.remove(binary.getTarget().left);
                nodes.remove(binary.getTarget().right);

                nodes.add(binary);
            }

            constructionGraph.getColumnGraph().updateResults(nodes);
            --k;

            // Loop for next Node...

        }

        if ( log.isDebugEnabled() ) {
            log.debug("Finalized Root:");
            for ( Node node : nodes ) {
                log.debug( "\t" + node + " : " + node.getResult().getColumns().stream().map( Column::getPolyType ).collect(Collectors.toList()));
            }
        }

        assert nodes.size() == 1;


    }

    /**
     * Implements a decision to project two result-sets prior to a set operation.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private Pair<Node, Node> projectionSetOp( Decision<?> decision, List<Node> nodes ) {

        Pair pair = (Pair) decision.getVal();

        if ( pair.left instanceof Pair ) {
            // Case Added Projections
            Pair<List<Column>, List<Column>> codes = (Pair<List<Column>, List<Column>>) pair.left;

            Pair<Node, Node> childrenTemp = (Pair<Node, Node>) pair.right;

            Project projectLeft = Project.project( (LinkedList<Column>) codes.left );
            Project projectRight = Project.project( (LinkedList<Column>) codes.right );

            nodes.remove( childrenTemp.left );
            nodes.remove( childrenTemp.right );

            if ( log.isDebugEnabled() ) {
                log.debug("Anticipating changes through retroactive projections...");
            }

            this.constructionGraph.getQueryGraph().addUnary( projectLeft, childrenTemp.left );
            this.constructionGraph.getQueryGraph().addUnary( projectRight, childrenTemp.right );

            this.constructionGraph.getColumnGraph().updateResults( nodes );

            return Pair.of( projectLeft, projectRight );

        } else {
            return (Pair<Node, Node>) decision.getVal();
        }

    }

    public static boolean evaluateFilter( DecisionGraph decisionGraph, List<Decision<Column>> decisions, List<Column> columns ) {
        // Evaluate Filter Expressions for column.

        boolean foundOption = false;

        for ( int i = 0; i < decisions.size(); i++ ) {
            Decision<?> decision = decisions.get( i );
            Column column = columns.get( i );

            double avgValueCount = column.getColumnStatistic().getAverageValueCount();

            // This check should see whether we can use equals filters...
            if ( avgValueCount > 4d && column.getColumnStatistic().getFrequency() != null && column.getColumnStatistic().getFrequency().size() > 1 ) {

                Decision<?> equals = decisionGraph.addDecision( decision, OperatorName.EQUALS, 1d ); // Add equals

                double threshold = 0d;
                List<Object> conjunctives = new LinkedList<>();
                for ( Pair<Object, Double> pair : column.getColumnStatistic().getFrequency() ) {
                    if ( threshold >= 0.45d || String.valueOf( pair.left ).equals( "Other" ) ) {
                        break;
                    }
                    threshold += pair.right;
                    conjunctives.add( pair.left );
                }
//                    for ( Object conj : conjunctives ) {
//                        // Todo, Include Optional OR statements in Filter Expressions..
//                    }
                // For now lets just take the most frequent value...
                Object obj = conjunctives.get( 0 );

                Decision<?> value = decisionGraph.addDecision( equals, obj, 1d ); // Add filter value

                foundOption = true;
            }

            // Checks whether we can use other filters...
            if (PolyType.NUMERIC_TYPES.contains( column.getColumnStatistic().getType() )) {
                double avg = column.getColumnStatistic().getNumericAverage();

                Decision<OperatorName> lessThan = decisionGraph.addDecision( decision, OperatorName.LESS_THAN, 1d );
                Decision<OperatorName> lessThanOrEqual = decisionGraph.addDecision( decision, OperatorName.LESS_THAN_OR_EQUAL, 1d );
                Decision<OperatorName> greaterThan = decisionGraph.addDecision( decision, OperatorName.GREATER_THAN, 1d );
                Decision<OperatorName> greaterThanOrEqual = decisionGraph.addDecision( decision, OperatorName.GREATER_THAN_OR_EQUAL, 1d );

                List<Decision<Double>> average = decisionGraph.addDecisionCross(
                        List.of( lessThan, lessThanOrEqual, greaterThan, greaterThanOrEqual ),
                        List.of( avg ),
                        List.of( 1d )
                );

                foundOption = true;
            }

        }

        return foundOption;

    }


    private static class Weights extends HashMap<String, Double> {
        public static Weights defaultWeights() {
            Weights weights = new Weights();

            weights.put("Union", 5/5d );
            weights.put("Minus", 4/10d );
            weights.put("Intersect", 3/10d );
            weights.put("Join", 1d );
            weights.put("Left", 1/2d );
            weights.put("Right", 3/4d );
            weights.put("Inner", 7/8d );
            weights.put("Full", 7/8d );
            weights.put("Filter", 4/7d );
            weights.put("Project", 5/7d );
            weights.put("Aggregate", 0d );
            weights.put("Sort", 8/14d );

            return weights;

        }

    }

}


