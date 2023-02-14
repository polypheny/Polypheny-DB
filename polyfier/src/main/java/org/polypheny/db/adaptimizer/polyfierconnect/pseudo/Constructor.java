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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.RandomWalkVertexIterator;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs.ConstructionGraph;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs.Decision;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.graphs.DecisionGraph;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes.*;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Operation;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Column;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.RandomNode;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Result;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.http.model.SortDirection;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.Pair;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class Constructor {

    private final Transaction transaction;
    private final int cap;
    private PolyGraph polyGraph;

    public Constructor( Transaction transaction, int cap ) {
        this.transaction = transaction;
        this.cap = cap;
    }

    public Optional<Triple<Statement, AlgNode, Long>> construct(RandomNode randomNode, List<CatalogTable> tables ) {
        Statement statement = transaction.createStatement();
        this.polyGraph = new PolyGraph( statement );
        log.debug("Constructing Tree with seed " + randomNode.get() );
        try {
            construct( randomNode, statement, tables );
        } catch ( NoSuchElementException noSuchElementException ) {
            if ( log.isDebugEnabled() ) {
                throw new RuntimeException( noSuchElementException );
            }
            log.error( "Could not find tree for seed" + randomNode.get() );
            return Optional.empty();
        }
        return Optional.of( Triple.of( statement, this.polyGraph.getConstructionGraph().finish( statement ), randomNode.get() ) );
    }

    private void construct( RandomNode randomNode, Statement statement, List<CatalogTable> tables ) throws NoSuchElementException {
        RandomNode rngNode = randomNode.branch("construct " + statement.toString());


        // We will capsule randomized operations such that if we modify within a capsule, we only change
        // this aspect of the generation. This does not always work fully which will be noted within the
        // capsules.

        // --- RNG CAPSULE rng1 --- BEGIN
        // NOTE: Changing the way tables are chosen will affect all following operations.
        Random rng1 = rngNode.getRng();

        // Get cap for nodes.
        int leafMax = cap + 1;
        int leaves = rng1.nextInt(1, leafMax + 1);

        // Randomly Select as many tables and scan them.

        List<Scan> scans = IntStream.generate(() -> rng1.nextInt(0, tables.size()))
                .limit(leaves)
                .mapToObj(tables::get)
                .map(Scan::scan)
                .collect(Collectors.toList());
        scans.forEach(polyGraph.getRcn()::addScanResult);

        // --- RNG CAPSULE 1 --- END

        scans.forEach( polyGraph.getConstructionGraph()::addScan );

        // Dynamic list of current nodes.
        LinkedList<Node> nodes = new LinkedList<>(scans);

        log.debug("Seed: " + randomNode.get() + " Placeable: " + this.cap );
        for ( Node node : nodes ) {
            log.debug( "\t" + node.getOperatorType() + " : " +  node.getResult().getColumns().stream().map( Column::getPolyType ).collect(Collectors.toList()).toString() );
        }


        // We have k nodes left to place.
        int k = this.cap;
        while (k > 0) {

            // Building the Decision Graph
            polyGraph.clearDecisionGraph();

            // At zero nodes we have to have one node left, ergo we can see what operations we are allowed to do.
            // Each binary op allows us to reduce the number of nodes by 1, so
            int n = nodes.size();
            boolean unaryAllowed = n <= k;

            log.debug( "Unary: " + unaryAllowed );

            boolean binaryAllowed = n > 1;

            log.debug( "Binary: " + binaryAllowed );


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
                Decision<OperatorType> filter = polyGraph.getDecisionGraph().addDecision( OperatorType.FILTER, 1.0d);
                List<Decision<Column>> filterFields = polyGraph.getDecisionGraph().addDecisionLayer( filter, columns, ConstructUtil.defaultWeights(columns) );

                log.debug("Filter Fields: " + Arrays.toString( (filterFields).stream().map(Decision::getVal).map( Column::getPolyType ).toArray() ) );

                List<Decision<OperatorName>> filterOperator = polyGraph.getDecisionGraph().addDecisionCross( filterFields, Operation.OPERATOR_NAMES, ConstructUtil.defaultWeights(Operation.OPERATOR_NAMES));
                // Gives us FILTER -> COLUMN -> OPERATOR at iteration.

                // Assess Projecting
                // We can do a lot when Projecting... Infinite things.. So we will simplify it by constraining.
                // First get nodes with more than x = 2 columns, x can be changed here...
                List<Node> projectableNodes = nodes.stream().filter(node -> node.getResult().getColumns().size() > 2).collect(Collectors.toList());

                log.debug("Projectable Nodes: " + Arrays.toString( (projectableNodes).stream().map(Node::getOperatorType).toArray() ) );

                if (!projectableNodes.isEmpty()) {
                    Decision<OperatorType> project = polyGraph.getDecisionGraph().addDecision(OperatorType.PROJECT, 1.0d);

                    // --- RNG CAPSULE 2 --- BEGIN
                    // NOTE: The result of the upstream nodes will be affected.

                    // For choosing possible Projections while not exploding we randomly select finite subsets from the columns
                    // of the nodes / do a little fuzzing.
                    Random rng2 = rngNode.getRng();
                    List<List<Column>> projectionTargets = ConstructUtil.flatten(nodes
                            .stream()
                            .filter(node -> node.getResult().getColumns().size() > 2)
                            .map(node -> ConstructUtil.subsetFuzz(node.getResult().getColumns(), rng2, 4, 4))
                            .collect(Collectors.toList())
                    );

                    // --- RNG CAPSULE 2 --- END

                    List<Decision<List<Column>>> projectColumns = polyGraph.getDecisionGraph().addDecisionLayer( project, projectionTargets, ConstructUtil.defaultWeights(projectionTargets));

                    log.debug( "projectTargets: " + projectColumns );

                }
                // Gives us PROJECT -> PROJECTION-TARGET at iteration.

                // Assess Sorting

                Decision<OperatorType> sort = polyGraph.getDecisionGraph().addDecision(OperatorType.SORT, 1.0d);

                List<SortDirection> sortDirections = List.of(SortDirection.ASC, SortDirection.DESC);

                List<Decision<SortDirection>> direction = polyGraph.getDecisionGraph().addDecisionLayer(sort, sortDirections, ConstructUtil.defaultWeights(sortDirections));

                // --- RNG CAPSULE 3 --- BEGIN
                // NOTE: The result of the upstream nodes will be affected.

                // For choosing possible Projections while not exploding we randomly select finite subsets from the columns
                // of the nodes / do a little fuzzing.
                Random rng3 = rngNode.getRng();
                List<List<Column>> sortTargets = ConstructUtil.flatten(nodes
                        .stream()
                        .map(node -> ConstructUtil.subsetFuzz(node.getResult().getColumns(), rng3, 3, 3))
                        .collect(Collectors.toList())
                );

                // --- RNG CAPSULE 3 --- END

                List<Decision<List<Column>>> sortFields = polyGraph.getDecisionGraph().addDecisionCross(direction, sortTargets, ConstructUtil.defaultWeights(sortTargets));

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

                polyGraph.getRcn().setOperationsWithoutProjections(nodes).ifPresent(
                        setOperationTargets -> {
                            Decision<OperatorType> union = polyGraph.getDecisionGraph().addDecision(OperatorType.UNION, 1.0d);
                            Decision<OperatorType> minus = polyGraph.getDecisionGraph().addDecision(OperatorType.MINUS, 1.0d);
                            Decision<OperatorType> intersect = polyGraph.getDecisionGraph().addDecision(OperatorType.INTERSECT, 1.0d);

                            List<Double> weights = ConstructUtil.defaultWeights(setOperationTargets);
                            List<Decision<Pair<Node, Node>>> unionTargets =  polyGraph.getDecisionGraph().addDecisionLayer( union, setOperationTargets, weights);
                            List<Decision<Pair<Node, Node>>> minusTargets =  polyGraph.getDecisionGraph().addDecisionLayer( minus, setOperationTargets, weights);
                            List<Decision<Pair<Node, Node>>> intersectTargets =  polyGraph.getDecisionGraph().addDecisionLayer( intersect, setOperationTargets, weights);
                        }
                );

                // Assess Join Operations without Projections on the current ResultSets.

                polyGraph.getRcn().joinOperationsWithoutProjections(nodes).ifPresent(
                        joinTargets -> {
                            Decision<OperatorType> join = polyGraph.getDecisionGraph().addDecision(OperatorType.JOIN, 1.0d);

                            List<OperatorName> joinOps = List.of(OperatorName.EQUALS);
                            List<Decision<OperatorName>> joinOperators = polyGraph.getDecisionGraph().addDecisionLayer(join, joinOps, ConstructUtil.defaultWeights(joinOps));
                            List<JoinAlgType> joinTypes = List.of(JoinAlgType.values());
                            List<Decision<JoinAlgType>> joinAlgTypes = polyGraph.getDecisionGraph().addDecisionCross(joinOperators, joinTypes, ConstructUtil.defaultWeights(joinTypes));
                            List<Decision<Pair<Column, Column>>> targets = polyGraph.getDecisionGraph().addDecisionCross(joinAlgTypes, joinTargets, ConstructUtil.defaultWeights(joinTargets));
                        }
                );

            }

            // Now we build the node.
            RandomWalkVertexIterator<Decision<?>, DefaultEdge> iterator = polyGraph.getDecisionGraph().randomWalk(randomNode.get(), true);

            iterator.next();
            Decision<?> decision = iterator.next();
            OperatorType operatorType = (OperatorType) decision.getVal();

            Node parent;
            Pair<Node, Node> children;
            switch ( operatorType ) {
                case UNION:
                    decision = iterator.next();
                    children = (Pair<Node, Node>) decision.getVal();
                    parent = Union.union(children);

                    break;
                case MINUS:
                    decision = iterator.next();
                    children = (Pair<Node, Node>) decision.getVal();
                    parent = Minus.minus(children);

                    break;
                case INTERSECT:
                    decision = iterator.next();
                    children = (Pair<Node, Node>) decision.getVal();
                    parent = Intersection.intersection(children);

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
                    SortDirection sortDirection = (SortDirection) decision.getVal();
                    decision = iterator.next();
                    List<Column> sortColumns = (List<Column>) decision.getVal();

                    parent = Sort.sort(sortDirection, sortColumns);

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

                    parent = Filter.filter(filterField, filterOperator, "?"); // Todo add filter values
                    break;
                case PROJECT:
                    decision = iterator.next();
                    List<Column> projectFields = (List<Column>) decision.getVal();

                    parent = Project.project( new LinkedList<>( projectFields ) );
                    break;
                default:
                    throw new RuntimeException();

            }

            if (parent instanceof Unary) {
                Unary unary = (Unary) parent;
                polyGraph.getConstructionGraph().addUnary( unary, unary.getTarget() );
                nodes.remove(unary.getTarget());
                nodes.add(unary);
            } else {
                Binary binary = (Binary) parent;
                polyGraph.getConstructionGraph().addBinary((Binary) parent, binary.getTarget());

                nodes.remove(binary.getTarget().left);
                nodes.remove(binary.getTarget().right);

                nodes.add(binary);
            }


            polyGraph.getRcn().updateResults(nodes);
            --k;

            log.debug("Seed: " + randomNode.get() + " Placeable: " + k );
            for ( Node node : nodes ) {
                log.debug( "\t" + node.getOperatorType() + " : " +  node.getResult().getColumns().stream().map( Column::getPolyType ).collect(Collectors.toList()).toString() );
            }

            // Loop

        }

        log.debug("Seed: " + randomNode.get() + " Placeable: " + k );
        for ( Node node : nodes ) {
            log.debug( "\t" + node.getOperatorType() + " : " +  node.getResult().getColumns().stream().map( Column::getPolyType ).collect(Collectors.toList()).toString() );
        }


    }

}


