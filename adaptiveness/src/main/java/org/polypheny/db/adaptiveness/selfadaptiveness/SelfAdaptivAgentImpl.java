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

package org.polypheny.db.adaptiveness.selfadaptiveness;

import static org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.DecisionStatus.CREATED;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.SelfAdaptivAgent;
import org.polypheny.db.adaptiveness.WorkloadInformation;
import org.polypheny.db.adaptiveness.exception.SelfAdaptiveRuntimeException;
import org.polypheny.db.adaptiveness.policy.BooleanClause;
import org.polypheny.db.adaptiveness.policy.Clause;
import org.polypheny.db.adaptiveness.policy.PoliceUtil.ClauseType;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.AdaptiveKind;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.DecisionStatus;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.Trigger;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

@Slf4j

public class SelfAdaptivAgentImpl implements SelfAdaptivAgent {


    private static AdaptiveQueryProcessor adaptiveQueryInterface;
    private static SelfAdaptivAgentImpl INSTANCE = null;


    public static SelfAdaptivAgentImpl getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SelfAdaptivAgentImpl();
        }
        return INSTANCE;
    }


    private final Map<Pair<String, Action>, List<ManualDecision>> manualDecisions = new HashMap<>();

    private final Map<String, List<Pair<Timestamp, AutomaticDecision>>> automaticDecisions = new HashMap<>();
    private final Map<Pair<String, Action>, ManualDecision> newlyAddedManualDecision = new HashMap<>();

    private final Queue<ManualDecision> adaptingQueue = new ConcurrentLinkedQueue<>();


    public void initialize( TransactionManager transactionManager, Authenticator authenticator ) {
        this.setAdaptiveQueryProcessor( new AdaptiveQueryProcessor( transactionManager, authenticator ) );
    }


    private void setAdaptiveQueryProcessor( AdaptiveQueryProcessor adaptiveQueryProcessor ) {
        adaptiveQueryInterface = adaptiveQueryProcessor;
    }


    public void addMaterializedViews( String algCompareString, AlgNode tableScan ) {
        materializedViews.put( algCompareString, tableScan );
    }


    public Map<String, AlgNode> getMaterializedViews() {
        Map<String, AlgNode> test = this.materializedViews;
        return this.materializedViews;
    }


    public <T> void addManualDecision( ManualDecision<T> manualDecision ) {
        if ( this.manualDecisions.containsKey( manualDecision.getKey() ) ) {
            List<ManualDecision> decisionsList = new ArrayList<>( manualDecisions.remove( manualDecision.getKey() ) );
            decisionsList.add( manualDecision );
            log.warn( "add second decision" );
            manualDecisions.put( manualDecision.getKey(), decisionsList );
            newDecision( manualDecision );
        } else {
            manualDecision.setDecisionStatus( CREATED );
            manualDecisions.put( manualDecision.getKey(), Collections.singletonList( manualDecision ) );
        }
    }


    public <T> void addAutomaticDecision( AutomaticDecision<T> automaticDecision ) {
        if ( this.automaticDecisions.containsKey( automaticDecision.getKey() ) ) {
            List<Pair<Timestamp, AutomaticDecision>> decisionsList = new ArrayList<>( automaticDecisions.remove( automaticDecision.getKey() ) );
            decisionsList.add( new Pair<>( automaticDecision.timestamp, automaticDecision ) );
            automaticDecisions.put( automaticDecision.getKey(), decisionsList );
        } else {
            automaticDecision.setDecisionStatus( CREATED );
            automaticDecisions.put( automaticDecision.getKey(), Collections.singletonList( new Pair<>( automaticDecision.timestamp, automaticDecision ) ) );
        }

    }


    private <T> void newDecision( ManualDecision<T> manualDecision ) {
        newlyAddedManualDecision.remove( manualDecision.getKey() );
        newlyAddedManualDecision.put( manualDecision.getKey(), manualDecision );
    }


    // todo ig: when to add to the queue
    private synchronized <T> void addToQueue( ManualDecision<T> manualDecision ) {
        adaptingQueue.add( manualDecision );
    }


    public synchronized void addAllDecisionsToQueue() {
        this.manualDecisions.forEach( ( k, v ) -> {
            adaptingQueue.add( v.get( 0 ) );
        } );
        adaptTheSystem();
    }


    private <T> boolean checkOldDecisions( ManualDecision<T> manualDecision ) {

        // Check if entity and namespace still exist
        Catalog catalog = Catalog.getInstance();
        if ( !catalog.checkIfExistsTable( manualDecision.getEntityId() ) && !catalog.checkIfExistsSchema( Catalog.defaultDatabaseId, catalog.getSchema( manualDecision.getNameSpaceId() ).name ) ) {
            updateDecisionStatus( manualDecision, DecisionStatus.NOT_APPLICABLE );
            return false;
        }

        switch ( manualDecision.getClauseCategory() ) {
            case STORE:
                List<DataStore> dataStores = (List<DataStore>) WeightedList.weightedToList( manualDecision.getWeightedList() );
                for ( DataStore dataStore : dataStores ) {
                    if ( !catalog.checkIfExistsAdapter( dataStore.getAdapterId() ) ) {
                        updateDecisionStatus( manualDecision, DecisionStatus.NOT_APPLICABLE );
                        return false;
                    }
                }
                break;
            case SELF_ADAPTING:
                // Nothing additional needs to be checked for the category SELF_ADAPTING
                break;
            default:
                log.warn( "Clause Category: " + manualDecision.getClauseCategory() + " is not yet implemented. Please add Clause Category to the methode checkOldDecisions" );
                throw new SelfAdaptiveRuntimeException( "Clause Category: " + manualDecision.getClauseCategory() + " is not yet implemented. Please add Clause Category to the methode checkOldDecisions" );
        }

        return true;
    }


    private <T> void updateDecisionStatus( ManualDecision<T> manualDecision, DecisionStatus decisionStatus ) {
        List<ManualDecision> decisionsList = new ArrayList<>( manualDecisions.remove( manualDecision.getKey() ) );
        if ( decisionsList != null ) {
            decisionsList.remove( manualDecision );
            manualDecision.setDecisionStatus( decisionStatus );
            decisionsList.add( manualDecision );
            manualDecisions.put( manualDecision.getKey(), decisionsList );
        }
    }


    private <T> void rerateDecision( ManualDecision<T> manualDecision ) {

        WeightedList<?> weightedList = null;
        if ( manualDecision.getAdaptiveKind() == AdaptiveKind.MANUAL ) {
            log.warn( "in passive" );
            weightedList = PoliciesManager.getInstance().makeDecisionWeighted( manualDecision.getClazz(),
                    manualDecision.getAction(),
                    manualDecision.getNameSpaceId(),
                    manualDecision.getEntityId(),
                    manualDecision.getPreSelection() );
        } else if ( manualDecision.getAdaptiveKind() == AdaptiveKind.AUTOMATIC ) {
            log.warn( "in active" );
            //weightedList = makeWorkloadDecision(  );
        } else {
            log.warn( "The AdaptiveKind " + manualDecision.getAdaptiveKind() + " is not implemented yet." );
            throw new SelfAdaptiveRuntimeException( "The AdaptiveKind " + manualDecision.getAdaptiveKind() + " is not implemented yet." );
        }

        ManualDecision newManualDecision = newlyAddedManualDecision.remove( manualDecision.getKey() );

        // Check if the correct Decision is safed
        if ( newManualDecision != null && weightedList.equals( newManualDecision.getWeightedList() ) ) {
            log.warn( "It is the same weighted List." );
        }
        if ( isNewDecisionBetter( getOrdered( manualDecision.getWeightedList() ), getOrdered( weightedList ) ) ) {
            Transaction trx = adaptiveQueryInterface.getTransaction();
            manualDecision.getAction().doChange( newManualDecision, trx );
            try {
                trx.commit();
            } catch ( TransactionException e ) {
                try {
                    trx.rollback();
                } catch ( TransactionException ex ) {
                    throw new RuntimeException( "Error while rolling back self-adapting workload: " + e );
                }
                throw new RuntimeException( "Error while committing self-adapting workload: " + e );
            }
        }
    }


    private WeightedList<?> getOrdered( WeightedList weightedList ) {
        WeightedList<Object> orderedList = new WeightedList<>();
        ((WeightedList<Object>) weightedList).entrySet().stream().sorted( Map.Entry.comparingByValue( Comparator.reverseOrder() ) ).forEachOrdered( x -> orderedList.put( x.getKey(), x.getValue() ) );

        return orderedList;
    }


    private boolean isNewDecisionBetter( WeightedList<?> oldWeightedList, WeightedList<?> newWeightedList ) {
        // Overall better
        Pair<Double, Double> overallBetter = WeightedList.compareOverall( oldWeightedList, newWeightedList );
        // Only first better
        Pair<Object, Object> firstBetter = WeightedList.comparefirst( oldWeightedList, newWeightedList );
        if ( overallBetter.left < overallBetter.right || !firstBetter.left.equals( firstBetter.right ) ) {
            return true;
        }
        return false;
    }


    public void adaptTheSystem() {
        while ( !adaptingQueue.isEmpty() ) {
            ManualDecision manualDecision = adaptingQueue.remove();
            if ( manualDecision.getDecisionStatus() == DecisionStatus.NOT_APPLICABLE || !checkOldDecisions( manualDecision ) ) {
                log.warn( "Decision is not applicable anymore, deleted from queue and marked in decision overview." );
            } else {
                rerateDecision( manualDecision );
            }
        }
    }


    public <T> void makeWorkloadDecision( Class<T> clazz, Trigger trigger, T selected, WorkloadInformation workloadInformation, boolean increase ) {

        switch ( trigger ) {
            case REPEATING_QUERY:
                // there are more repeating queries detected
                if ( increase ) {

                    List<Object> possibleActions = List.of( Action.MATERIALIZED_VIEW_ADDITION, Action.INDEX_ADDITION );

                    InformationContext context = new InformationContext();
                    context.setPossibilities( possibleActions, Action.class );
                    String decisionKey = trigger.name() + ((AlgNode) selected).algCompareString();

                    AutomaticDecision automaticDecision = new AutomaticDecision(
                            new Timestamp( System.currentTimeMillis() ),
                            AdaptiveKind.AUTOMATIC,
                            clazz,
                            selected,
                            decisionKey,
                            workloadInformation
                    );

                    WeightedList<T> weightedList = rankPossibleActions( context, Optimization.WORKLOAD_REPEATING_QUERY );
                    if ( weightedList != null ) {
                        Action bestAction = WeightedList.getBest( weightedList );
                        automaticDecision.setWeightedList( weightedList );
                        automaticDecision.setBestAction( bestAction );
                        addAutomaticDecision( automaticDecision );

                        boolean adaptSystem = false;
                        if ( automaticDecisions.containsKey( decisionKey ) ) {
                            for ( Pair<Timestamp, AutomaticDecision> automaticDecisionTime : automaticDecisions.get( decisionKey ) ) {
                                if ( automaticDecisionTime.left.getTime() + TimeUnit.MINUTES.toMillis( 30 ) < new Timestamp( System.currentTimeMillis() ).getTime() ) {
                                    adaptSystem = true;
                                }
                            }
                            // Do the change if it is the first or it is  an old decision
                            if ( automaticDecisions.get( decisionKey ).size() == 1 || adaptSystem ) {
                                bestAction.doChange( automaticDecision, adaptiveQueryInterface.getTransaction() );
                                adaptSystem = false;
                            }
                        }
                    }

                    //there are less repeating queries
                } else {

                }

                break;
            case JOIN_FREQUENCY:

                break;
            case AVG_TIME_CHANGE:
                break;
            default:
                log.warn( "This Trigger " + trigger.name() + " is not defined in makeWorkloadDecision." );
                throw new SelfAdaptiveRuntimeException( "This Trigger" + trigger.name() + " is not defined in makeWorkloadDecision." );

        }
    }


    private <T> WeightedList<T> rankPossibleActions( InformationContext informationContext, Optimization optimization ) {
        List<WeightedList<?>> rankings = new ArrayList<>();
        List<Clause> interestingClauses = PoliciesManager.getInstance().getSelfAdaptiveClauses();

        if ( interestingClauses.isEmpty() ) {
            log.warn( "No Self-Adaptive Options Selected, System can not adapt itself." );
        } else {
            for ( Clause interestingClause : interestingClauses ) {
                if ( interestingClause.isA( ClauseType.BOOLEAN ) && ((BooleanClause) interestingClause).isValue() && optimization.getRank().containsKey( interestingClause.getClauseName() ) ) {
                    rankings.add( optimization.getRank().get( interestingClause.getClauseName() ).apply( informationContext ) );
                }
            }

            WeightedList<T> avgRankings = WeightedList.avg( rankings );

            return avgRankings;

        }

        return null;
    }


    @Getter
    public static class InformationContext {

        private List<Object> possibilities = null;
        private Class<?> clazz = null;
        @Setter
        private SchemaType nameSpaceModel;


        public void setPossibilities( List<Object> possibilities, Class<?> clazz ) {
            if ( this.possibilities != null ) {
                throw new RuntimeException( "Already set possibilities." );
            }
            this.possibilities = possibilities;
            this.clazz = clazz;
        }


    }


}
