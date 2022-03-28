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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.exception.SelfAdaptiveRuntimeException;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.adaptiveness.selfadaptiveness.SelfAdaptiveUtil.AdaptiveKind;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

@Slf4j
@Getter
public class SelfAdaptivAgent {


    private static AdaptiveQueryProcessor adaptiveQueryInterface;
    private static SelfAdaptivAgent INSTANCE = null;


    public static SelfAdaptivAgent getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SelfAdaptivAgent();
        }
        return INSTANCE;
    }


    private final Map<Pair<String, Action>, List<ManualDecision>> manualDecisions = new HashMap<>();
    private final Map<Pair<String, Action>, ManualDecision> newlyAddedManualDecision = new HashMap<>();

    private final Queue<ManualDecision> adaptingQueue = new ConcurrentLinkedQueue<>();


    public void initialize( TransactionManager transactionManager, Authenticator authenticator ) {
        this.setAdaptiveQueryProcessor( new AdaptiveQueryProcessor( transactionManager, authenticator ) );
    }


    private void setAdaptiveQueryProcessor( AdaptiveQueryProcessor adaptiveQueryProcessor ) {
        adaptiveQueryInterface = adaptiveQueryProcessor;
    }


    public <T> void addDecision( ManualDecision<T> manualDecision ) {
        if ( this.manualDecisions.containsKey( manualDecision.getKey() ) ) {
            List<ManualDecision> decisionsList = new ArrayList<>( manualDecisions.remove( manualDecision.getKey() ) );
            decisionsList.add( manualDecision );
            log.warn( "add second decision" );
            manualDecisions.put( manualDecision.getKey(), decisionsList );
            newDecision( manualDecision );
        } else {
            manualDecision.setDecisionStatus( DecisionStatus.CREATED );
            manualDecisions.put( manualDecision.getKey(), Collections.singletonList( manualDecision ) );
            log.warn( "add first decision" );
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

        //
        if ( isNewDecisionBetter( getOrdered( manualDecision.getWeightedList() ), getOrdered( weightedList ) ) ) {
            manualDecision.getAction().redo( newManualDecision, adaptiveQueryInterface.getTransaction() );
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


    public void makeWorkloadDecision( Class clazz, Action action, AlgNode algNode ) {
        switch ( action ) {
            case LESS_COMPLEX_QUERIES:

                break;
            case MORE_COMPLEX_QUERIES:


/*

                InformationContext context = new InformationContext();
                context.setPossibilities( possibleStores, DataStore.class );
                context.setNameSpaceModel( Catalog.getInstance().getSchema( namespaceId ).schemaType );


                if ( RuntimeConfig.SELF_ADAPTIVE.getBoolean() ) {
                    Decision decision = new Decision(

                            new Timestamp( System.currentTimeMillis() ),
                            ClauseCategory.STORE,
                            AdaptiveKind.PASSIVE,
                            DataStore.class,
                            Action.CHECK_STORES_ADD,
                            namespaceId,
                            entityId,
                            preSelection );

                    return rank( context, Optimization.SELECT_STORE, decision );
                }


 */
                break;
            default:
                log.warn( "This Action " + action.name() + " is not defined in makeWorkloadDecision." );
                throw new SelfAdaptiveRuntimeException( "This Action " + action.name() + " is not defined in makeWorkloadDecision." );
        }


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


    /**
     * CREATED: new decision added for the first time
     * ADJUSTED: redo of the decision was done
     * NOT_APPLICABLE: not all involved components are still available
     * OLD_DECISION: since the decision was added to the list it was redone manually
     */
    public enum DecisionStatus {
        CREATED, ADJUSTED, NOT_APPLICABLE, OLD_DECISION
    }


    public enum WorkloadAdaptions {
        ADD_INDEX, DELETE_INDEX, ADD_MATERIALIZED_VIEW, DELETE_MATERIALIZED_VIEW
    }

}
