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
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.AdapterManager.AdapterInformation;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptiveness.exception.SelfAdaptiveRuntimeException;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.adaptiveness.policy.PoliciesManager.Action;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.entity.CatalogAdapter.AdapterType;
import org.polypheny.db.util.Pair;

@Slf4j
@Getter
public class SelfAdaptivAgent {


    private static SelfAdaptivAgent INSTANCE = null;


    public static SelfAdaptivAgent getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new SelfAdaptivAgent();
        }
        return INSTANCE;
    }


    private final Map<Pair<Long, Action>, List<Decision>> decisions = new HashMap<>();

    private final Queue<Decision> adaptingQueue = new ConcurrentLinkedQueue<>();


    public void initialize() {
        List<AdapterInformation> test = AdapterManager.getInstance().getAvailableAdapters( AdapterType.STORE );

        for ( AdapterInformation adapterInformation : test ) {
            log.warn( adapterInformation.name );

        }
    }


    public void addDecision( Pair<Long, Action> key, Decision decision ) {
        if ( this.decisions.containsKey( key ) ) {
            List<Decision> decisionsList = decisions.remove( key );
            decisionsList.add( decision );
            decisions.put( key, decisionsList );
        } else {
            decision.setDecisionStatus( DecisionStatus.CREATED );
            decisions.put( key, Collections.singletonList( decision ) );
        }

    }


    // todo ig: when to add to the queue
    private synchronized void addToQueue( Decision decision ) {
        adaptingQueue.add( decision );
    }


    private boolean checkOldDecisions( Decision decision ) {

        // Check if entity and namespace still exist
        Catalog catalog = Catalog.getInstance();
        if ( !catalog.checkIfExistsTable( decision.getEntityId() ) && !catalog.checkIfExistsSchema( Catalog.defaultDatabaseId, catalog.getSchema( decision.getNameSpaceId() ).name ) ) {
            updateDecisionStatus( decision, DecisionStatus.NOT_APPLICABLE );
            return false;
        }

        switch ( decision.getClauseCategory() ) {
            case STORE:
                List<DataStore> dataStores = (List<DataStore>) WeightedList.weightedToList( decision.getWeightedList() );
                for ( DataStore dataStore : dataStores ) {
                    if ( !catalog.checkIfExistsAdapter( dataStore.getAdapterId() ) ) {
                        updateDecisionStatus( decision, DecisionStatus.NOT_APPLICABLE );
                        return false;
                    }
                }
                break;
            case SELF_ADAPTING:
                // Nothing additional needs to be checked for the category SELF_ADAPTING
                break;
            default:
                log.warn( "Clause Category: " + decision.getClauseCategory() + " is not yet implemented. Please add Clause Category to the methode checkOldDecisions" );
                throw new SelfAdaptiveRuntimeException( "Clause Category: " + decision.getClauseCategory() + " is not yet implemented. Please add Clause Category to the methode checkOldDecisions" );
        }

        return true;
    }


    private void updateDecisionStatus( Decision decision, DecisionStatus decisionStatus ) {
        List<Decision> decisionsList = decisions.remove( decision.getKey() );
        decisionsList.remove( decision );
        decision.setDecisionStatus( decisionStatus );
        decisionsList.add( decision );
    }


    private void rerateDecision( Decision decision ) {

        WeightedList<?> weightedList = PoliciesManager.getInstance().makeDecisionWeighted( decision.getClazz(),
                decision.getAction(),
                decision.getNameSpaceId(),
                decision.getEntityId(),
                decision.getPreSelection() );

        //
        if(isNewDecisionBetter( getOrdered( decision.getWeightedList() ), getOrdered( weightedList ) )){
            decision.getAction().newWeightedList();
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
            Decision decision = adaptingQueue.remove();
            if ( !checkOldDecisions( decision ) ) {
                log.warn( "Decision is not applicable anymore, deleted from queue and marked in decision overview." );
            }
            rerateDecision( decision );

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

}
