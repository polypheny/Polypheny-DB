/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.monitoring;



import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.monitoring.subscriber.Subscriber;
import org.polypheny.db.monitoring.subscriber.SubscriptionTopic;


/**
 * This class is the heart of the messaging brokerage.
 * It keeps track of all running subscriptions and will inform Subscribers about incoming messages
 */
@Slf4j
public class EventBroker {



    //TODO make subscriber lists persistent
    //Table_ID with ListOfSubscribers
    private Map<Long, Set<Subscriber>> tableSubscription = new HashMap<Long, Set<Subscriber>>();

    //Store_ID with ListOfSubscribers
    private Map<Long,Set<Subscriber>> storeSubscription = new HashMap<Long, Set<Subscriber>>();;


    //Todo remove keys if Stores and tables get deleted.
    // Do this as post step in catalog removal
    // and then end subscription completely
    @Getter
    private Set<Subscriber> allSubscribers = new HashSet<>();

    /**
     * Adds subscription to specific type and id. To get informed about events on that topic
     *
     * @param subscriber    Subscriber to be added to
     * @param objectType    type/topic to subscribe to
     * @param objectId      specific id or _empty_String_ to narrow down messages
     */
    public void addSubscription( Subscriber subscriber, SubscriptionTopic objectType, long objectId ){
        //TODO HENNLO Generalize this more


        //Can be added all the time since we are using a set
        //Its faster than using  list and an if
        allSubscribers.add( subscriber );


        switch ( objectType ){
            case STORE:
                Set<Subscriber> tempStoreSubscription;
                if ( storeSubscription.containsKey( objectId ) ) {
                    tempStoreSubscription = storeSubscription.get( objectId );
                    tempStoreSubscription.add( subscriber );
                }
                else{
                    tempStoreSubscription = new HashSet<>();
                    tempStoreSubscription.add( subscriber );
                }
                storeSubscription.put( objectId, tempStoreSubscription );
                break;

            case TABLE:
                Set<Subscriber> tempTableSubscription;
                if ( tableSubscription.containsKey( objectId ) ) {
                    tempTableSubscription = tableSubscription.get( objectId );
                    tempTableSubscription.add( subscriber );
                }
                else{
                    tempTableSubscription = new HashSet<>();
                    tempTableSubscription.add( subscriber );
                }
                tableSubscription.put( objectId, tempTableSubscription );
                break;

            case ALL:
                throw  new RuntimeException("Not yet implemented");
        }
    }


    /**
     * Removes subscription from specific type and id. To not get informed anymore about events on a specific topic
     *
     * @param subscriber    Subscriber to be added to
     * @param objectType    type/topic to subscribe to
     * @param objectId      specific id or _empty_String_ to narrow down messages
     */
    public void removeSubscription( Subscriber subscriber, SubscriptionTopic objectType, long objectId ){

        //TODO HENNLO Generalize this more // same as in add Subscription
        switch ( objectType ){
            case STORE:
                Set<Subscriber> tempStoreSubscription;
                if ( storeSubscription.containsKey( objectId ) ) {
                    tempStoreSubscription = storeSubscription.get( objectId );
                    tempStoreSubscription.remove( subscriber );
                    storeSubscription.put( objectId, tempStoreSubscription );
                }
                else{
                    log.info( "No active subscription found for Subscriber: " + subscriber + " and " + objectType + " =" + objectId );
                }

                break;

            case TABLE:
                Set<Subscriber> tempTableSubscription;
                if ( tableSubscription.containsKey( objectId ) ) {
                    tempTableSubscription = tableSubscription.get( objectId );
                    tempTableSubscription.remove( subscriber );
                    tableSubscription.put( objectId, tempTableSubscription );
                }
                else{
                    log.info( "No active subscription found for Subscriber: " + subscriber + " and " + objectType + " =" + objectId );
                }
                break;

            case ALL:
                throw  new RuntimeException("Not yet implemented");
        }

        // If this was the last occurence of the Subscriber in any Subscription remove him from ALL list
        if ( !hasActiveSubscription( subscriber ) ){
            allSubscribers.remove( subscriber );
        }
    }



    //INFO @Cedric I think it is useful to do some kind of pre-processing on the event before distributing it to the subscribers
    // I think our first approach (although much leaner) with sending the complete events to Subscribers and letting them decide whether the event is relevant for them
    // would greatly increase the overall load since evry subscriber had to to this, with growing subscribers the load also grows linerarily
    //Therefore i would suggest only sendig necessary events to subscribers
    //Would also be better do implement a real MOM in the future with dedicated topics


    /**
     * Preprocesses the event to retrieve all relevant subscribers
     * Appends each subscriber to single distribution list
     * @param event     Event to be analyzed and send to subscribers
     */
    public void processEvent(MonitorEvent event){

        //distribution list for specificEvent
        Stream<Subscriber> relevantSubscriberStream = Stream.of();
        Set<Subscriber> relevants = new HashSet<>();

        //todo remove test
        //dummy information retrieved from event extraction from processing
        long tableId = 6;
        long storeId = 1;

        //Get all subscribers to be notified about event
        if ( storeSubscription.containsKey( storeId ) ){
            relevantSubscriberStream = Stream.concat( relevantSubscriberStream, storeSubscription.get( storeId ).stream() );
            relevants.addAll( storeSubscription.get( storeId ) );
            System.out.println("STORE SUBS: " + storeSubscription.get( storeId ));
        }

        if ( tableSubscription.containsKey( tableId ) ){
            relevantSubscriberStream = Stream.concat( relevantSubscriberStream, tableSubscription.get( tableId ).stream() );
            relevants.addAll( tableSubscription.get( tableId ) );
            System.out.println("Table SUBS: " + tableSubscription.get( tableId ));
        }

        //process Event
        //and get relevant information

        System.out.println("----->   " + getAllSubscribers());
        System.out.println("----->   " + relevantSubscriberStream.collect( Collectors.toSet()));
        System.out.println("----->   " + relevants);


        //only send DISTINCT relevantSubscribers, therefore make to SET and back to LIST to only deliver events to subscribers once
        //deliverEvent( event, relevantSubscriberStream.collect( Collectors.toSet()).stream().collect( Collectors.toList()) );
//        deliverEvent( event, relevantSubscriberStream.collect( Collectors.toSet()));
        deliverEvent( event, relevants);

    }


    /**
     *  Essentially only delivers the event to relevant nodes
     *
     * @param event                 Events to be delivered
     * @param relevantSubscribers   Subscribers to deliver the event to
     */
    private void deliverEvent(MonitorEvent event, Set<Subscriber> relevantSubscribers){

        for ( Subscriber subscriber : relevantSubscribers ) {
            subscriber.handleEvent( event );
        }
        
    }


    public void removeAllSubscriptions( Subscriber subscriber ) {

        Set<Subscriber> tempStoreSubscription;
        Set<Subscriber> tempTableSubscription;

        //loop through every existing subscriptions and remove the subscriber
        for ( Entry storeSub : storeSubscription.entrySet() ) {
            tempStoreSubscription = storeSubscription.get( storeSub.getKey() );
            if ( tempStoreSubscription.contains( subscriber ) ){
                tempStoreSubscription.remove( subscriber );
                storeSubscription.put( (Long) storeSub.getKey(), tempStoreSubscription );
            }
        }

        for ( Entry tableSub : tableSubscription.entrySet() ) {
            tempTableSubscription = tableSubscription.get( tableSub.getKey() );
            if ( tempTableSubscription.contains( subscriber ) ){
                tempTableSubscription.remove( subscriber );
                storeSubscription.put( (Long) tableSub.getKey(), tempTableSubscription );
            }
        }

        log.info( "Removed all active Subscription from: " + subscriber.getSubscriptionTitle() );
    }


    /**
     * Mainly used as a helper to identify if subscriber has active subscriptions left or can be completely removed from Broker
     * @param subscriber
     * @return if Subscriber ist still registered to events
     */
    private boolean hasActiveSubscription(Subscriber subscriber){

        for ( Entry storeSub : storeSubscription.entrySet() ) {
            if ( storeSubscription.get( storeSub.getKey() ).contains( subscriber ) ){
                return true;
            }
        }

        for ( Entry tableSub : tableSubscription.entrySet() ) {
            if ( tableSubscription.get( tableSub.getKey() ).contains( subscriber ) ){
                return true;
            }
        }

        return false;
    }
}
