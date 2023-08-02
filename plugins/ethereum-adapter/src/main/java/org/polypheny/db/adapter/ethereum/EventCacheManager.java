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

package org.polypheny.db.adapter.ethereum;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.web3j.abi.datatypes.Event;

@Slf4j
public class EventCacheManager {

    private static EventCacheManager INSTANCE = null;

    private final TransactionManager transactionManager;

    // concurrent map, which maintains multiple caches, which correspond to the adapter which requested the caches
    public Map<Integer, EventCache> caches = new ConcurrentHashMap<>();


    /**
     * This gets called only once at the start of Polypheny to create a single instance of the manager
     * after that the method will throw and the {@link #getInstance()} method is used to retrieve the initially create instance.
     *
     * @param manager is used to create new transactions, which are required to create new queries.
     */
    public static synchronized EventCacheManager getAndSet( TransactionManager manager ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( String.format( "The %s was already set.", EventCacheManager.class.getSimpleName() ) );
        }
        INSTANCE = new EventCacheManager( manager );
        return INSTANCE;
    }


    public static EventCacheManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( String.format( "The %s was not correctly initialized.", EventCacheManager.class.getSimpleName() ) );
        }
        return INSTANCE;
    }


    // Create one instance to handle caching (better for load balancing if we have multiple stores)
    // EventCacheManager is addressed by the Adapter (with registry method)
    // get all the information: adapterId (adapter target name?), threshold, smart contract address, etherscan api key... all the necessary information
    private EventCacheManager( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    public EventCache register( int adapterId, String clientUrl, int batchSizeInBlocks, String smartContractAddress, BigInteger fromBlock, BigInteger toBlock, List<Event> events, Map<String, List<ExportedColumn>> map ) {
        EventCache cache = new EventCache( adapterId, clientUrl, batchSizeInBlocks, smartContractAddress, fromBlock, toBlock, events, map );
        this.caches.put( adapterId, cache );
        return cache;
    }


    @Nullable
    public EventCache getCache( int adapterId ) {
        return caches.get( adapterId );
    }


    void createTables( int sourceAdapterId, Map<String, List<FieldInformation>> tableInformations, int adapterId ){
        try {
            long namespaceId = Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, "public" ).id;
            Transaction transaction = transactionManager.startTransaction( Catalog.defaultDatabaseId, Catalog.defaultUserId, false, "Ethereum Plugin" );
            DataStore store = AdapterManager.getInstance().getStore(adapterId);
            for ( Entry<String, List<FieldInformation>> table : tableInformations.entrySet() ) {
                ConstraintInformation primaryConstraint = new ConstraintInformation( table.getKey()+"primary", ConstraintType.PRIMARY, List.of( table.getValue().get( 0 ).name ) ); // todo atm first column is primary, we should adjust that
                DdlManager.getInstance().createTable( namespaceId, table.getKey(), table.getValue(), List.of(primaryConstraint), false, List.of(store), PlacementType.AUTOMATIC, transaction.createStatement() );
            }

            try {
                transaction.commit();
            } catch ( TransactionException e ) {
                throw new RuntimeException( e );
            }
        } catch ( EntityAlreadyExistsException | ColumnNotExistsException | UnknownPartitionTypeException | UnknownColumnException | PartitionGroupNamesNotUniqueException | UnknownSchemaException | UnknownDatabaseException | GenericCatalogException | UnknownUserException e ) {
            throw new RuntimeException( e );
        }

    }


    private Map<Integer, CachingStatus> getAllStreamStatus() {
        // return status of process
        return caches.values().stream().collect( Collectors.toMap( c -> c.adapterId, EventCache::getStatus ) );
    }

}
