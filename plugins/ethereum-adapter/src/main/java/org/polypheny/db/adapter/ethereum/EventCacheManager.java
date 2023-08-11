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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource.ExportedColumn;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
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
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.DdlManager.FieldInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.generated.Uint256;


@Slf4j
public class EventCacheManager {

    // Singleton instance of EventCacheManager (T)
    private static EventCacheManager INSTANCE = null;

    private final TransactionManager transactionManager;

    // concurrent map, which maintains multiple caches, which correspond to the adapter which requested the caches
    // to allow multiple threads to read and modify; keys: adapterId, value: EventCache (T)
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


    private EventCacheManager( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    public EventCache register( int sourceAdapterId, int targetAdapterId, String clientUrl, int batchSizeInBlocks, String smartContractAddress, BigInteger fromBlock, BigInteger toBlock, List<Event> events, Map<String, List<ExportedColumn>> map ) {
        EventCache cache = new EventCache( sourceAdapterId, targetAdapterId, clientUrl, batchSizeInBlocks, smartContractAddress, fromBlock, toBlock, events, map );
        this.caches.put( sourceAdapterId, cache );
        return cache;
    }


    @Nullable
    public EventCache getCache( int adapterId ) {
        return caches.get( adapterId );
    }


    void createTables( int sourceAdapterId, Map<String, List<FieldInformation>> tableInformations, int targetAdapterId ) {
        log.warn( "start to create tables" );
        try {
            long namespaceId = Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, "public" ).id; // get the default schema
            Transaction transaction = getTransaction(); // get the transaction
            DataStore store = AdapterManager.getInstance().getStore( targetAdapterId ); // get the target store from the adapater

            // For each table, a new table is created with their constraint (e.g., a primary key).
            for ( Entry<String, List<FieldInformation>> table : tableInformations.entrySet() ) {
                ConstraintInformation primaryConstraint = new ConstraintInformation( table.getKey() + "primary", ConstraintType.PRIMARY, List.of( table.getValue().get( 0 ).name ) ); // todo atm first column is primary, we should adjust that
                DdlManager.getInstance().createTable( namespaceId, table.getKey(), table.getValue(), List.of( primaryConstraint ), false, List.of( store ), PlacementType.AUTOMATIC, transaction.createStatement() );
            }

            try {
                transaction.commit();
            } catch ( TransactionException e ) {
                throw new RuntimeException( e );
            }
        } catch ( EntityAlreadyExistsException | ColumnNotExistsException | UnknownPartitionTypeException | UnknownColumnException | PartitionGroupNamesNotUniqueException | UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
    }


    private Transaction getTransaction() {
        try {
            Transaction transaction = transactionManager.startTransaction( Catalog.defaultDatabaseId, Catalog.defaultUserId, false, "Ethereum Plugin" );
            return transaction;
        } catch ( UnknownSchemaException | UnknownDatabaseException | GenericCatalogException | UnknownUserException e ) {
            throw new RuntimeException( e );
        }
    }


    void writeToStore( String tableName, List<List<Object>> logResults ) {
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();

        AlgBuilder builder = AlgBuilder.create( statement );

        // TableEntry table = transaction.getSchema().getTable( EthereumPlugin.HIDDEN_PREFIX + tableName );
        AlgOptSchema algOptSchema = transaction.getCatalogReader();
        AlgOptTable table = algOptSchema.getTableForMember( Collections.singletonList( EthereumPlugin.HIDDEN_PREFIX + tableName ) );

        AlgDataType rowType = table.getTable().getRowType( transaction.getTypeFactory() );
        builder.values( rowType );
        builder.project( rowType.getFieldList().stream().map( f -> new RexDynamicParam( f.getType(), f.getIndex() ) ).collect( Collectors.toList() ) );
        builder.insert( (AlgOptTable) table );
        // TODO: we should re-use this for all batches (ignore right now); David will do this

        AlgNode node = builder.build(); // Construct the algebraic node
        AlgRoot root = AlgRoot.of( node, Kind.INSERT ); // Wrap the node into an AlgRoot as required by Polypheny

        // Add the dynamic parameters to the context
        // TODO: Correctly fill in the dynamic parameters with the correct information from the event (event.getIndexedParameters().get( i++ ).toString())
        int i = 0;
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            long idx = field.getIndex();
            AlgDataType type = field.getType();

            // Extracting the values for the current field from the log results
            List<Object> fieldValues = new ArrayList<>();
            for ( List<Object> logResult : logResults ) {
                Object value = logResult.get( i );
                Object processedValue;
                // temporarily
                if ( value instanceof Address ) {
                    processedValue = ((Address) value).toString();
                } else if ( value instanceof Uint256 ) {
                    processedValue = ((Uint256) value).getValue();
                } else if ( value instanceof BigInteger ) {
                    processedValue = value; // Already a BigInteger
                } else if ( value instanceof Boolean ) {
                    processedValue = value; // No need to convert boolean
                } else {
                    processedValue = value.toString(); // handle other types as needed
                }
                fieldValues.add( processedValue );
            }
            i++;
            statement.getDataContext().addParameterValues( idx, type, fieldValues ); // take the correct indexedParameters - at the moment we only add one row at a time, could refactor to add the whole batch
        }

        log.warn( "write to store before; table name: " + tableName );
        // execute the transaction (query will be executed)
        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( root, false ); // implements the code basically
        log.warn( "write to store after; table name: " + tableName );
        implementation.getRows( statement, -1 ); // Executes the query, with -1 meaning to fill in the whole batch
        log.warn( "finish write to store for table: " + tableName );
    }


    private Map<Integer, CachingStatus> getAllStreamStatus() {
        // return status of process
        return caches.values().stream().collect( Collectors.toMap( c -> c.sourceAdapterId, EventCache::getStatus ) );
    }

}