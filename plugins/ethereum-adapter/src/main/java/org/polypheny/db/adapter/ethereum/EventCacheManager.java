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

import java.math.BigDecimal;
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
import org.polypheny.db.algebra.logical.relational.LogicalValues;
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
import org.web3j.abi.datatypes.Bool;
import org.web3j.abi.datatypes.Bytes;
import org.web3j.abi.datatypes.DynamicBytes;
import org.web3j.abi.datatypes.Int;
import org.web3j.abi.datatypes.Uint;


@Slf4j
public class EventCacheManager implements Runnable {

    // Singleton instance of EventCacheManager (T)
    private static EventCacheManager INSTANCE = null;

    private final TransactionManager transactionManager;

    // concurrent map, which maintains multiple caches, which correspond to the adapter which requested the caches
    // to allow multiple threads to read and modify; keys: adapterId, value: EventCache (T)
    public Map<Integer, ContractCache> caches = new ConcurrentHashMap<>();


    /**
     * This gets called only once at the start of Polypheny to create a single instance of the manager
     * after that the method will throw and the {@link #getInstance()} method is used to retrieve the initially create instance.
     *
     * @param manager is used to create new transactions, which are required to create new queries.
     */
    public static synchronized void getAndSet( TransactionManager manager ) {
        if ( INSTANCE != null ) {
            throw new RuntimeException( String.format( "The %s was already set.", EventCacheManager.class.getSimpleName() ) );
        }
        INSTANCE = new EventCacheManager( manager );
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


    public ContractCache register( int sourceAdapterId, int targetAdapterId, String clientUrl, int batchSizeInBlocks, BigInteger fromBlock, BigInteger toBlock, Map<String, List<EventData>> eventsPerContract, Map<String, List<ExportedColumn>> columns ) {
        ContractCache cache = new ContractCache( sourceAdapterId, targetAdapterId, clientUrl, batchSizeInBlocks, fromBlock, toBlock, eventsPerContract, columns );
        this.caches.put( sourceAdapterId, cache );
        return cache;
    }


    @Nullable
    public ContractCache getCache( int adapterId ) {
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
                ConstraintInformation primaryConstraint = new ConstraintInformation( table.getKey() + "primary", ConstraintType.PRIMARY, List.of( "log_index", "transaction_index", "block_number" ) );
                DdlManager.getInstance().createTable( namespaceId, table.getKey(), table.getValue(), List.of( primaryConstraint ), false, List.of( store ), PlacementType.AUTOMATIC, false, transaction.createStatement(), false );
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
            return transactionManager.startTransaction( Catalog.defaultDatabaseId, Catalog.defaultUserId, false, "Ethereum Plugin" );
        } catch ( UnknownSchemaException | UnknownDatabaseException | GenericCatalogException | UnknownUserException e ) {
            throw new RuntimeException( e );
        }
    }


    void writeToStore( String tableName, List<List<Object>> logResults, int targetAdapterId ) {
        if ( logResults.isEmpty() ) {
            return;
        }
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();

        AlgBuilder builder = AlgBuilder.create( statement );

        // TableEntry table = transaction.getSchema().getTable( EthereumPlugin.HIDDEN_PREFIX + tableName );
        AlgOptSchema algOptSchema = transaction.getCatalogReader();
        AlgOptTable table = algOptSchema.getTableForMember( Collections.singletonList( Catalog.HIDDEN_PREFIX + tableName ) );

        AlgDataType rowType = table.getTable().getRowType( transaction.getTypeFactory() );
        builder.push( LogicalValues.createOneRow( builder.getCluster() ) );
        builder.project( rowType.getFieldList().stream().map( f -> new RexDynamicParam( f.getType(), f.getIndex() ) ).collect( Collectors.toList() ), rowType.getFieldNames() );
        builder.insert( table );
        // todo DL: we should re-use this for all batches (ignore right now)

        AlgNode node = builder.build(); // Construct the algebraic node
        AlgRoot root = AlgRoot.of( node, Kind.INSERT ); // Wrap the node into an AlgRoot as required by Polypheny

        // Add the dynamic parameters to the context
        int i = 0;
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            long idx = field.getIndex();
            AlgDataType type = field.getType();

            // Extracting the values for the current field from the log results
            List<Object> fieldValues = new ArrayList<>();
            for ( List<Object> logResult : logResults ) {
                Object value = logResult.get( i );
                value = convertValueBasedOnType(value);
                fieldValues.add( value );
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
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }


    protected Map<Integer, CachingStatus> getAllStreamStatus() {
        // return status of process
        return caches.values().stream().collect( Collectors.toMap( c -> c.sourceAdapterId, ContractCache::getStatus ) );
    }

    private Object convertValueBasedOnType(Object value) {
        if (value instanceof Address) {
            return value.toString();
        } else if (value instanceof Bool) {
            return ((Bool) value).getValue();
        } else if (value instanceof DynamicBytes) {
            return ((DynamicBytes) value).getValue().toString();
        } else if (value instanceof Bytes) {
            return value.toString();
        } else if (value instanceof Uint) {   // Similarly for Uint and its subclasses
            BigInteger bigIntValue = ((Uint) value).getValue();
            return bigIntValue == null ? null : new BigDecimal(bigIntValue);
        } else if (value instanceof Int) {    // Similarly for Int and its subclasses
            BigInteger bigIntValue = ((Int) value).getValue();
            return bigIntValue == null ? null : new BigDecimal(bigIntValue);
        }
        return value; // return the original value if none of the conditions match
    }


    @Override
    public void run() {

    }

}