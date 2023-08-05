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
import org.polypheny.db.schema.PolyphenyDbSchema.TableEntry;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.web3j.abi.datatypes.Event;

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


    // Returns the singleton instance; we only want to create exactly one EventCacheManager; Singleton pattern is a Java pattern; EventCacheManager can be called everywhere
    public static EventCacheManager getInstance() {
        if ( INSTANCE == null ) {
            throw new RuntimeException( String.format( "The %s was not correctly initialized.", EventCacheManager.class.getSimpleName() ) );
        }
        return INSTANCE;
    }


    // Transaction Manager: Process of ensuring that database transactions are processed reliably, mainly focusing on the ACID properties (T)
    private EventCacheManager( TransactionManager transactionManager ) {
        this.transactionManager = transactionManager;
    }


    // sourceAdapterId == ethereum adapter (source)
    // targetAdapterId == e.g. hsqldb adapter (store)
    // construct and register a new EventCache for a specific source adapter; in this case the ethereum adapter
    public EventCache register( int sourceAdapterId, int targetAdapterId, String clientUrl, int batchSizeInBlocks, String smartContractAddress, BigInteger fromBlock, BigInteger toBlock, List<Event> events, Map<String, List<ExportedColumn>> map ) {
        EventCache cache = new EventCache( sourceAdapterId, targetAdapterId, clientUrl, batchSizeInBlocks, smartContractAddress, fromBlock, toBlock, events, map );
        this.caches.put( sourceAdapterId, cache );
        return cache;
    }


    // Retrieves the EventCache object associated with the specified adapterId. (T)
    @Nullable
    public EventCache getCache( int adapterId ) {
        return caches.get( adapterId );
    }


    void createTables( int sourceAdapterId, Map<String, List<FieldInformation>> tableInformations, int targetAdapterId ) {
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
            // Question: Where is thsi Catalog coming from, bzw. defaultDatabaseId etc.?
            // Why does the TM need catalog metadata?
            Transaction transaction = transactionManager.startTransaction( Catalog.defaultDatabaseId, Catalog.defaultUserId, false, "Ethereum Plugin" );
            return transaction;
        } catch ( UnknownSchemaException | UnknownDatabaseException | GenericCatalogException | UnknownUserException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * Writes the event(s) caches to a specified table within the store.
     * Meta: This method constructs a relational algebra query, representing
     * the logical query plan, and then translates it into the underlying
     * database's native query language. This translation leverages
     * an internal algebra, common among many databases, to facilitate
     * the conversion process.
     *
     * This approach allows for more efficient reusability, making the execution significantly faster.
     *
     * @param event The event to be written to the store.
     * @param tableName The name of the table where the event should be stored.
     */
    void writeToStore( Event event, String tableName ) {
        // Create a fresh transaction. A transaction can consist of multiple statements,
        // each representing a single SQL command to be executed as part of the transaction.
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement(); // statement is an object used to execute SQL commands; Creating an individual SQL command within the transaction

        // Create an Algebra Builder (AlgBuilder) instance.
        // This is a helper class to simplify the construction of the relational algebra tree representing the SQL query.
        // It abstracts the complexity of building this tree, allowing for the creation of even complex expressions in a more manageable way.
        AlgBuilder builder = AlgBuilder.create( statement );

        // TableEntry table = transaction.getSchema().getTable( EthereumPlugin.HIDDEN_PREFIX + tableName );
        AlgOptSchema algOptSchema = (AlgOptSchema) transaction.getSchema();
        AlgOptTable table = algOptSchema.getTableForMember( Collections.singletonList(EthereumPlugin.HIDDEN_PREFIX + tableName) );


        // In Polypheny, algebra operations are used to represent various SQL operations such as scans, projections, filters, etc.
        // Unlike typical handling of DML (Data Manipulation Language) operations, Polypheny can include these operations as well.
        // An insert operation (normally not in alg operations, but in dml), for example, may be represented as a tree structure with a values algebra operation at the bottom (containing one or more rows),
        // followed by projections if needed, and finally topped with a table modifier operation to signify the insert.
        // This internal representation of modifiers for DML operations allows for a cohesive handling of queries within Polypheny.

        // 'rowType' represents the structure of a row in the table, like columns. For example, one might be 'name', another 'age', etc.
        // It essentially provides the signature of what the row looks like for this particular table.
        AlgDataType rowType = table.getTable().getRowType( transaction.getTypeFactory() );
        builder.values( rowType );

        // we use a project with dynamic parameters, so we can re-use it
        builder.project( rowType.getFieldList().stream().map( f -> new RexDynamicParam( f.getType(), f.getIndex() ) ).collect( Collectors.toList() ) );

        builder.insert( (AlgOptTable) table ); // modifier
        // todo we should re-use this for all batches (ignore right now); David will do this
        // In the current code, values are always newly built. Ideally, we would use dynamic/prepared parameters that can be cached and reused.
        // In Polypheny, we use a special structure: values (with only rowType) -> projection with dynamic parameters (see RegDynamicParam - map every rowType to a dynamic parameter)
        // This allows us to map each rowType to a dynamic parameter. We have theoretically dynamic parameters that we could re-use. We could cache them and not always create it newly.

        AlgNode node = builder.build(); // Construct the algebraic node
        AlgRoot root = AlgRoot.of( node, Kind.INSERT ); // Wrap the node into an AlgRoot as required by Polypheny

        // Add the dynamic parameters to the context
        // This part (above) could be reused, but we still need to create new statements and define what these dynamic parameters are (below)
        // addParameterValues: add all the names (Alex, Joe, Jane, etc.) for index 0, all the ages (rows) for index 1 and so on...
        // TODO: Correctly fill in the dynamic parameters with the correct information from the event (event.getIndexedParameters().get( i++ ).toString())
        int i = 0;
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            statement.getDataContext().addParameterValues( field.getIndex(), field.getType(), List.of( event.getIndexedParameters().get( i++ ).toString() ) ); // take the correct indexedParameters - at the moment we only add one row at a time, could refactor to add the whole batch
        }

        // execute the transaction (query will be executed)
        PolyImplementation implementation = statement.getQueryProcessor().prepareQuery( root, false ); // implements the code basically
        implementation.getRows( statement, -1 ); // Executes the query, with -1 meaning to fill in the whole batch

    }


    private Map<Integer, CachingStatus> getAllStreamStatus() {
        // return status of process
        return caches.values().stream().collect( Collectors.toMap( c -> c.sourceAdapterId, EventCache::getStatus ) );
    }

}