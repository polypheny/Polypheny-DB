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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.pf4j.Extension;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingBoolean;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogAdapter;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownAdapterException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.web3j.abi.datatypes.Event;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;

@Slf4j
@Extension
@AdapterProperties(
        name = "Ethereum",
        description = "An adapter for querying the Ethereum blockchain. It uses the ethereum JSON-RPC API. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.REMOTE)
@AdapterSettingString(name = "ClientUrl", description = "The URL of the ethereum JSON RPC client", defaultValue = "https://mainnet.infura.io/v3/4d06589e97064040b5da99cf4051ef04", position = 1)
@AdapterSettingInteger(name = "Blocks", description = "The number of Blocks to fetch when processing a query", defaultValue = 10, position = 2, modifiable = true)
@AdapterSettingBoolean(name = "ExperimentalFiltering", description = "Experimentally filter Past Block", defaultValue = false, position = 3, modifiable = true)
@AdapterSettingBoolean(name = "EventDataRetrieval", description = "Enables or disables the retrieval of event data. When set to true, all subsequent adapter settings will be taken into account.", defaultValue = true, position = 4, modifiable = true)
@AdapterSettingString(name = "SmartContractAddresses", description = "Comma sepretaed addresses of the smart contracts", defaultValue = "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984, 0x6b175474e89094c44da98b954eedeac495271d0f", position = 5, modifiable = true) // Event Data: Add annotation
@AdapterSettingString(name = "EtherscanApiKey", description = "Etherscan API Token", defaultValue = "PJBVZ3BE1AI5AKIMXGK1HNC59PCDH7CQSP", position = 6, modifiable = true) // Event Data: Add annotation
@AdapterSettingString(name = "fromBlock", description = "Fetch block from (Smart Contract)", defaultValue = "17669045", position = 7, modifiable = true)
@AdapterSettingString(name = "toBlock", description = "Fetch block to (Smart Contract)", defaultValue = "17669155", position = 8, modifiable = true)
@AdapterSettingBoolean(name = "Caching", description = "Cache event data", defaultValue = true, position = 9, modifiable = true)
@AdapterSettingInteger(name = "batchSizeInBlocks", description = "Batch size for caching in blocks", defaultValue = 50, position = 10, modifiable = true)
@AdapterSettingString(name = "CachingAdapterTargetName", description = "Adapter Target Name", defaultValue = "hsqldb", position = 11, modifiable = true)
@AdapterSettingBoolean(name = "UseManualABI", description = "Cache event data", defaultValue = false, position = 12, modifiable = true)
@AdapterSettingString(name = "ContractABI", description = "Contract ABI", defaultValue = "", position = 13, modifiable = true, required = false)
@AdapterSettingString(name = "ContractName", description = "Contract name", defaultValue = "", position = 14, modifiable = true, required = false)
public class EthereumDataSource extends DataSource {

    public static final String SCHEMA_NAME = "public";
    @Getter
    private final boolean eventDataRetrieval;
    private String clientURL;
    @Getter
    private int blocks;
    @Getter
    private boolean experimentalFiltering;
    private EthereumSchema currentSchema;
    @Getter
    final List<String> smartContractAddresses;
    private final String etherscanApiKey;
    @Getter
    private final BigInteger fromBlock;
    @Getter
    private final BigInteger toBlock;
    private final int batchSizeInBlocks;

    private final Map<String, EventData> eventDataMap;
    private Boolean caching;
    private String cachingAdapterTargetName;

    private Map<String, List<ExportedColumn>> map;

    private final boolean useManualABI;
    private final String contractABI;
    private final String contractName;


    public EthereumDataSource( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );
        this.canCache = true;
        setClientURL( settings.get( "ClientUrl" ) );
        this.blocks = Integer.parseInt( settings.get( "Blocks" ) );
        this.experimentalFiltering = Boolean.parseBoolean( settings.get( "ExperimentalFiltering" ) );
        this.eventDataRetrieval = Boolean.parseBoolean( settings.get( "EventDataRetrieval" ) );
        String smartContractAddressesStr = settings.get( "SmartContractAddresses" );
        this.smartContractAddresses = Arrays.stream( smartContractAddressesStr.split( "," ) )
                .map( String::trim )
                .collect( Collectors.toList() );
        this.etherscanApiKey = settings.get( "EtherscanApiKey" );
        this.fromBlock = new BigInteger( settings.get( "fromBlock" ) );
        this.toBlock = new BigInteger( settings.get( "toBlock" ) );
        this.batchSizeInBlocks = Integer.parseInt( settings.get( "batchSizeInBlocks" ) );
        this.eventDataMap = new HashMap<>();
        this.caching = Boolean.parseBoolean( settings.get( "Caching" ) );
        this.cachingAdapterTargetName = settings.get( "CachingAdapterTargetName" );
        this.useManualABI = Boolean.parseBoolean( settings.get( "UseManualABI" ) );
        this.contractABI = settings.get( "ContractABI" );
        this.contractName = settings.get( "ContractName" );
        // todo DL
        new Thread( () -> {
            createInformationPage();
            enableInformationPage();
        } ).start();

        //createInformationPage();
        //enableInformationPage();
    }


    private void setClientURL( String clientURL ) {
        Web3j web3j = Web3j.build( new HttpService( clientURL ) );
        try {
            BigInteger latest = web3j.ethBlockNumber().send().getBlockNumber();
        } catch ( Exception e ) {
            throw new RuntimeException( "Unable to connect the client URL '" + clientURL + "'" );
        }
        web3j.shutdown();
        this.clientURL = clientURL;
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new EthereumSchema( this.clientURL );
    }


    @Override
    public Table createTableSchema( CatalogTable combinedTable, List<CatalogColumnPlacement> columnPlacementsOnStore, CatalogPartitionPlacement partitionPlacement ) {
        return currentSchema.createBlockchainTable( combinedTable, columnPlacementsOnStore, this );
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "Blockchain adapter does not support truncate" );
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        log.warn( "getExportedColumn" );
        // Ensure that this block of code is called only once by checking if 'map' is null before proceeding
        if ( map != null ) {
            return map;
        }

        Map<String, List<ExportedColumn>> map = new HashMap<>();

        String[] blockColumns = { "number", "hash", "parent_hash", "nonce", "sha3uncles", "logs_bloom", "transactions_root", "state_root", "receipts_root", "author", "miner", "mix_hash", "difficulty", "total_difficulty", "extra_data", "size", "gas_limit", "gas_used", "timestamp" };
        PolyType[] blockTypes = { PolyType.DECIMAL, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.DECIMAL, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.VARCHAR, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.TIMESTAMP };
        createExportedColumns( "block", map, blockColumns, blockTypes );

        String[] transactionColumns = { "hash", "nonce", "block_hash", "block_number", "transaction_index", "from", "to", "value", "gas_price", "gas", "input", "creates", "public_key", "raw", "r", "s" };
        PolyType[] transactionTypes = { PolyType.VARCHAR, PolyType.DECIMAL, PolyType.VARCHAR, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR };
        createExportedColumns( "transaction", map, transactionColumns, transactionTypes );

        if ( !eventDataRetrieval ) {
            this.map = map;
            return map;
        }

        String[] commonEventColumns = { "removed", "log_index", "transaction_index", "transaction_hash", "block_hash", "block_number", "address" };
        PolyType[] commonEventTypes = { PolyType.BOOLEAN, PolyType.DECIMAL, PolyType.DECIMAL, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.DECIMAL, PolyType.VARCHAR };
        createExportedColumnsForEvents( map, commonEventColumns, commonEventTypes );

        if ( caching == Boolean.TRUE ) {
            // Disable caching to prevent multiple unnecessary attempts to cache the same data.
            caching = false;
            this.map = map;
            Map<String, List<ExportedColumn>> columns = new HashMap<>( map ); // create new map instance for caching
            columns.remove( "block" );
            columns.remove( "transaction" );

            // todo DL: fix concurrency issues (dirty solution right now)
            new Thread( () -> {
                try {
                    Thread.sleep( 1200 );
                } catch ( InterruptedException e ) {
                    throw new RuntimeException( e );
                }
                try {
                    Map<String, List<EventData>> eventsPerContract = eventDataMap.values().stream()
                            .collect( Collectors.groupingBy(
                                    EventData::getSmartContractAddress,
                                    Collectors.toList()
                            ) );
                    CatalogAdapter cachingAdapter = Catalog.getInstance().getAdapter( cachingAdapterTargetName );
                    EventCacheManager.getInstance()
                            .register( getAdapterId(), cachingAdapter.id, clientURL, batchSizeInBlocks, fromBlock, toBlock, eventsPerContract, columns )
                            .initializeCaching();
                } catch ( UnknownAdapterException e ) {
                    // If the specified adapter is not found, throw a RuntimeException
                    throw new RuntimeException( e );
                }
            } ).start();
        }

        return map;
    }


    @Override
    public boolean prepare( PolyXid xid ) {
        log.debug( "Blockchain Store does not support prepare()." );
        return true;
    }


    @Override
    public void commit( PolyXid xid ) {
        log.debug( "Blockchain Store does not support commit()." );
    }


    @Override
    public void rollback( PolyXid xid ) {
        log.debug( "Blockchain Store does not support rollback()." );
    }


    @Override
    public void shutdown() {
        removeInformationPage();
    }


    @Override
    protected void reloadSettings( List<String> updatedSettings ) {
        if ( updatedSettings.contains( "ClientUrl" ) ) {
            setClientURL( settings.get( "ClientUrl" ) );
        }
        if ( updatedSettings.contains( "Blocks" ) ) {
            this.blocks = Integer.parseInt( settings.get( "Blocks" ) );
        }
        if ( updatedSettings.contains( "ExperimentalFiltering" ) ) {
            this.experimentalFiltering = Boolean.parseBoolean( settings.get( "ExperimentalFiltering" ) );
        }
    }


    protected void createInformationPage() {
        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup(
                    informationPage,
                    entry.getValue().get( 0 ).physicalSchemaName + "." + entry.getValue().get( 0 ).physicalTableName );

            InformationTable table = new InformationTable(
                    group,
                    Arrays.asList( "Position", "Column Name", "Type", "Primary" ) );
            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                table.addRow(
                        exportedColumn.physicalPosition,
                        exportedColumn.name,
                        exportedColumn.getDisplayType(),
                        exportedColumn.primary ? "✔" : ""
                );
            }
            informationElements.add( table );
            informationGroups.add( group );
        }
    }


    private void createExportedColumns( String physicalTableName, Map<String, List<ExportedColumn>> map, String[] columns, PolyType[] types ) {
        PolyType collectionsType = null;
        Integer length = 300;
        Integer scale = null;
        Integer dimension = null;
        Integer cardinality = null;
        int position = 0;
        List<ExportedColumn> cols = new ArrayList<>();
        for ( String col : columns ) {
            cols.add( new ExportedColumn(
                    col,
                    types[position],
                    collectionsType,
                    length,
                    scale,
                    dimension,
                    cardinality,
                    false,
                    SCHEMA_NAME,
                    physicalTableName,
                    col,
                    position,
                    position == 0 ) );
            position++;

        }
        map.put( physicalTableName, cols );
    }


    private void createExportedColumnsForEvents( Map<String, List<ExportedColumn>> map, String[] commonEventColumns, PolyType[] commonEventTypes ) {
        for ( String address : smartContractAddresses ) {
            String contractName = null;
            List<JSONObject> contractEvents = null;
            if ( useManualABI && !contractABI.isEmpty() && !this.contractName.isEmpty() ) {
                if ( smartContractAddresses.size() > 1 ) {
                    throw new IllegalArgumentException( "Only one smart contract address should be provided when using a manual ABI." );
                }
                JSONArray abiArray = new JSONArray( contractABI );
                contractEvents = getEventsFromABIArray( abiArray );
                contractName = this.contractName;
            } else {
                try {
                    contractName = callWithExponentialBackoff( () -> getContractName( address ) );
                    contractEvents = callWithExponentialBackoff( () -> getEventsFromABI( etherscanApiKey, address ) );
                } catch ( Exception e ) {
                    throw new RuntimeException( e );
                }
            }

            for ( JSONObject event : contractEvents ) {
                if ( event.getBoolean( "anonymous" ) ) {
                    continue;
                }
                String eventName = event.getString( "name" ); // to match it later with catalogTable.name
                String compositeKey = contractName + "_" + eventName; // e.g. Uni_Transfer & Dai_Transfer
                JSONArray abiInputs = event.getJSONArray( "inputs" ); // indexed and non-indexed values (topics + data)

                eventDataMap.put( compositeKey.toLowerCase(), new EventData( eventName, contractName, address, abiInputs ) );
            }
        }

        PolyType collectionsType = null;
        Integer scale = null;
        Integer dimension = null;
        Integer cardinality = null;

        // Event Data: Creating columns for each event for specified smart contract based on ABI
        for ( Map.Entry<String, EventData> eventEntry : eventDataMap.entrySet() ) {
            // String eventName = eventEntry.getValue().getOriginalKey(); // Get the original event name
            String compositeEventName = eventEntry.getValue().getCompositeName();
            JSONArray abiInputs = eventEntry.getValue().getAbiInputs(); // Get the data
            List<ExportedColumn> eventDataCols = new ArrayList<>();
            int inputPosition = 0;

            for ( int i = 0; i < abiInputs.length(); i++ ) {
                JSONObject inputObject = abiInputs.getJSONObject( i );
                String col = inputObject.getString( "name" );
                PolyType type = convertToPolyType( inputObject.getString( "type" ) ); // convert event types to polytype
                eventDataCols.add( new ExportedColumn(
                        col,
                        type,
                        collectionsType,
                        getLengthForType( type ),
                        scale,
                        dimension,
                        cardinality,
                        false,
                        SCHEMA_NAME,
                        compositeEventName, // event name
                        col,
                        inputPosition,
                        inputPosition == 0
                ) );
                inputPosition++;
            }

            // Adding common columns
            for ( int i = 0; i < commonEventColumns.length; i++ ) {
                String columnName = commonEventColumns[i];
                PolyType columnType = commonEventTypes[i];
                eventDataCols.add( new ExportedColumn(
                        columnName,
                        columnType,
                        collectionsType,
                        getLengthForType( columnType ),
                        scale,
                        dimension,
                        cardinality,
                        false,
                        SCHEMA_NAME,
                        compositeEventName, // event name
                        columnName,
                        inputPosition,
                        inputPosition == 0
                ) );
                inputPosition++;
            }

            map.put( compositeEventName, eventDataCols );
        }

    }


    protected List<JSONObject> getEventsFromABI( String etherscanApiKey, String contractAddress ) {
        List<JSONObject> events = new ArrayList<>();
        try {
            URL url = new URL( "https://api.etherscan.io/api?module=contract&action=getabi&address=" + contractAddress + "&apikey=" + etherscanApiKey );
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod( "GET" );
            int responseCode = connection.getResponseCode();
            if ( responseCode == HttpURLConnection.HTTP_OK ) {
                BufferedReader in = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ( (inputLine = in.readLine()) != null ) {
                    response.append( inputLine );
                }
                in.close();

                JSONObject jsonObject = new JSONObject( response.toString() );
                String apiStatus = jsonObject.getString( "status" );

                if ( "0".equals( apiStatus ) ) {
                    String errorMessage = jsonObject.getString( "message" );
                    throw new RuntimeException( "Etherscan API error getting abi from contract: " + errorMessage );
                }

                String abi = jsonObject.getString( "result" );
                JSONArray abiArray = new JSONArray( abi ); // Convert ABI string to JSON Array
                for ( int i = 0; i < abiArray.length(); i++ ) {
                    JSONObject obj = abiArray.getJSONObject( i );
                    // Check if the current object is an event
                    if ( obj.getString( "type" ).equals( "event" ) ) {
                        events.add( obj );
                    }
                }
            }

        } catch ( IOException e ) {
            throw new RuntimeException( "Network or IO error occurred", e );
        }

        return events;
    }


    protected List<JSONObject> getEventsFromABIArray( JSONArray abiArray ) {
        List<JSONObject> events = new ArrayList<>();

        // Loop through the ABI
        for ( int i = 0; i < abiArray.length(); i++ ) {
            JSONObject item = abiArray.getJSONObject( i );

            // Check if the item is of type 'event'
            if ( item.has( "type" ) && "event".equals( item.getString( "type" ) ) ) {
                events.add( item );
            }
        }

        return events;
    }


    private String getContractName( String contractAddress ) {
        try {
            URL url = new URL( "https://api.etherscan.io/api?module=contract&action=getsourcecode&address=" + contractAddress + "&apikey=" + etherscanApiKey );
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod( "GET" );
            int responseCode = connection.getResponseCode();
            if ( responseCode == HttpURLConnection.HTTP_OK ) {
                BufferedReader in = new BufferedReader( new InputStreamReader( connection.getInputStream() ) );
                String inputLine;
                StringBuilder response = new StringBuilder();

                while ( (inputLine = in.readLine()) != null ) {
                    response.append( inputLine );
                }
                in.close();

                JSONObject jsonObject = new JSONObject( response.toString() );
                String apiStatus = jsonObject.getString( "status" );

                if ( "0".equals( apiStatus ) ) {
                    String errorMessage = jsonObject.getString( "message" );
                    throw new RuntimeException( "Etherscan API error getting contract name: " + errorMessage );
                }

                JSONArray resultArray = jsonObject.getJSONArray( "result" ); // Get result array
                if ( resultArray.length() > 0 ) {
                    JSONObject contractObject = resultArray.getJSONObject( 0 ); // Get the first object in result array
                    return contractObject.getString( "ContractName" ); // Return ContractName field
                }
            }

        } catch ( IOException e ) {
            throw new RuntimeException( "Network or IO error occurred", e );
        }
        return null;
    }


    protected Event getEventFromCatalogTable( String catalogTableName ) {
        if ( catalogTableName.equals( "block" ) || catalogTableName.equals( "transaction" ) ) {
            return null;
        }
        return eventDataMap.get( catalogTableName ).getEvent();
    }


    protected String getSmartContractAddressFromCatalogTable( String catalogTableName ) {
        if ( catalogTableName.equals( "block" ) || catalogTableName.equals( "transaction" ) ) {
            return null;
        }
        return eventDataMap.get( catalogTableName ).getSmartContractAddress();

    }


    private Integer getLengthForType( PolyType type ) {
        switch ( type ) {
            case VARCHAR:
                return 300;
            case VARBINARY:
                return 32;
            case DECIMAL:
                return 100;
            default:
                return null;
        }
    }


    static PolyType convertToPolyType( String type ) {
        if ( type.equals( "bool" ) ) {
            return PolyType.BOOLEAN;
        } else if ( type.equals( "address" ) || type.equals( "string" ) ) {
            return PolyType.VARCHAR;
        } else if ( type.startsWith( "int" ) || type.startsWith( "uint" ) ) {
            return PolyType.DECIMAL;
        } else if ( type.equals( "bytes" ) || type.startsWith( "bytes" ) ) {
            return PolyType.VARCHAR; // for dynamic and fixed-size
        }
        throw new RuntimeException( "Could not find a matching PolyType" );
    }


    public <T> T callWithExponentialBackoff( Callable<T> callable ) throws Exception {
        int maxRetries = 5;
        long waitTime = 1000; // 1 second

        for ( int retry = 0; retry < maxRetries; retry++ ) {
            try {
                return callable.call();
            } catch ( Exception e ) {
                if ( retry == maxRetries - 1 ) {
                    throw e; // If this was our last retry, rethrow the exception
                }
                try {
                    Thread.sleep( waitTime );
                } catch ( InterruptedException ie ) {
                    Thread.currentThread().interrupt(); // Restore the interrupted status
                }
                waitTime *= 2; // Double the delay for the next retry
            }
        }
        throw new Exception( "Exponential backoff failed after " + maxRetries + " attempts." );
    }


}
