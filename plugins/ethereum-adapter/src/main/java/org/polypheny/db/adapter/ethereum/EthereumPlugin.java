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


import com.google.common.collect.ImmutableMap;
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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.pf4j.Extension;
import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingBoolean;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.Adapter;
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
import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Address;
import org.web3j.abi.datatypes.Event;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;


public class EthereumPlugin extends Plugin {


    public static final String ADAPTER_NAME = "ETHEREUM";

    public static final String HIDDEN_PREFIX = "$hidden$";


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public EthereumPlugin( PluginWrapper wrapper ) {
        super( wrapper );
    }


    @Override
    public void start() {
        Map<String, String> settings = ImmutableMap.of(
                "ClientUrl", "https://mainnet.infura.io/v3/4d06589e97064040b5da99cf4051ef04",
                "Blocks", "10",
                "ExperimentalFiltering", "false"
        );

        Adapter.addAdapter( EthereumDataSource.class, ADAPTER_NAME, settings );
    }


    @Override
    public void stop() {
        Adapter.removeAdapter( EthereumDataSource.class, ADAPTER_NAME );
    }


    @Slf4j
    @Extension
    @AdapterProperties(
            name = "Ethereum",
            description = "An adapter for querying the Ethereum blockchain. It uses the ethereum JSON-RPC API. Currently, this adapter only supports read operations.",
            usedModes = DeployMode.REMOTE)
    @AdapterSettingString(name = "ClientUrl", description = "The URL of the ethereum JSON RPC client", defaultValue = "https://mainnet.infura.io/v3/4d06589e97064040b5da99cf4051ef04", position = 1)
    @AdapterSettingInteger(name = "Blocks", description = "The number of Blocks to fetch when processing a query", defaultValue = 10, position = 2, modifiable = true)
    @AdapterSettingBoolean(name = "ExperimentalFiltering", description = "Experimentally filter Past Block", defaultValue = false, position = 3, modifiable = true)
    @AdapterSettingString(name = "SmartContractAddress", description = "Address of the smart contract address", defaultValue = "0x1f9840a85d5aF5bf1D1762F925BDADdC4201F984", position = 4, modifiable = true) // Event Data: Add annotation
    @AdapterSettingString(name = "EtherscanApiKey", description = "Etherscan API Token", defaultValue = "PJBVZ3BE1AI5AKIMXGK1HNC59PCDH7CQSP", position = 5, modifiable = true) // Event Data: Add annotation
    @AdapterSettingString(name = "fromBlock", description = "Fetch block from (Smart Contract)", defaultValue = "17669045", position = 6, modifiable = true)
    @AdapterSettingString(name = "toBlock", description = "Fetch block to (Smart Contract)", defaultValue = "17669155", position = 7, modifiable = true)
    @AdapterSettingBoolean(name = "Caching", description = "Cache event data", defaultValue = true, position = 8, modifiable = true)
    @AdapterSettingString(name = "AdapterTargetName", description = "Adapter Target Name", defaultValue = "ethereum", position = 6, modifiable = true)
    public static class EthereumDataSource extends DataSource {

        public static final String SCHEMA_NAME = "public";
        private String clientURL;
        @Getter
        private int blocks;
        @Getter
        private boolean experimentalFiltering;
        private EthereumSchema currentSchema;
        private final String smartContractAddress;
        private final String etherscanApiKey;
        private final BigInteger fromBlock;
        private final BigInteger toBlock;
        private final Map<String, EventData> eventInputsMap;
        private Boolean startCaching;
        private String adpaterTargetName;
        @Getter
        List<Event> events = new ArrayList<>(); // for caching


        public EthereumDataSource( final int storeId, final String uniqueName, final Map<String, String> settings ) {
            super( storeId, uniqueName, settings, true );
            setClientURL( settings.get( "ClientUrl" ) );
            this.blocks = Integer.parseInt( settings.get( "Blocks" ) );
            this.experimentalFiltering = Boolean.parseBoolean( settings.get( "ExperimentalFiltering" ) );
            this.smartContractAddress = settings.get( "SmartContractAddress" ); // Event Data; Add smartContractAddress to EDataSource
            this.etherscanApiKey = settings.get( "EtherscanApiKey" );
            this.fromBlock = new BigInteger( settings.get( "fromBlock" ) );
            this.toBlock = new BigInteger( settings.get( "toBlock" ) );
            this.eventInputsMap = new HashMap<>();
            this.startCaching = Boolean.parseBoolean( settings.get( "Caching" ) );
            this.adpaterTargetName = settings.get( "AdapterTargetName" );
            createInformationPage();
            enableInformationPage();
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
            Map<String, List<ExportedColumn>> map = new HashMap<>();
            String[] blockColumns = { "number", "hash", "parent_hash", "nonce", "sha3uncles", "logs_bloom", "transactions_root", "state_root", "receipts_root", "author", "miner", "mix_hash", "difficulty", "total_difficulty", "extra_data", "size", "gas_limit", "gas_used", "timestamp" };
            PolyType[] blockTypes = { PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.BIGINT, PolyType.TIMESTAMP };
            String[] transactionColumns = { "hash", "nonce", "block_hash", "block_number", "transaction_index", "from", "to", "value", "gas_price", "gas", "input", "creates", "public_key", "raw", "r", "s" };
            PolyType[] transactionTypes = { PolyType.VARCHAR, PolyType.BIGINT, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR };

            String[] commonEventColumns = { "removed", "log_index", "transaction_index", "transaction_hash", "block_hash", "block_number", "address" };
            PolyType[] commonEventTypes = { PolyType.BOOLEAN, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.VARCHAR };

            // Caching: Init own caching class. Start caching with "startCaching (idAdapter, smartContractAddress, wo gecached (in welchen Store))".
            // In the background (another Thread) logs are fetched (also see restrictions eth_logs)
            // As usual: Schema is still created here.
            // Caching: We can define a threshold in which part of the data is inserted into the tables. (use flag for this)

            // Event Data Dynamic Scheme
            List<JSONObject> eventList = getEventsFromABI( etherscanApiKey, smartContractAddress );
            eventInputsMap.clear(); // clear the map
            events.clear(); // clear the map
            for ( JSONObject event : eventList ) {
                String eventName = event.getString( "name" ); // to match it later with catalogTable.name
                JSONArray inputsArray = event.getJSONArray( "inputs" );
                List<JSONObject> inputsList = new ArrayList<>();
                List<TypeReference<?>> eventParameters = new ArrayList<>();
                for ( int i = 0; i < inputsArray.length(); i++ ) {
                    JSONObject inputObject = inputsArray.getJSONObject( i );
                    inputsList.add( inputObject );
                    // put this into a method (modular)
                    String type = inputObject.getString( "type" );
                    boolean indexed = inputObject.getBoolean( "indexed" );
                    if ( type.equals( "address" ) ) {
                        eventParameters.add( indexed ? new TypeReference<Address>( true ) {
                        } : new TypeReference<Address>( false ) {
                        } );
                    } else if ( type.equals( "uint256" ) ) {
                        eventParameters.add( indexed ? new TypeReference<Uint256>( true ) {
                        } : new TypeReference<Uint256>( false ) {
                        } );
                    }
                }
                eventInputsMap.put( eventName.toLowerCase(), new EventData( eventName, inputsList ) );
                events.add( new Event( eventName, eventParameters ) );
            }

            PolyType type = PolyType.VARCHAR;
            PolyType collectionsType = null;
            Integer length = 300;
            Integer scale = null;
            Integer dimension = null;
            Integer cardinality = null;
            int position = 0;
            List<ExportedColumn> blockCols = new ArrayList<>();
            for ( String blockCol : blockColumns ) {
                blockCols.add( new ExportedColumn(
                        blockCol,
                        blockTypes[position],
                        collectionsType,
                        length,
                        scale,
                        dimension,
                        cardinality,
                        false,
                        SCHEMA_NAME,
                        "block",
                        blockCol,
                        position,
                        position == 0 ) );
                position++;

            }
            map.put( "block", blockCols );
            List<ExportedColumn> transactCols = new ArrayList<>();
            position = 0;
            for ( String transactCol : transactionColumns ) {
                transactCols.add( new ExportedColumn(
                        transactCol,
                        transactionTypes[position],
                        collectionsType,
                        length,
                        scale,
                        dimension,
                        cardinality,
                        false,
                        SCHEMA_NAME,
                        "transaction",
                        transactCol,
                        position,
                        position == 0 ) );
                position++;
            }
            map.put( "transaction", transactCols );

            // Event Data: Creating columns for each event for specified smart contract based on ABI
            for ( Map.Entry<String, EventData> eventEntry : eventInputsMap.entrySet() ) {
                String eventName = eventEntry.getValue().getOriginalKey(); // Get the original event name
                List<JSONObject> inputsList = eventEntry.getValue().getData(); // Get the data
                List<ExportedColumn> eventDataCols = new ArrayList<>();
                int inputPosition = 0;

                for ( JSONObject input : inputsList ) {
                    String inputName = input.getString( "name" );
                    PolyType inputType = convertToPolyType( input.getString( "type" ) ); // convert event types to polytype
                    eventDataCols.add( new ExportedColumn(
                            inputName,
                            inputType,
                            collectionsType,
                            length,
                            scale,
                            dimension,
                            cardinality,
                            false,
                            SCHEMA_NAME,
                            eventName, // event name
                            inputName,
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
                            length,
                            scale,
                            dimension,
                            cardinality,
                            false,
                            SCHEMA_NAME,
                            eventName, // event name
                            columnName,
                            inputPosition,
                            inputPosition == 0
                    ) );
                    inputPosition++;
                }

                map.put( eventName, eventDataCols );
            }

            // caching
            if ( startCaching == Boolean.TRUE ) {
                try {
                    CatalogAdapter cachingAdapter = Catalog.getInstance().getAdapter( "hsqldb" ); // todo atm we use just the default store to cache
                    EventCacheManager.getInstance()
                            .register( getAdapterId(), cachingAdapter.id, clientURL, 50, smartContractAddress, fromBlock, toBlock, events, map )
                            .startCaching();
                } catch ( UnknownAdapterException e ) {
                    throw new RuntimeException( e );
                }


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


        protected List<JSONObject> getEventsFromABI( String etherscanApiKey, String contractAddress ) {
            List<JSONObject> eventList = new ArrayList<>();
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
                    String abi = jsonObject.getString( "result" );
                    // Convert ABI string to JSON Array
                    JSONArray abiArray = new JSONArray( abi );
                    for ( int i = 0; i < abiArray.length(); i++ ) {
                        JSONObject obj = abiArray.getJSONObject( i );

                        // Check if the current object is an event
                        if ( obj.getString( "type" ).equals( "event" ) ) {
                            eventList.add( obj );
                        }
                    }
                }

            } catch ( IOException e ) {
                throw new RuntimeException( e );
            }

            return eventList;
        }


        static PolyType convertToPolyType( String ethereumType ) {
            if ( ethereumType.startsWith( "uint" ) || ethereumType.startsWith( "int" ) ) {
                // Ethereum's uint and int types map to BIGINT in PolyType
                return PolyType.BIGINT;
            } else if ( ethereumType.startsWith( "bytes" ) || ethereumType.equals( "string" ) || ethereumType.equals( "address" ) ) {
                // Ethereum's bytes, string and address types map to VARCHAR in PolyType
                return PolyType.VARCHAR;
            } else if ( ethereumType.equals( "bool" ) ) {
                // Ethereum's bool type maps to BOOLEAN in PolyType
                return PolyType.BOOLEAN;
            } else {
                // If the type is unknown, use VARCHAR as a general type
                return PolyType.VARCHAR;
            }
        }


        protected String getSmartContractAddress() {
            return this.smartContractAddress;
        }


        protected BigInteger getFromBlock() {
            return this.fromBlock;
        }


        protected BigInteger getToBlock() {
            return this.toBlock;
        }


        protected Event getEventFromCatalogTable( String catalogTableName ) {
            if ( catalogTableName.equals( "block" ) || catalogTableName.equals( "transaction" ) ) {
                return null;
            }
            EventData eventData = eventInputsMap.get( catalogTableName );
            List<JSONObject> jsonObjects = eventData.getData();
            List<TypeReference<?>> parameterTypes = new ArrayList<>();
            for ( JSONObject jsonObject : jsonObjects ) {
                String type = jsonObject.getString( "type" );
                boolean indexed = jsonObject.getBoolean( "indexed" );

                if ( type.equals( "address" ) ) {
                    parameterTypes.add( indexed ? new TypeReference<Address>( true ) {
                    } : new TypeReference<Address>( false ) {
                    } );
                } else if ( type.equals( "uint256" ) ) {
                    parameterTypes.add( indexed ? new TypeReference<Uint256>( true ) {
                    } : new TypeReference<Uint256>( false ) {
                    } );
                }
                // ...
            }

            return new Event( eventData.getOriginalKey(), parameterTypes );
        }

    }

}