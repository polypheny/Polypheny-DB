/*
 * Copyright 2019-2024 The Polypheny Project
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.adapter.RelationalScanDelegate;
import org.polypheny.db.adapter.annotations.AdapterProperties;
import org.polypheny.db.adapter.annotations.AdapterSettingBoolean;
import org.polypheny.db.adapter.annotations.AdapterSettingInteger;
import org.polypheny.db.adapter.annotations.AdapterSettingString;
import org.polypheny.db.catalog.catalogs.RelAdapterCatalog;
import org.polypheny.db.catalog.entity.allocation.AllocationTableWrapper;
import org.polypheny.db.catalog.entity.logical.LogicalTableWrapper;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;


public class EthereumPlugin extends PolyPlugin {


    public static final String ADAPTER_NAME = "Ethereum";
    private long id;


    /**
     * Constructor to be used by plugin manager for plugin instantiation.
     * Your plugins have to provide constructor with this exact signature to be successfully loaded by manager.
     */
    public EthereumPlugin( PluginContext context ) {
        super( context );
    }


    @Override
    public void afterCatalogInit() {
        this.id = AdapterManager.addAdapterTemplate( EthereumDataSource.class, ADAPTER_NAME, EthereumDataSource::new );
    }


    @Override
    public void stop() {
        AdapterManager.removeAdapterTemplate( id );
    }


    @Slf4j
    @Extension
    @AdapterProperties(
            name = "Ethereum",
            description = "An adapter for querying the Ethereum blockchain. It uses the ethereum JSON-RPC API. Currently, this adapter only supports read operations.",
            usedModes = DeployMode.REMOTE,
            defaultMode = DeployMode.REMOTE)
    @AdapterSettingString(name = "ClientUrl", description = "The URL of the ethereum JSON RPC client", defaultValue = "https://mainnet.infura.io/v3/4d06589e97064040b5da99cf4051ef04", position = 1)
    @AdapterSettingInteger(name = "Blocks", description = "The number of Blocks to fetch when processing a query", defaultValue = 10, position = 2, modifiable = true)
    @AdapterSettingBoolean(name = "ExperimentalFiltering", description = "Experimentally filter Past Block", defaultValue = false, position = 3, modifiable = true)
    public static class EthereumDataSource extends DataSource<RelAdapterCatalog> {

        @Delegate(excludes = Excludes.class)
        private final RelationalScanDelegate delegate;
        private String clientURL;
        @Getter
        private int blocks;
        @Getter
        private boolean experimentalFiltering;
        @Getter
        private EthereumNamespace currentNamespace;


        public EthereumDataSource( final long storeId, final String uniqueName, final Map<String, String> settings ) {
            super( storeId, uniqueName, settings, true, new RelAdapterCatalog( storeId ) );
            setClientURL( settings.get( "ClientUrl" ) );
            this.blocks = Integer.parseInt( settings.get( "Blocks" ) );
            this.experimentalFiltering = Boolean.parseBoolean( settings.get( "ExperimentalFiltering" ) );
            createInformationPage();
            enableInformationPage();

            this.delegate = new RelationalScanDelegate( this, adapterCatalog );
        }


        private void setClientURL( String clientURL ) {
            Web3j web3j = Web3j.build( new HttpService( clientURL ) );
            try {
                BigInteger latest = web3j.ethBlockNumber().send().getBlockNumber();
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Unable to connect the client URL '" + clientURL + "'" );
            }
            web3j.shutdown();
            this.clientURL = clientURL;
        }


        @Override
        public void updateNamespace( String name, long id ) {
            currentNamespace = new EthereumNamespace( id, adapterId, this.clientURL );
        }


        @Override
        public List<PhysicalEntity> createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation ) {
            PhysicalTable table = adapterCatalog.createTable(
                    logical.table.getNamespaceName(),
                    logical.table.name,
                    logical.columns.stream().collect( Collectors.toMap( c -> c.id, c -> c.name ) ),
                    logical.table,
                    logical.columns.stream().collect( Collectors.toMap( t -> t.id, t -> t ) ),
                    logical.pkIds, allocation );

            EthereumTable physical = currentNamespace.createBlockchainTable( table, this );

            adapterCatalog.replacePhysical( physical );

            return List.of( physical );
        }


        @Override
        public void truncate( Context context, long allocId ) {
            throw new GenericRuntimeException( "Blockchain adapter does not support truncate" );
        }


        @Override
        public Map<String, List<ExportedColumn>> getExportedColumns() {
            Map<String, List<ExportedColumn>> map = new HashMap<>();
            String[] blockColumns = { "number", "hash", "parent_hash", "nonce", "sha3uncles", "logs_bloom", "transactions_root", "state_root", "receipts_root", "author", "miner", "mix_hash", "difficulty", "total_difficulty", "extra_data", "size", "gas_limit", "gas_used", "timestamp" };
            PolyType[] blockTypes = { PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.BIGINT, PolyType.TIMESTAMP };
            String[] transactionColumns = { "hash", "nonce", "block_hash", "block_number", "transaction_index", "from", "to", "value", "gas_price", "gas", "input", "creates", "public_key", "raw", "r", "s" };
            PolyType[] transactionTypes = { PolyType.VARCHAR, PolyType.BIGINT, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR };

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
                        "public",
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
                        "public",
                        "transaction",
                        transactCol,
                        position,
                        position == 0 ) );
                position++;
            }
            map.put( "transaction", transactCols );
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


        protected void updateNativePhysical( long allocId ) {
            PhysicalTable table = this.adapterCatalog.fromAllocation( allocId );
            adapterCatalog.replacePhysical( this.currentNamespace.createBlockchainTable( table, this ) );
        }


        @Override
        public void renameLogicalColumn( long id, String newColumnName ) {
            adapterCatalog.renameLogicalColumn( id, newColumnName );
            adapterCatalog.fields.values().stream().filter( c -> c.id == id ).forEach( c -> updateNativePhysical( c.allocId ) );
        }

    }


    @SuppressWarnings("unused")
    private interface Excludes {

        void renameLogicalColumn( long id, String newColumnName );

        void refreshTable( long allocId );

        void createTable( Context context, LogicalTableWrapper logical, AllocationTableWrapper allocation );

    }

}