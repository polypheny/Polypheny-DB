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

package org.polypheny.db.adapter.blockchain;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingBoolean;
import org.polypheny.db.adapter.Adapter.AdapterSettingInteger;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;


@Slf4j
@AdapterProperties(
        name = "Blockchain",
        description = "An adapter for querying the Ethereum blockchain. It uses the ethereum JSON-RPC API. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.REMOTE)
@AdapterSettingString(name = "ClientUrl", description = "The URL of the ethereum JSON RPC client", defaultValue = "https://mainnet.infura.io/v3/4d06589e97064040b5da99cf4051ef04", position = 1)
@AdapterSettingInteger(name = "Blocks", description = "The number of Blocks to fetch when processing a query", defaultValue = 10, position = 2)
@AdapterSettingBoolean(name = "ExperimentalFiltering", description = "Experimentally filter Past Block", defaultValue = false, position = 3)
public class BlockchainDataSource extends DataSource {

    private String clientURL;
    private final int blocks;
    private final boolean experimentalFiltering;
    private BlockchainSchema currentSchema;


    public BlockchainDataSource( final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );
        setClientURL( settings.get( "ClientUrl" ) );
        this.blocks = Integer.parseInt( settings.get( "Blocks" ) );
        this.experimentalFiltering = Boolean.parseBoolean( settings.get( "ExperimentalFiltering" ) );
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
        currentSchema = new BlockchainSchema( this.clientURL, this.blocks, this.experimentalFiltering );
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.createBlockchainTable( catalogTable, columnPlacementsOnStore, this );
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
        String[] blockColumns = { "number", "hash", "parentHash", "nonce", "sha3Uncles", "logsBloom", "transactionsRoot", "stateRoot", "receiptsRoot", "author", "miner", "mixHash", "difficulty", "totalDifficulty", "extraData", "size", "gasLimit", "gasUsed", "timestamp" };
        PolyType[] blockTypes = { PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.VARCHAR, PolyType.BIGINT, PolyType.BIGINT, PolyType.BIGINT, PolyType.TIMESTAMP };
        String[] transactionColumns = { "hash", "nonce", "blockHash", "blockNumber", "transactionIndex", "from", "to", "value", "gasPrice", "gas", "input", "creates", "publicKey", "raw", "r", "s" };
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

}
