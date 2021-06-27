package org.polypheny.db.adapter.blockchain;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.Adapter.AdapterProperties;
import org.polypheny.db.adapter.Adapter.AdapterSettingString;
import org.polypheny.db.adapter.DataSource;
import org.polypheny.db.adapter.DeployMode;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Table;
import org.polypheny.db.transaction.PolyXid;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.http.HttpService;


@Slf4j
@AdapterProperties(
        name = "Blockchain",
        description = "An adapter for querying the ethereum blockchain.It uses the ethereum JSON-RPC API. Currently, this adapter only supports read operations.",
        usedModes = DeployMode.EMBEDDED)
@AdapterSettingString(name = "ClientUrl", description = "The URL of the ethereum JSONRPC client",defaultValue = "https://mainnet.infura.io/v3/4d06589e97064040b5da99cf4051ef04", position = 1)
public class BlockchainDataSource extends DataSource {

    private String clientURL;
    private BlockchainSchema currentSchema;

    public BlockchainDataSource(final int storeId, final String uniqueName, final Map<String, String> settings ) {
        super( storeId, uniqueName, settings, true );

        setClientURL( settings );
        registerInformationPage( uniqueName );
        enableInformationPage();
    }


    private void setClientURL( Map<String, String> settings ) {
        String clientURL = settings.get("ClientUrl");
        Web3j web3j = Web3j.build(new HttpService(clientURL));
        try {
            BigInteger latest = web3j.ethBlockNumber().send().getBlockNumber();
        } catch (Exception e) {
            throw new RuntimeException( ":Unable to connect the client URL '"+clientURL+"'");
        }
        web3j.shutdown();
        this.clientURL = clientURL;
    }


    @Override
    public void createNewSchema( SchemaPlus rootSchema, String name ) {
        currentSchema = new BlockchainSchema(this.clientURL);
    }


    @Override
    public Table createTableSchema( CatalogTable catalogTable, List<CatalogColumnPlacement> columnPlacementsOnStore ) {
        return currentSchema.   createBlockchainTable(catalogTable,columnPlacementsOnStore,this);
    }


    @Override
    public Schema getCurrentSchema() {
        return currentSchema;
    }


    @Override
    public void truncate( Context context, CatalogTable table ) {
        throw new RuntimeException( "CSV adapter does not support truncate" );
    }


    @Override
    public Map<String, List<ExportedColumn>> getExportedColumns() {
        Map<String, List<ExportedColumn>> map = new HashMap<>();
        String[] blockColumns = {"number","hash","parentHash","nonce","sha3Uncles","logsBloom","transactionsRoot","stateRoot","receiptsRoot","author","miner","mixHash","difficulty","totalDifficulty","extraData","size","gasLimit","gasUsed","timestamp"};
        String[] transactionColumns = {"hash","nonce","blockHash","blockNumber","transactionIndex","from","to","value","gasPrice","gas","input","creates","publicKey","raw","r","s"};

        PolyType type = PolyType.VARCHAR;
        PolyType collectionsType = null;
        Integer length = 300;
        Integer scale = null;
        Integer dimension = null;
        Integer cardinality = null;
        int position = 0;
        List<ExportedColumn> blockCols = new ArrayList<>();
        for(String blockCol:blockColumns){
            position ++ ;
            blockCols.add( new ExportedColumn(
                    blockCol,
                    type,
                    collectionsType,
                    length,
                    scale,
                    dimension,
                    cardinality,
                    false,
                    "ethereum",
                    "block",
                    blockCol,
                    position,
                    position == 1 ) );

        }
        map.put("block",blockCols);
        List<ExportedColumn> transactCols = new ArrayList<>();
        position=0;
        for(String transactCol:transactionColumns){
            position ++ ;
            transactCols.add( new ExportedColumn(
                    transactCol,
                    type,
                    collectionsType,
                    length,
                    scale,
                    dimension,
                    cardinality,
                    false,
                    "ethereum",
                    "transaction",
                    transactCol,
                    position,
                    position == 1 ) );
        }
        map.put("transaction",transactCols);
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
            setClientURL( settings );
        }
    }


    protected void registerInformationPage( String uniqueName ) {
        InformationManager im = InformationManager.getInstance();
        /*informationPage = new InformationPage( uniqueName, "CSV Data Source" ).setLabel( "Sources" );
        im.addPage( informationPage );*/

        for ( Map.Entry<String, List<ExportedColumn>> entry : getExportedColumns().entrySet() ) {
            InformationGroup group = new InformationGroup( informationPage, entry.getValue().get( 0 ).physicalSchemaName );

            InformationTable table = new InformationTable(
                    group,
                    Arrays.asList( "Position", "Column Name", "Type", "Nullable", "Filename", "Primary" ) );
            for ( ExportedColumn exportedColumn : entry.getValue() ) {
                table.addRow(
                        exportedColumn.physicalPosition,
                        exportedColumn.name,
                        exportedColumn.getDisplayType(),
                        exportedColumn.nullable ? "✔" : "",
                        exportedColumn.physicalSchemaName,
                        exportedColumn.primary ? "✔" : ""
                );
            }
            informationElements.add( table );
            informationGroups.add( group );
        }
    }

}
