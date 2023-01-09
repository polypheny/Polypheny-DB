/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.extraction;

import jakarta.json.Json;
import jakarta.json.JsonArrayBuilder;
import jakarta.json.JsonObjectBuilder;
import lombok.Setter;
import org.polypheny.db.StatisticsManager;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.information.*;
import org.polypheny.db.transaction.TransactionManager;

import java.util.Arrays;
import java.util.Objects;

public class SchemaExtractor {

    private static final SchemaExtractor INSTANCE = new SchemaExtractor();

    private InformationCode informationLogOutput;
    private InformationCode informationResult;

    @Setter
    private TransactionManager transactionManager;


    private SchemaExtractor() {
        registerMonitoringPage();
    }


    /**
     * Singleton
     *
     * @return The SchemaExtractor
     */
    public static SchemaExtractor getInstance() {
        return INSTANCE;
    }


    public void startServer(TransactionManager transactionManager) {
        // Start server (for communication with Python)
        // TODO: Port number? For now, leave as is, but give this port number to python docker container for starting
        Server server = new Server(20598, transactionManager, this);
        Thread thread = new Thread(server);
        thread.start();
    }


    /**
     * Your central method that serves as an entry point. Start with your implementation in this method.
     *
     * @param namespaceId   The id of the namespace to analyze
     * @param namespaceName
     */
    void execute(long namespaceId, String namespaceName) {
        // Build input
        String inputJsonStr = buildInput(namespaceId, namespaceName);

        // Send json to listener
        Server.broadcastMessage("Server", "namespaceInfo", inputJsonStr);
    }


    /**
     * In this method you gather the input for python part. The example show how to access some basic information
     * from the catalog.
     *
     * @param namespaceId   The id of the namespace
     * @param namespaceName
     * @return A JSON string
     */
    private String buildInput(long namespaceId, String namespaceName) {
        Catalog catalog = Catalog.getInstance();
        CatalogSchema catalogSchema = catalog.getSchema(namespaceId);
        JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder()
                .add("datamodel", catalogSchema.namespaceType.name());

        // TODO: In case of large tables, only send samples from tables!
        JsonArrayBuilder tablesBuilder = Json.createArrayBuilder();
        for (CatalogTable catalogTable : catalog.getTables(namespaceId, null)) {

            // Array of column names in namespace
            JsonArrayBuilder columnsBuilder = Json.createArrayBuilder();
            for ( String columnName : catalogTable.getColumnNames() ) {
                columnsBuilder.add( columnName );
            }

            // Array of primary key column names in namespace
            JsonArrayBuilder primaryKeyBuilder = Json.createArrayBuilder();
            for ( String primaryKeyColumnName : catalog.getPrimaryKey( catalogTable.primaryKey ).getColumnNames() ) {
                primaryKeyBuilder.add( primaryKeyColumnName );
            }

            // Array of foreign key relationships (from this table to other tables)
            JsonArrayBuilder foreignKeyBuilder = Json.createArrayBuilder();
            for ( CatalogForeignKey foreignKey : catalog.getForeignKeys( catalogTable.id ) ) {
                JsonArrayBuilder columnNamesHere = Json.createArrayBuilder();
                String tableNameHere = catalogTable.name;
                for ( Long columnId : foreignKey.columnIds ) {
                    columnNamesHere.add( catalog.getColumn( columnId ).name );
                }
                long tableIdThere = foreignKey.referencedKeyTableId;
                String targetTableName = catalog.getTable( tableIdThere ).name;
                JsonArrayBuilder primaryKeyThereBuilder = Json.createArrayBuilder();
                for ( String primaryKeyThereColumnName : catalog.getPrimaryKey( catalog.getTable( tableIdThere ).primaryKey ).getColumnNames() ) {
                    primaryKeyThereBuilder.add( primaryKeyThereColumnName);
                }

                JsonObjectBuilder foreignKeyInformation = Json.createObjectBuilder();
                foreignKeyInformation.add("tableName", tableNameHere);
                foreignKeyInformation.add("columnNames", columnNamesHere);
                foreignKeyInformation.add("foreignTableName", targetTableName);
                foreignKeyInformation.add("foreignTableColumnNames", primaryKeyThereBuilder);
                foreignKeyBuilder.add(foreignKeyInformation);
            }

            // Array of statistics for tables in namespace
            JsonArrayBuilder statisticsBuilder = Json.createArrayBuilder();
            Integer rowCount = StatisticsManager.getInstance().rowCountPerTable(catalogTable.id);
            statisticsBuilder.add(rowCount);

            // Send all information arrays to Python part
            tablesBuilder.add(Json.createObjectBuilder()
                    .add("namespaceName", namespaceName)
                    .add("tableName", catalogTable.name)
                    .add("columnNames", columnsBuilder)
                    .add("primaryKey", primaryKeyBuilder)
                    .add("foreignKeys", foreignKeyBuilder)
                    .add("statistics", statisticsBuilder)
            );
        }
        jsonObjectBuilder.add( "tables", tablesBuilder );
        return jsonObjectBuilder.build().toString();
    }


    /**
     * This method adds a page to the monitoring section of the Polypheny-UI. The page allows you to
     * easily trigger the execute() method.
     */
    private void registerMonitoringPage() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage("Schema Integration" );
        im.addPage( page );

        InformationGroup actionGroup = new InformationGroup(page, "Action ").setOrder(1);
        im.addGroup( actionGroup );

        InformationText actionText = new InformationText(
                actionGroup,
                "Run schema integration on specified namespace." );
        actionText.setOrder( 1 );
        im.registerInformation( actionText );

        InformationAction runAction = new InformationAction( actionGroup, "Run", parameters -> {
            if ( parameters == null || parameters.size() != 1 || parameters.get( "namespace" ) == null ) {
                return "No or invalid parameter!";
            }
            String namespaceName = parameters.get( "namespace" );
            CatalogSchema catalogSchema;
            try {
                catalogSchema = Catalog.getInstance().getSchema( Catalog.defaultDatabaseId, namespaceName );
            } catch ( UnknownSchemaException e ) {
                return "There is no namespace with this name!";
            }
            if ( Server.listenerMap.isEmpty() ) {
                return "No listeners!";
            } else {
                SchemaExtractor.getInstance().execute(catalogSchema.id, namespaceName );
            }
            return "Running schema integration!";
        } ).withParameters( "namespace" );
        runAction.setOrder( 2 );
        im.registerInformation( runAction);

        InformationGroup logGroup = new InformationGroup(page, "Log Output").setOrder(2);
        im.addGroup(logGroup);
        informationLogOutput = new InformationCode(logGroup, "");
        im.registerInformation(informationLogOutput);

        InformationGroup resultGroup = new InformationGroup(page, "Result").setOrder(3);
        im.addGroup(resultGroup);
        informationResult = new InformationCode(resultGroup, "");
        im.registerInformation(informationResult);

        InformationGroup speedThoroughnessGroup = new InformationGroup(page, "Speed/Thoroughness").setOrder(4);
        im.addGroup(speedThoroughnessGroup);

        InformationText speedThoroughnessText = new InformationText(
                speedThoroughnessGroup,
                "Set speed/thoroughness preference: 1 = all speed. 5 = all thoroughness.");
        actionText.setOrder(4);
        im.registerInformation(speedThoroughnessText);

        InformationAction runSpeedThoroughness = new InformationAction(speedThoroughnessGroup, "Set", parameters -> {
            if (parameters == null || parameters.size() != 1 || parameters.get("st") == null) {
                return "No or invalid parameter!";
            }
            String speedThoroughness = parameters.get("st");
            Server.broadcastMessage("Server", "speedThoroughness", speedThoroughness);
            return "Successfully set speed/thoroughness preference!";
        }).withParameters("st");
        runAction.setOrder(5);
        im.registerInformation(runSpeedThoroughness);

        InformationGroup groundTruthGroup = new InformationGroup(page, "Ground Truth").setOrder(6);
        im.addGroup(groundTruthGroup);
        InformationTable groundTruthTable = new InformationTable(
                groundTruthGroup,
                Arrays.asList("Table 1", "Table 2")
        );
        groundTruthTable.setOrder(7);
        im.registerInformation(groundTruthTable);
    }

    public void updateResultCode(String newResultCode) {
        if (!Objects.equals(newResultCode, "")) {
            informationResult.updateCode(newResultCode + "\r\n");
            InformationResponse msg = new InformationResponse();
            msg.message("Schema integration results are in!");
        } else {
            InformationResponse msg = new InformationResponse();
            msg.error("Empty schema integration result.");
        }
    }

}
