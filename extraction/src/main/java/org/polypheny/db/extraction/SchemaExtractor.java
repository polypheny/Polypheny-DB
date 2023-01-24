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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogCollection;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.cypher.cypher2alg.CypherQueryParameters;
import org.polypheny.db.information.*;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.runtime.PolyCollections;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Slf4j
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

    private Transaction getTransaction() {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction(Catalog.defaultUserId, Catalog.defaultDatabaseId, false, "Schema Extraction Server");
        } catch (GenericCatalogException | UnknownUserException | UnknownDatabaseException | UnknownSchemaException e) {
            throw new RuntimeException(e);
        }
        return transaction;
    }

    // For executing Cypher commands
    private PolyImplementation processCypherQuery(Statement statement, String cypherql, String namespaceName) {
        PolyImplementation result;
        Processor cypherProcessor = statement.getTransaction().getProcessor(Catalog.QueryLanguage.CYPHER);

        Node parsed = cypherProcessor.parse(cypherql).get(0);

        if (parsed.isA(Kind.DDL)) {
            throw new RuntimeException("No DDL expected here");
        } else {
            AlgRoot logicalRoot = cypherProcessor.translate(statement, parsed, new CypherQueryParameters(cypherql, Catalog.NamespaceType.GRAPH, namespaceName));

            // Prepare
            result = statement.getQueryProcessor().prepareQuery(logicalRoot, true);
        }
        return result;
    }

    private List<String> findCypherLabels(final Statement statement, final String cypherMatch, String namespacename) throws Exception {
        PolyImplementation result;
        try {
            result = processCypherQuery(statement, cypherMatch, namespacename);
        } catch (Throwable t) {
            throw new Exception(t);
        }
        List<List<Object>> rows = result.getRows(statement, (int) result.getMaxRowCount());

        List<String> allLabels = new ArrayList<String>();
        for (List<Object> row : rows) {
            PolyNode node = (PolyNode) row.get(0);
            PolyCollections.PolyList<String> labels = node.getLabels();
            for (String label : labels) {
                if (!allLabels.contains(label)) {
                    allLabels.add(label);
                }
            }
        }

        return allLabels;
    }

    public List<String> executeCypher(String query, String namespaceName) {
        List<String> labels = new ArrayList<String>();
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        try {
            labels = findCypherLabels(statement, query, namespaceName);
            // Could execute multiple queries here before commit
            transaction.commit();
        } catch (Exception | TransactionException e) {
            log.error("Caught exception while executing a query from the console", e);
            try {
                transaction.rollback();
            } catch (TransactionException ex) {
                log.error("Caught exception while rollback", e);
            }
        }
        return labels;
    }

    // Next three methods copypasted from explore-by-example:QueryProcessExplorer
    public QueryResult executeSQL(String query) {
        QueryResult result = new QueryResult();
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        try {
            result = executeSqlSelect(statement, query);
            // Could execute multiple queries here before commit
            transaction.commit();
        } catch (Exception | TransactionException e) {
            log.error("Caught exception while executing a query from the console", e);
            try {
                transaction.rollback();
            } catch (TransactionException ex) {
                log.error("Caught exception while rollback", e);
            }
        }
        return result;
    }


    private QueryResult executeSqlSelect(final Statement statement, final String sqlSelect) throws Exception {
        PolyImplementation result;
        try {
            result = processQuery(statement, sqlSelect);
        } catch (Throwable t) {
            throw new Exception(t);
        }
        //List<List<Object>> rows = result.getRows( statement, DEFAULT_SIZE );
        List<List<Object>> rows = result.getRows(statement, (int) result.getMaxRowCount());

        List<String> typeInfo = new ArrayList<>();
        List<String> name = new ArrayList<>();
        for (AlgDataTypeField metaData : result.getRowType().getFieldList()) {
            typeInfo.add(metaData.getType().getFullTypeString());
            name.add(metaData.getName());
        }

        if (rows.size() == 1) {
            for (List<Object> row : rows) {
                if (row.size() == 1) {
                    for (Object o : row) {
                        return new QueryResult(o.toString(), rows.size(), typeInfo, name);
                    }
                }
            }
        }

        List<String[]> data = new ArrayList<>();
        for (List<Object> row : rows) {
            String[] temp = new String[row.size()];
            int counter = 0;
            for (Object o : row) {
                if (o == null) {
                    temp[counter] = null;
                } else {
                    temp[counter] = o.toString();
                }
                counter++;
            }
            data.add(temp);
        }

        String[][] d = data.toArray(new String[0][]);

        return new QueryResult(d, rows.size(), typeInfo, name);
    }


    private PolyImplementation processQuery(Statement statement, String sql) {
        PolyImplementation result;
        Processor sqlProcessor = statement.getTransaction().getProcessor(Catalog.QueryLanguage.SQL);

        Node parsed = sqlProcessor.parse(sql).get(0);

        if (parsed.isA(Kind.DDL)) {
            // explore by example should not execute any ddls
            throw new RuntimeException("No DDL expected here");
        } else {
            Pair<Node, AlgDataType> validated = sqlProcessor.validate(statement.getTransaction(), parsed, false);
            AlgRoot logicalRoot = sqlProcessor.translate(statement, validated.left, null);

            // Prepare
            result = statement.getQueryProcessor().prepareQuery(logicalRoot, true);
        }
        return result;
    }

    public List<String> getGraphProperties(String graphLabel, String namespace) {
        List<String> propertyNames = new ArrayList<>();
        String query = "SELECT * FROM " + namespace + ".\"" + graphLabel + "\"";
        QueryResult queryResult = executeSQL(query);
        for (String[] row : queryResult.data) {
            // remove leading and trailing curly braces
            String singleProperties = row[1].substring(1, row[1].length() - 1);
            String[] propertyParts = singleProperties.split(",");
            for (String propertyPart : propertyParts) {
                String[] property = propertyPart.split("=");
                String propertyName = property[0];
                if (!propertyNames.contains(propertyName)) {
                    propertyNames.add(propertyName);
                }
                //String propertyValue = property[1];
            }
        }
        return propertyNames;
    }

    public List<String> getFirstLevelTagsFromCollection(String collectionName, String namespace) {
        List<String> tagNames = new ArrayList<>();
        String query = "SELECT * FROM " + namespace + ".\"" + collectionName + "\"";
        QueryResult queryResult = executeSQL(query);
        for (String[] row : queryResult.data) {
            // remove leading and trailing curly braces
            String singleTags = row[0].substring(1, row[0].length() - 1);
            String[] tagParts = singleTags.split(",");
            for (String tagPart : tagParts) {
                String[] tag = tagPart.split(":");
                String tagName = tag[0];
                // trim whitespace and leading/trailing "
                tagName = tagName.trim();
                tagName = tagName.substring(1, tagName.length() - 1);
                if (!tagNames.contains(tagName)) {
                    tagNames.add(tagName);
                }
            }
        }
        return tagNames;
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

        JsonArrayBuilder tablesBuilder = Json.createArrayBuilder();

        switch (catalogSchema.namespaceType.name()) {
            case "RELATIONAL":
                for (CatalogTable catalogTable : catalog.getTables(namespaceId, null)) {

                    // Array of column names in namespace
                    JsonArrayBuilder columnsBuilder = Json.createArrayBuilder();
                    for (String columnName : catalogTable.getColumnNames()) {
                        columnsBuilder.add(columnName);
                    }

                    // Array of primary key column names in namespace
                    JsonArrayBuilder primaryKeyBuilder = Json.createArrayBuilder();
                    for (String primaryKeyColumnName : catalog.getPrimaryKey(catalogTable.primaryKey).getColumnNames()) {
                        primaryKeyBuilder.add(primaryKeyColumnName);
                    }

                    // Array of foreign key relationships (from this table to other tables)
                    JsonArrayBuilder foreignKeyBuilder = Json.createArrayBuilder();
                    for (CatalogForeignKey foreignKey : catalog.getForeignKeys(catalogTable.id)) {
                        JsonArrayBuilder columnNamesHere = Json.createArrayBuilder();
                        String tableNameHere = catalogTable.name;
                        for (Long columnId : foreignKey.columnIds) {
                            columnNamesHere.add(catalog.getColumn(columnId).name);
                        }
                        long tableIdThere = foreignKey.referencedKeyTableId;
                        String targetTableName = catalog.getTable(tableIdThere).name;
                        JsonArrayBuilder primaryKeyThereBuilder = Json.createArrayBuilder();
                        for (String primaryKeyThereColumnName : catalog.getPrimaryKey(catalog.getTable(tableIdThere).primaryKey).getColumnNames()) {
                            primaryKeyThereBuilder.add(primaryKeyThereColumnName);
                        }

                        JsonObjectBuilder foreignKeyInformation = Json.createObjectBuilder();
                        foreignKeyInformation.add("tableName", tableNameHere);
                        foreignKeyInformation.add("columnNames", columnNamesHere);
                        foreignKeyInformation.add("foreignTableName", targetTableName);
                        foreignKeyInformation.add("foreignTableColumnNames", primaryKeyThereBuilder);
                        foreignKeyBuilder.add(foreignKeyInformation);
                    }

                    // Array of statistics for tables in namespace
                    //JsonArrayBuilder statisticsBuilder = Json.createArrayBuilder();
                    //Integer rowCount = StatisticsManager.getInstance().rowCountPerTable(catalogTable.id);
                    //statisticsBuilder.add(rowCount);

                    // Send all information arrays to Python part
                    tablesBuilder.add(Json.createObjectBuilder()
                                    .add("namespaceName", namespaceName)
                                    .add("tableName", catalogTable.name)
                                    .add("columnNames", columnsBuilder)
                                    .add("primaryKey", primaryKeyBuilder)
                                    .add("foreignKeys", foreignKeyBuilder)
                            //.add("statistics", statisticsBuilder)
                    );
                }
                break;
            case "GRAPH":
                // Get all graph labels
                // they will be handled as equivalent to table names
                List<String> graphLabels = executeCypher("Match (n) return (n)", namespaceName);

                // Get properties
                // they will be handled as equivalent to column names
                for (String graphLabel : graphLabels) {
                    // Array of column names in namespace
                    JsonArrayBuilder propertyBuilder = Json.createArrayBuilder();
                    List<String> propertyNames = getGraphProperties(graphLabel, namespaceName);
                    for (String propertyName : propertyNames) {
                        propertyBuilder.add(propertyName);
                    }

                    // Send all information arrays to Python part
                    tablesBuilder.add(Json.createObjectBuilder()
                            .add("namespaceName", namespaceName)
                            .add("graphLabel", graphLabel)
                            .add("propertyNames", propertyBuilder)
                    );
                }
                break;
            case "DOCUMENT":
                for (CatalogCollection catalogCollection : catalog.getCollections(namespaceId, null)) {

                    // Collection name
                    // it will be handled as equivalent to table name
                    String collectionName = catalogCollection.name;

                    // Array of collection names in namespace
                    JsonArrayBuilder tagsBuilder = Json.createArrayBuilder();
                    List<String> firstLevelTagNames = getFirstLevelTagsFromCollection(collectionName, namespaceName);
                    for (String firstLevelTag : firstLevelTagNames) {
                        tagsBuilder.add(firstLevelTag);
                    }

                    // Send all information arrays to Python part
                    tablesBuilder.add(Json.createObjectBuilder()
                            .add("namespaceName", namespaceName)
                            .add("collectionName", collectionName)
                            .add("firstLevelTags", tagsBuilder)
                    );
                }

                break;
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

//        InformationGroup groundTruthGroup = new InformationGroup(page, "Ground Truth").setOrder(6);
//        im.addGroup(groundTruthGroup);
//        InformationTable groundTruthTable = new InformationTable(
//                groundTruthGroup,
//                Arrays.asList("Table 1", "Table 2")
//        );
//        groundTruthTable.setOrder(7);
//        im.registerInformation(groundTruthTable);
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

    public void updateLogCode(String newLogInformation) {
        if (!Objects.equals(newLogInformation, "")) {
            informationLogOutput.extendCode(newLogInformation + "\r\n");
        }
    }

}
