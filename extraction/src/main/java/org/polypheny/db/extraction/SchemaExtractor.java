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
import jakarta.json.JsonObject;
import jakarta.json.JsonObjectBuilder;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.LinkedList;
import java.util.List;

import lombok.Setter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogForeignKey;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationCode;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.transaction.TransactionManager;

public class SchemaExtractor {

    // TODO: The "PYTHON_COMMAND" needs to be system-agnostic (python3 didn't work for me)
    public static final String PYTHON_COMMAND = "python";
    private static final SchemaExtractor INSTANCE = new SchemaExtractor();

    @Setter
    private TransactionManager transactionManager;
    private InformationCode informationLogOutput;
    private InformationCode informationResult;


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


    /**
     * Your central method that serves as an entry point. Start with your implementation in this method.
     *
     * @param namespaceId The id of the namespace to analyze
     */
    void execute( long namespaceId ) {
        // Create files
        final File inputFile;
        final File outputFile;
        try {
            inputFile = File.createTempFile( "pdb-schema-extraction", "input" );
            outputFile = File.createTempFile( "pdb-schema-extraction", "output" );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        // Build input
        String inputJsonStr = buildInput( namespaceId );
        try ( FileWriter fileWriter = new FileWriter( inputFile );
                BufferedWriter writer = new BufferedWriter( fileWriter ) ) {
            writer.write( inputJsonStr );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        // Execute python
        try {
            String log = executePython( inputFile, outputFile );
            informationLogOutput.updateCode( log );
        } catch ( IOException | InterruptedException e ) {
            throw new RuntimeException( e );
        }

        // Read output
        StringBuilder resultBuilder = new StringBuilder();
        try ( FileReader fileReader = new FileReader( outputFile );
                BufferedReader reader = new BufferedReader( fileReader ) ) {
            String line;
            while ( (line = reader.readLine()) != null ) {
                resultBuilder.append( line ).append( System.lineSeparator() );
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
        informationResult.updateCode( resultBuilder.toString() );
    }


    /**
     * In this method you gather the input for python script. The example show how to access some basic information
     * from the catalog.
     *
     * @param namespaceId The id of the namespace
     * @return A JSON string
     */
    private String buildInput( long namespaceId ) {
        Catalog catalog = Catalog.getInstance();
        CatalogSchema catalogSchema = catalog.getSchema( namespaceId );
        JsonObjectBuilder jsonObjectBuilder = Json.createObjectBuilder()
                .add( "datamodel", catalogSchema.namespaceType.name() );

        JsonArrayBuilder tablesBuilder = Json.createArrayBuilder();
        for ( CatalogTable catalogTable : catalog.getTables( namespaceId, null ) ) {

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
            for ( CatalogForeignKey foreignKey : catalog.getForeignKeys( catalogTable.id ) ) {
                JsonArrayBuilder columnNamesHere = Json.createArrayBuilder();
                String tableNameHere = catalogTable.name;
                for (Long columnId : foreignKey.columnIds) {
                    columnNamesHere.add(catalog.getColumn(columnId).name);
                }
                long tableIdThere = foreignKey.referencedKeyTableId;
                String targetTableName = catalog.getTable(tableIdThere).name;
                JsonArrayBuilder primaryKeyThereBuilder = Json.createArrayBuilder();
                for (String primaryKeyThereColumnName:catalog.getPrimaryKey(catalog.getTable(tableIdThere).primaryKey).getColumnNames() ) {
                    primaryKeyThereBuilder.add(primaryKeyThereColumnName);
                }

                JsonObjectBuilder foreignKeyInformation = Json.createObjectBuilder();
                foreignKeyInformation.add("tableName", tableNameHere);
                foreignKeyInformation.add("columnNames", columnNamesHere);
                foreignKeyInformation.add("foreignTableName", targetTableName);
                foreignKeyInformation.add("foreignTableColumnNames", primaryKeyThereBuilder);
                foreignKeyBuilder.add(foreignKeyInformation);
            }

            tablesBuilder.add( Json.createObjectBuilder()
                    .add("tableName", catalogTable.name )
                    .add("columnNames", columnsBuilder )
                    .add("primaryKey", primaryKeyBuilder)
                    .add("foreignKeys", foreignKeyBuilder)
            );
        }
        jsonObjectBuilder.add( "tables", tablesBuilder );
        return jsonObjectBuilder.build().toString();
    }


    private String executePython( File inputFile, File outputFile ) throws IOException, InterruptedException {
        // Create tmp file for code
        final File codeFile;
        try {
            codeFile = File.createTempFile( "pdb-schema-extraction", "script" );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        // Write to tmp file
        try ( FileWriter fileWriter = new FileWriter( codeFile );
                BufferedWriter writer = new BufferedWriter( fileWriter );
                InputStream in = SchemaExtractor.class.getResourceAsStream( "schemaExtraction.py" );
                BufferedReader reader = new BufferedReader( new InputStreamReader( in ) ) ) {
            String line;
            while ( (line = reader.readLine()) != null ) {
                writer.write( line );
                writer.write( System.lineSeparator() );
            }
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }

        ProcessBuilder processBuilder = new ProcessBuilder(
                PYTHON_COMMAND,
                codeFile.getAbsolutePath(),
                inputFile.getPath(),
                outputFile.getPath() );
        processBuilder.redirectErrorStream( true );

        Process process = processBuilder.start();

        StringBuilder logOutput = new StringBuilder();
        try ( var reader = new BufferedReader( new InputStreamReader( process.getInputStream() ) ) ) {
            String line;
            while ( (line = reader.readLine()) != null ) {
                logOutput.append( line ).append( System.lineSeparator() );
            }
        }
        int exitCode = process.waitFor();
        return logOutput.toString();
    }


    /**
     * This method adds a page to the monitoring section of the Polypheny-UI. The page allows you to
     * easily trigger the execute() method.
     */
    private void registerMonitoringPage() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Schema Extraction" );
        im.addPage( page );

        InformationGroup actionGroup = new InformationGroup( page, "Actio " ).setOrder( 1 );
        im.addGroup( actionGroup );

        InformationText actionText = new InformationText(
                actionGroup,
                "Run schema extraction on specified namespace." );
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
            SchemaExtractor.getInstance().execute( catalogSchema.id );
            return "Successfully executed schema extractor!";
        } ).withParameters( "namespace" );
        runAction.setOrder( 2 );
        im.registerInformation( runAction );

        InformationGroup logGroup = new InformationGroup( page, "Log Output" ).setOrder( 2 );
        im.addGroup( logGroup );
        informationLogOutput = new InformationCode( logGroup, "" );
        im.registerInformation( informationLogOutput );

        InformationGroup resultGroup = new InformationGroup( page, "Result" ).setOrder( 3 );
        im.addGroup( resultGroup );
        informationResult = new InformationCode( resultGroup, "" );
        im.registerInformation( informationResult );
    }

}
