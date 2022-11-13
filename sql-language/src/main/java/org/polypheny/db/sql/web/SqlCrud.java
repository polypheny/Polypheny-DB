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

package org.polypheny.db.sql.web;

import static org.polypheny.db.webui.HttpServer.gson;

import au.com.bytecode.opencsv.CSVReader;
import com.google.gson.JsonSyntaxException;
import io.javalin.http.Context;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import kong.unirest.HttpResponse;
import kong.unirest.Unirest;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringEscapeUtils;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.operators.ChainedOperatorTable;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.config.PolyphenyDbConnectionProperty;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.languages.sql.parser.impl.SqlParserImpl;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.sql.SqlProcessorImpl;
import org.polypheny.db.sql.SqlRegisterer;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.fun.SqlStdOperatorTable;
import org.polypheny.db.sql.language.validate.PolyphenyDbSqlValidator;
import org.polypheny.db.sql.web.SchemaToJsonMapper.JsonColumn;
import org.polypheny.db.sql.web.SchemaToJsonMapper.JsonTable;
import org.polypheny.db.sql.web.hub.HubMeta;
import org.polypheny.db.sql.web.hub.HubMeta.TableMapping;
import org.polypheny.db.sql.web.hub.HubRequest;
import org.polypheny.db.sql.web.hub.HubResult;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.Crud.QueryExecutionException;
import org.polypheny.db.webui.HttpServer;
import org.polypheny.db.webui.HttpServer.HandlerType;
import org.polypheny.db.webui.WebSocket;
import org.polypheny.db.webui.crud.LanguageCrud;
import org.polypheny.db.webui.models.Result;
import org.polypheny.db.webui.models.Status;
import org.polypheny.db.webui.models.requests.UIRequest;

@Slf4j
public class SqlCrud {

    static {
        HttpServer.getInstance().addRoute( "/importDataset", SqlCrud::importDataset, HubRequest.class, HandlerType.POST );

        HttpServer.getInstance().addSerializedRoute( "/exportTable", SqlCrud::exportTable, HandlerType.POST );

        Catalog.QueryLanguage.addQueryLanguage(
                NamespaceType.RELATIONAL,
                "sql",
                SqlParserImpl.FACTORY,
                SqlProcessorImpl::new,
                SqlCrud::getSqlValidator );

        LanguageCrud.REGISTER.put( "sql", (
                session,
                request,
                transactionManager,
                userId,
                databaseId,
                c ) -> LanguageCrud.getCrud().anySqlQuery( request, session ) );

        if ( !SqlRegisterer.isInit() ) {
            SqlRegisterer.registerOperators();
        }

        // webuiServer.post( "/importDataset", crud::importDataset );

        // webuiServer.post( "/exportTable", crud::exportTable );
    }


    public static <T> T fun( Class<T> operatorTableClass, T defaultOperatorTable ) {
        final String fun = PolyphenyDbConnectionProperty.FUN.wrap( new Properties() ).getString();
        if ( fun == null || fun.equals( "" ) || fun.equals( "standard" ) ) {
            return defaultOperatorTable;
        }
        final Collection<OperatorTable> tables = new LinkedHashSet<>();
        for ( String s : fun.split( "," ) ) {
            operatorTable( s, tables );
        }
        tables.add( SqlStdOperatorTable.instance() );
        return operatorTableClass.cast( ChainedOperatorTable.of( tables.toArray( new OperatorTable[0] ) ) );
    }


    public static void operatorTable( String s, Collection<OperatorTable> tables ) {
        switch ( s ) {
            case "standard":
                tables.add( SqlStdOperatorTable.instance() );
                return;
            case "oracle":
                tables.add( OracleSqlOperatorTable.instance() );
                return;
            //case "spatial":
            //    tables.add( PolyphenyDbCatalogReader.operatorTable( GeoFunctions.class.getName() ) );
            //    return;
            default:
                throw new IllegalArgumentException( "Unknown operator table: " + s );
        }
    }


    private static PolyphenyDbSqlValidator getSqlValidator( org.polypheny.db.prepare.Context context, PolyphenyDbCatalogReader catalogReader ) {

        final OperatorTable opTab0 = fun( OperatorTable.class, SqlStdOperatorTable.instance() );
        final OperatorTable opTab = ChainedOperatorTable.of( opTab0, catalogReader );
        final JavaTypeFactory typeFactory = context.getTypeFactory();
        final Conformance conformance = context.config().conformance();
        return new PolyphenyDbSqlValidator( opTab, catalogReader, typeFactory, conformance );
    }


    private static HubResult createTableFromJson( final String json, final String newName, final HubRequest request, final Transaction transaction, Crud crud ) throws QueryExecutionException {
        // create table from .json file
        List<CatalogTable> tablesInSchema = Catalog.getInstance().getTables( crud.getDatabaseId(), new Catalog.Pattern( request.schema ), null );
        int tableAlreadyExists = (int) tablesInSchema.stream().filter( t -> t.name.equals( newName ) ).count();
        if ( tableAlreadyExists > 0 ) {
            return new HubResult( String.format( "Cannot import the dataset since the schema '%s' already contains a entity with the name '%s'", request.schema, newName ) );
        }

        String createTable = SchemaToJsonMapper.getCreateTableStatementFromJson( json, request.createPks, request.defaultValues, request.schema, newName, request.store );
        crud.executeSqlUpdate( transaction, createTable );
        return null;
    }


    /**
     * Import a dataset from Polypheny-Hub into Polypheny-DB
     */
    static HubResult importDataset( HubRequest request, final Crud crud ) {

        String error = null;

        String randomFileName = UUID.randomUUID().toString();
        final String sysTempDir = System.getProperty( "java.io.tmpdir" );
        final File tempDir = new File( sysTempDir + File.separator + "hub" + File.separator + randomFileName + File.separator );
        if ( !tempDir.mkdirs() ) { // create folder
            log.error( "Unable to create temp folder: {}", tempDir.getAbsolutePath() );
            return new HubResult( "Unable to create temp folder" );
        }

        // see: https://www.baeldung.com/java-download-file
        final File zipFile = new File( tempDir, "import.zip" );
        Transaction transaction = null;
        try (
                BufferedInputStream in = new BufferedInputStream( new URL( request.url ).openStream() );
                FileOutputStream fos = new FileOutputStream( zipFile )
        ) {
            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ( (bytesRead = in.read( dataBuffer, 0, 1024 )) != -1 ) {
                fos.write( dataBuffer, 0, bytesRead );
            }

            // extract zip, see https://www.baeldung.com/java-compress-and-uncompress
            dataBuffer = new byte[1024];
            final File extractedFolder = new File( tempDir, "import" );
            if ( !extractedFolder.mkdirs() ) {
                log.error( "Unable to create folder for extracting files: {}", tempDir.getAbsolutePath() );
                return new HubResult( "Unable to create folder for extracting files" );
            }
            try ( ZipInputStream zis = new ZipInputStream( new FileInputStream( zipFile ) ) ) {
                ZipEntry zipEntry = zis.getNextEntry();
                while ( zipEntry != null ) {
                    File newFile = new File( extractedFolder, zipEntry.getName() );
                    try ( FileOutputStream fosEntry = new FileOutputStream( newFile ) ) {
                        int len;
                        while ( (len = zis.read( dataBuffer )) > 0 ) {
                            fosEntry.write( dataBuffer, 0, len );
                        }
                    }
                    zipEntry = zis.getNextEntry();
                }
            }

            // delete .zip after unzipping
            if ( !zipFile.delete() ) {
                log.error( "Unable to delete zip file: {}", zipFile.getAbsolutePath() );
            }

            Status status = new Status( "tableImport", request.tables.size() );
            int ithTable = 0;
            for ( TableMapping m : request.tables.values() ) {
                //create table from json
                Path jsonPath = Paths.get( new File( extractedFolder, m.initialName + ".json" ).getPath() );
                String json = new String( Files.readAllBytes( jsonPath ), StandardCharsets.UTF_8 );
                JsonTable table = gson.fromJson( json, JsonTable.class );
                String newName = m.newName != null ? m.newName : table.tableName;
                assert (table.tableName.equals( m.initialName ));

                transaction = crud.getTransaction();
                HubResult createdTableError = createTableFromJson( json, newName, request, transaction, crud );
                transaction.commit();
                if ( createdTableError != null ) {
                    transaction.rollback();
                    return createdTableError;
                    //todo check
                }

                // Import data from .csv file
                transaction = crud.getTransaction();
                importCsvFile( m.initialName + ".csv", table, transaction, extractedFolder, request, newName, status, ithTable++, crud );
                transaction.commit();
            }
        } catch ( IOException | TransactionException e ) {
            log.error( "Could not import dataset", e );
            error = "Could not import dataset" + e.getMessage();
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rolling back transaction", e );
                }
            }
        } catch ( QueryExecutionException e ) {
            log.error( "Could not create table from imported json file", e );
            error = "Could not create table from imported json file" + e.getMessage();
            if ( transaction != null ) {
                try {
                    transaction.rollback();
                } catch ( TransactionException ex ) {
                    log.error( "Caught exception while rolling back transaction", e );
                }
            }
            //} catch ( CsvValidationException | GenericCatalogException e ) {
        } finally {
            // Delete temp folder
            if ( !deleteDirectory( tempDir ) ) {
                log.error( "Unable to delete temp folder: {}", tempDir.getAbsolutePath() );
            }
        }

        if ( error != null ) {
            return new HubResult( error );
        } else {
            return new HubResult().setMessage( String.format( "Imported dataset into entity %s on store %s", request.schema, request.store ) );
        }
    }


    /**
     * Helper function to delete a directory.
     * Taken from https://www.baeldung.com/java-delete-directory
     */
    static boolean deleteDirectory( final File directoryToBeDeleted ) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if ( allContents != null ) {
            for ( File file : allContents ) {
                deleteDirectory( file );
            }
        }
        return directoryToBeDeleted.delete();
    }


    /**
     * Export a table into a .zip consisting of a json file containing information of the table and columns and a csv files with the data
     */
    static Result exportTable( final Context ctx, final Crud crud ) {
        HubRequest request = ctx.bodyAsClass( HubRequest.class );
        Transaction transaction = crud.getTransaction( false, true );
        Statement statement = transaction.createStatement();
        HubMeta metaData = new HubMeta( request.schema );

        String randomFileName = UUID.randomUUID().toString();
        final Charset charset = StandardCharsets.UTF_8;
        final String sysTempDir = System.getProperty( "java.io.tmpdir" );
        final File tempDir = new File( sysTempDir + File.separator + "hub" + File.separator + randomFileName + File.separator );
        if ( !tempDir.mkdirs() ) { // create folder
            log.error( "Unable to create temp folder: {}", tempDir.getAbsolutePath() );
            return new Result( "Unable to create temp folder" );
        }
        File tableFile;
        File catalogFile;
        ArrayList<File> tableFiles = new ArrayList<>();
        ArrayList<File> catalogFiles = new ArrayList<>();
        final int BATCH_SIZE = RuntimeConfig.HUB_IMPORT_BATCH_SIZE.getInteger();
        int ithTable = 0;
        Status status = new Status( "tableExport", request.tables.size() );
        try {
            for ( TableMapping table : request.tables.values() ) {
                tableFile = new File( tempDir, table.initialName + ".csv" );
                catalogFile = new File( tempDir, table.initialName + ".json" );
                tableFiles.add( tableFile );
                catalogFiles.add( catalogFile );
                OutputStreamWriter catalogWriter = new OutputStreamWriter( new FileOutputStream( catalogFile ), charset );
                FileOutputStream tableStream = new FileOutputStream( tableFile );
                log.info( String.format( "Exporting %s.%s", request.schema, table.initialName ) );
                CatalogTable catalogTable = Catalog.INSTANCE.getTable( crud.getDatabaseId(), request.schema, table.initialName );

                catalogWriter.write( SchemaToJsonMapper.exportTableDefinitionAsJson( catalogTable, request.createPks, request.defaultValues ) );
                catalogWriter.flush();
                catalogWriter.close();

                String query = String.format( "SELECT * FROM \"%s\".\"%s\"", request.schema, table.initialName );
                // TODO use iterator instead of Result
                Result tableData = crud.executeSqlSelect( statement, new UIRequest(), query, true );

                int totalRows = tableData.getData().length;
                int counter = 0;
                for ( String[] row : tableData.getData() ) {
                    int cols = row.length;
                    for ( int i = 0; i < cols; i++ ) {
                        if ( row[i].contains( "\n" ) ) {
                            String line = String.format( "\"%s\"", row[i] );
                            tableStream.write( line.getBytes( charset ) );
                        } else {
                            tableStream.write( row[i].getBytes( charset ) );
                        }
                        if ( i != cols - 1 ) {
                            tableStream.write( ",".getBytes( charset ) );
                        } else {
                            tableStream.write( "\n".getBytes( charset ) );
                        }
                    }
                    counter++;
                    if ( counter % BATCH_SIZE == 0 ) {
                        status.setStatus( counter, totalRows, ithTable );
                        WebSocket.broadcast( gson.toJson( status, Status.class ) );
                    }
                }
                status.setStatus( counter, totalRows, ithTable );
                WebSocket.broadcast( gson.toJson( status, Status.class ) );
                tableStream.flush();
                tableStream.close();
                metaData.addTable( table.initialName, counter );
                ithTable++;
            }
            status.complete();

            File zipFile = new File( tempDir, "table.zip" );
            FileOutputStream zipStream = new FileOutputStream( zipFile );
            //from https://www.baeldung.com/java-compress-and-uncompress
            ArrayList<File> allFiles = new ArrayList<>( tableFiles );
            allFiles.addAll( catalogFiles );
            try ( ZipOutputStream zipOut = new ZipOutputStream( zipStream, charset ) ) {
                for ( File fileToZip : allFiles ) {
                    try ( FileInputStream fis = new FileInputStream( fileToZip ) ) {
                        ZipEntry zipEntry = new ZipEntry( fileToZip.getName() );
                        zipOut.putNextEntry( zipEntry );

                        byte[] bytes = new byte[1024];
                        int length;
                        while ( (length = fis.read( bytes )) >= 0 ) {
                            zipOut.write( bytes, 0, length );
                        }
                    }
                }
                zipOut.finish();
            }
            zipStream.close();

            metaData.setFileSize( zipFile.length() );
            File metaFile = new File( tempDir, "meta.json" );
            FileOutputStream metaOutputStream = new FileOutputStream( metaFile );
            metaOutputStream.write( gson.toJson( metaData, HubMeta.class ).getBytes() );
            metaOutputStream.flush();
            metaOutputStream.close();

            //send file to php backend using Unirest
            HttpResponse<String> jsonResponse = Unirest.post( request.hubLink )
                    .field( "action", "uploadDataset" )
                    .field( "userId", String.valueOf( request.userId ) )
                    .field( "secret", request.secret )
                    .field( "name", request.name )
                    .field( "description", request.description )
                    .field( "pub", String.valueOf( request.pub ) )
                    .field( "dataset", zipFile )
                    .field( "metaData", metaFile )
                    .asString();

            // Get result
            String resultString = jsonResponse.getBody();
            log.info( String.format( "Exported %s.[%s]", request.schema, request.tables.values().stream().map( n -> n.initialName ).collect( Collectors.joining( "," ) ) ) );

            try {
                return new Result( resultString );//ctx.result( resultString );
            } catch ( JsonSyntaxException e ) {
                return new Result( resultString );
            }
        } catch ( IOException e ) {
            log.error( "Failed to write temporary file", e );
            ctx.status( 400 ).json( "Failed to write temporary file" );
        } catch ( Exception e ) {
            log.error( "Error while exporting table", e );
            ctx.status( 400 ).json( new Result( "Error while exporting table" ) );
        } finally {
            // delete temp folder
            if ( !deleteDirectory( tempDir ) ) {
                log.error( "Unable to delete temp folder: {}", tempDir.getAbsolutePath() );
            }
            try {
                transaction.commit();
            } catch ( TransactionException e ) {
                log.error( "Error while fetching table", e );
                try {
                    transaction.rollback();
                } catch ( TransactionException transactionException ) {
                    log.error( "Exception while rollback", transactionException );
                }
            }
        }
        return null;
    }


    private static void importCsvFile( final String csvFileName, final JsonTable table, final Transaction transaction, final File extractedFolder, final HubRequest request, final String tableName, final Status status, final int ithTable, Crud crud ) throws IOException, QueryExecutionException {
        StringJoiner columnJoiner = new StringJoiner( ",", "(", ")" );
        for ( JsonColumn col : table.getColumns() ) {
            columnJoiner.add( "\"" + col.columnName + "\"" );
        }
        String columns = columnJoiner.toString();
        StringJoiner valueJoiner = new StringJoiner( ",", "VALUES", "" );
        StringJoiner rowJoiner;

        //see https://www.callicoder.com/java-read-write-csv-file-opencsv/

        final int BATCH_SIZE = RuntimeConfig.HUB_IMPORT_BATCH_SIZE.getInteger();
        long csvCounter = 0;
        try (
                Reader reader = new BufferedReader( new FileReader( new File( extractedFolder, csvFileName ) ) );
                CSVReader csvReader = new CSVReader( reader )
        ) {
            long lineCount = Files.lines( new File( extractedFolder, csvFileName ).toPath() ).count();
            String[] nextRecord;
            while ( (nextRecord = csvReader.readNext()) != null ) {
                rowJoiner = new StringJoiner( ",", "(", ")" );
                for ( int i = 0; i < table.getColumns().size(); i++ ) {
                    if ( PolyType.get( table.getColumns().get( i ).type ).getFamily() == PolyTypeFamily.CHARACTER ) {
                        rowJoiner.add( "'" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else if ( PolyType.get( table.getColumns().get( i ).type ) == PolyType.DATE ) {
                        rowJoiner.add( "date '" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else if ( PolyType.get( table.getColumns().get( i ).type ) == PolyType.TIME ) {
                        rowJoiner.add( "time '" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else if ( PolyType.get( table.getColumns().get( i ).type ) == PolyType.TIMESTAMP ) {
                        rowJoiner.add( "timestamp '" + StringEscapeUtils.escapeSql( nextRecord[i] ) + "'" );
                    } else {
                        rowJoiner.add( nextRecord[i] );
                    }
                }
                valueJoiner.add( rowJoiner.toString() );
                csvCounter++;
                if ( csvCounter % BATCH_SIZE == 0 && csvCounter != 0 ) {
                    String insertQuery = String.format( "INSERT INTO \"%s\".\"%s\" %s %s", request.schema, tableName, columns, valueJoiner.toString() );
                    crud.executeSqlUpdate( transaction, insertQuery );
                    valueJoiner = new StringJoiner( ",", "VALUES", "" );
                    status.setStatus( csvCounter, lineCount, ithTable );
                    WebSocket.broadcast( gson.toJson( status, Status.class ) );
                }
            }
            if ( csvCounter % BATCH_SIZE != 0 ) {
                String insertQuery = String.format( "INSERT INTO \"%s\".\"%s\" %s %s", request.schema, tableName, columns, valueJoiner.toString() );
                crud.executeSqlUpdate( transaction, insertQuery );
                status.setStatus( csvCounter, lineCount, ithTable );
                WebSocket.broadcast( gson.toJson( status, Status.class ) );
            }
        }
    }

}
