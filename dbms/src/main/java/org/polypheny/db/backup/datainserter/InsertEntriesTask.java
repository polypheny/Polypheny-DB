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

package org.polypheny.db.backup.datainserter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.datasaver.BackupFileReader;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class InsertEntriesTask implements Runnable{
    TransactionManager transactionManager;
    File dataFile;
    DataModel dataModel;
    Long namespaceId;
    String namespaceName;
    String entityName;
    int nbrCols;

    public InsertEntriesTask( TransactionManager transactionManager, File dataFile, DataModel dataModel, Long namespaceId, String namespaceName, String entityName, int nbrCols ) {
        this.transactionManager = transactionManager;
        this.dataFile = dataFile;
        this.dataModel = dataModel;
        this.namespaceId = namespaceId;
        this.namespaceName = namespaceName;
        this.entityName = entityName;
        this.nbrCols = nbrCols;
    }


    @Override
    public void run() {
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        try(
                DataInputStream iin = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile), 32768));
                //BufferedReader bIn = new BufferedReader( new InputStreamReader( new BufferedInputStream( new FileInputStream( dataFile ), 32768 ) ) );
            )
        {
            BackupFileReader in = new BackupFileReader( dataFile );
            int counter = 0;
            String query = "";

            switch ( dataModel ) {
                case RELATIONAL:
                    String inLine = "";
                    String row = "";
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Inserter" );
                    statement = transaction.createStatement();
                    //build up row for query (since each value is one row in the file), and then execute query for each row
                    while ( (inLine = in.readLine()) != null ) {
                        counter++;
                        //PolyValue deserialized = PolyValue.deserialize( inLine );   //somehow only reads two first lines from table file and nothing else (if there is only doc file, it doesnt read)
                        PolyValue deserialized = PolyValue.fromTypedJson( inLine, PolyValue.class );

                        String value = deserialized.toJson();
                        value = value.replaceAll( "'", "''" );
                        if (PolyType.CHAR_TYPES.contains( deserialized.getType() ) || PolyType.DATETIME_TYPES.contains( deserialized.getType() ) ) {
                            value = String.format( "'%s'", value );
                        } else {
                            value = String.format( "%s", value );
                        }
                        row += value + ", ";

                        if (counter == nbrCols) {
                            row = row.substring( 0, row.length() - 2 ); // remove last ", "
                            query = String.format( "INSERT INTO %s.%s VALUES (%s)", namespaceName, entityName, row );
                            log.info( query );

                            ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );

                            counter = 0;
                            query = "";
                            row= "";
                        }

                    }
                    transaction.commit();
                    break;
                case DOCUMENT:
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Inserter" );
                    statement = transaction.createStatement();
                    while ( (inLine = in.readLine()) != null ) {
                        //PolyValue deserialized = PolyValue.deserialize( inLine );   //somehow only reads two first lines from table file and nothing else (if there is only doc file, it doesnt read)
                        PolyValue deserialized = PolyValue.fromTypedJson( inLine, PolyValue.class );
                        String value = deserialized.toJson();
                        query = String.format( "db.%s.insertOne(%s)", entityName, value );
                        log.info( query );

                        transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                        statement = transaction.createStatement();
                        ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "mql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                        transaction.commit();


                    }
                    transaction.commit();
                    break;
                case GRAPH:
                    //lkj
                    break;
                default:
                    throw new GenericRuntimeException( "Unknown data model" );
            }
            in.close();
            log.info( "data-insertion: end of thread for " + entityName );

            /*
            String test = bIn.readLine();
            StringTokenizer tk = new StringTokenizer(inLine);
            int a = Integer.parseInt(tk.nextToken()); // <-- read single word on inLine and parse to int
            log.info( test );

            byte[] bytes = new byte[1000];
            int whatever = in.read( bytes );
            String resultt = new String();
            for (byte b : bytes) {
                resultt += (char) b;
            }

             */



        } catch(Exception e){
            throw new GenericRuntimeException( "Error while inserting entries", e );
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }

}
