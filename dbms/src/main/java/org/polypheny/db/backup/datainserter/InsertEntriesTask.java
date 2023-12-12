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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

@Slf4j
public class InsertEntriesTask implements Runnable{
    File dataFile = null;
    DataModel dataModel = null;
    Long namespaceId = null;
    String namespaceName = null;
    String collectionName = null;
    String tableName = null;

    public InsertEntriesTask( File dataFile, DataModel dataModel, Long namespaceId, String collectionName ) {
        this.dataFile = dataFile;
        this.dataModel = dataModel;
        this.namespaceId = namespaceId;
        this.collectionName = collectionName;
    }

    public InsertEntriesTask( File dataFile, DataModel dataModel, String namespaceName, String tableName ) {
        this.dataFile = dataFile;
        this.dataModel = dataModel;
        this.namespaceName = namespaceName;
        this.tableName = tableName;
    }


    @Override
    public void run() {
        Transaction transaction;
        Statement statement = null;
        PolyImplementation result;

        try(
                DataInputStream in = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile), 32768));
                BufferedReader bIn = new BufferedReader( new InputStreamReader( new BufferedInputStream( new FileInputStream( dataFile ), 32768 ) ) );
            )
        {
            switch ( dataModel ) {
                case RELATIONAL:
                    //lkj
                    break;
                case DOCUMENT:
                    //lkj
                    break;
                case GRAPH:
                    //lkj
                    break;
                default:
                    throw new GenericRuntimeException( "Unknown data model" );
            }



            String line = "";
            String query = "";
            DataModel dataModel = null;
            while ( (line = bIn.readLine()) != null ) {
                //PolyValue deserialized = PolyValue.deserialize( line );   //somehow only reads two first lines from table file and nothing else (if there is only doc file, it doesnt read)
                PolyValue deserialized = PolyValue.fromTypedJson( line, PolyValue.class );

                String value = deserialized.toJson();
                value = value.replaceAll( "'", "''" );
                if (PolyType.CHAR_TYPES.contains( deserialized.getType() ) || PolyType.DATETIME_TYPES.contains( deserialized.getType() ) ) {
                    query = String.format( " DEFAULT '%s'", value );
                } else {
                    query = String.format( " DEFAULT %s", value );
                }
                log.info( value );
            }



            /*
            String test = bIn.readLine();
            StringTokenizer tk = new StringTokenizer(line);
            int a = Integer.parseInt(tk.nextToken()); // <-- read single word on line and parse to int
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
        }
    }

}
