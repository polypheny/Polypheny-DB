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

package org.polypheny.db.mqtt;

import javafx.util.converter.LocalTimeStringConverter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.PolyphenyHomeDirManager;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Slf4j
public class StreamCapture {
    TransactionManager transactionManager;
    PolyphenyHomeDirManager homeDirManager;
    String namespace;

    StreamCapture (final TransactionManager transactionManager, String namespace) {
        this.transactionManager = transactionManager;
        this.namespace = namespace;
    }


    public static void saveMsgInDocument(String namespace, String topic, String message) {
        String path = registerTopicFolder(topic);
        saveMsgInFile(path, message);
        createCollection(namespace, topic);
    }


    static void createCollection(String namespace, String topic) {
        /**
        Transaction transaction = getTransaction();
        Statement statement = transaction.createStatement();
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = transaction.getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        List<String> names = List.of(namespace, );
        AlgOptTable collection = catalogReader.getCollection()
         **/

/**
        Catalog catalog = Catalog.getInstance();
        AdapterManager adapterManager = AdapterManager.getInstance();

        long schemaId;
        try {
            schemaId = catalog.getSchema( Catalog.defaultDatabaseId, ((MqlQueryParameters) parameters).getDatabase() ).id;
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "The used document database (Polypheny Schema) is not available." );
        }

        Catalog.PlacementType placementType = Catalog.PlacementType.AUTOMATIC;

        try {
            List<DataStore> dataStores = stores
                    .stream()
                    .map( store -> (DataStore) adapterManager.getAdapter( store ) )
                    .collect( Collectors.toList() );
            DdlManager.getInstance().createCollection(
                    schemaId,
                    topic,
                    true,
                    dataStores.size() == 0 ? null : dataStores,
                    placementType,
                    statement );
        } catch ( EntityAlreadyExistsException e ) {
            throw new RuntimeException( "The generation of the collection was not possible, due to: " + e.getMessage() );
        }
 **/

    }

    private static String registerTopicFolder(String topic) {
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();

        String path = File.separator + "mqttStreamPlugin" + File.separator + topic.replace("/", File.separator);;

        File file = null;
        if ( !homeDirManager.checkIfExists(path) ) {
            file = homeDirManager.registerNewFolder(path);
            log.info( "New Directory created!" );
        } else {
            //TODO: rmv log
            log.info("Directory already exists");
        }

        return path;

    }

    static boolean saveMsgInFile (String path, String msg) {
        PolyphenyHomeDirManager homeDirManager = PolyphenyHomeDirManager.getInstance();
        String filePath = path + File.separator + System.currentTimeMillis() + ".txt";
        File msgFile = homeDirManager.registerNewFile(filePath);
        if ( msgFile.canWrite() ) {
            //msgFile.
        } else {
            log.error( "Cannot write write in " );
        }
        return true;
    }


    private void getTransaction() {
        /**
        try {
            return transactionManager.startTransaction( userId, databaseId, false, "Stream Processing", Transaction.MultimediaFlavor.FILE );
        } catch (UnknownUserException | UnknownDatabaseException | UnknownSchemaException e ) {
            throw new RuntimeException( "Error while starting transaction", e );
        }
         **/
    }
}
