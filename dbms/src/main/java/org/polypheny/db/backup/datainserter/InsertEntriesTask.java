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
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.backup.BackupManager;
import org.polypheny.db.backup.datasaver.BackupFileReader;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
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
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.relational.PolyMap;

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
        Statement statement;
        PolyImplementation result;

        try(
                DataInputStream iin = new DataInputStream(new BufferedInputStream(new FileInputStream(dataFile), 32768));
                //BufferedReader bIn = new BufferedReader( new InputStreamReader( new BufferedInputStream( new FileInputStream( dataFile ), 32768 ) ) );
            )
        {
            BackupFileReader in = new BackupFileReader( dataFile );
            int elementCounter = 0;
            int batchCounter = 0;
            String query = "";

            switch ( dataModel ) {
                case RELATIONAL:
                    String inLine = "";
                    String row = "";
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Inserter" ); //FIXME: bruuche en transaction för jede batch, bzw sobald transaction commited... passts...... commit erscht am schloss....
                    //statement = transaction.createStatement();

                    //statement.getDataContext().setParameterTypes(  );
                    //statement.getDataContext().setParameterValues(  );//liste
                    //ImplementationContext lol =  LanguageManager.getINSTANCE().anyPrepareQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                    //Statement statementTrue = lol.getStatement();

                    String relValues = "";
                    //build up row for query (since each value is one row in the file), and then execute query for each row
                    while ( (inLine = in.readLine()) != null ) {
                        elementCounter++;
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

                        if (elementCounter == nbrCols) {
                            row = row.substring( 0, row.length() - 2 ); // remove last ", "
                            //query = String.format( "INSERT INTO %s.%s VALUES (%s)", namespaceName, entityName, row );
                            //log.info( row );

                            //ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                            //relValues = relValues + row + ", ";
                            row = String.format( "(%s), ", row );
                            relValues = relValues + row;
                            //log.info( relValues );
                            elementCounter = 0;
                            batchCounter ++;
                            //query = "";
                            row= "";



                        }

                        if (batchCounter == BackupManager.batchSize) {
                            //log.info( "in batchcounter: " + relValues );
                            relValues = relValues.substring( 0, relValues.length() - 2 ); // remove last ", "
                            query = String.format( "INSERT INTO %s.%s VALUES %s", namespaceName, entityName, relValues );

                            log.info( query );
                            try {
                                ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                                        .anyQuery(
                                                QueryContext.builder()
                                                        .language( QueryLanguage.from( "sql" ) )
                                                        .query( query ).origin( "Backup Manager" )
                                                        .transactionManager( transactionManager )
                                                        //.statement( statement )
                                                        .namespaceId( namespaceId )
                                                        .build()
                                                        .addTransaction( transaction ) ).get( 0 );
                            } catch ( Exception e) {
                                throw new GenericRuntimeException("Could not insert relational backup data from query: " + query + " with error message:" + e.getMessage());
                            }

                            batchCounter = 0;
                            relValues = "";
                        }

                    }
                    if ( batchCounter != 0 ) {
                        //execute the statement with the remaining values
                        relValues = relValues.substring( 0, relValues.length() - 2 ); // remove last ", "
                        query = String.format( "INSERT INTO %s.%s VALUES %s", namespaceName, entityName, relValues );

                        log.info( "rest: " + query );
                        try {
                            ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                                    .anyQuery(
                                            QueryContext.builder()
                                                    .language( QueryLanguage.from( "sql" ) )
                                                    .query( query )
                                                    .origin( "Backup Manager" )
                                                    .transactionManager( transactionManager )
                                                    .namespaceId( namespaceId )
                                                    //.statement( statement )
                                                    .build().addTransaction( transaction ) ).get( 0 );
                        } catch ( Exception e ) {
                            throw new GenericRuntimeException("Could not insert relational backup data from query: " + query + " with error message:" + e.getMessage());

                        }

                        // ImplementationContext lol =  LanguageManager.getINSTANCE().anyPrepareQuery( QueryContext.builder().language( QueryLanguage.from( "sql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                        //lol.getStatement()
                        //statementTrue - values iifüege wie obe
                        batchCounter = 0;
                        query = "";
                    }
                    transaction.commit();
                    batchCounter = 0;
                    break;
                case DOCUMENT:
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Inserter" );
                    //statement = transaction.createStatement();
                    String docValues = "";
                    while ( (inLine = in.readLine()) != null ) {
                        //PolyValue deserialized = PolyValue.deserialize( inLine );   //somehow only reads two first lines from table file and nothing else (if there is only doc file, it doesnt read)
                        PolyValue deserialized = PolyValue.fromTypedJson( inLine, PolyValue.class );
                        String value = deserialized.toJson();
                        docValues += value + ", ";
                        batchCounter++;
                        //query = String.format( "db.%s.insertOne(%s)", entityName, value );

                        //transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Gatherer" );
                        //statement = transaction.createStatement();
                        //ExecutedContext executedQuery = LanguageManager.getINSTANCE().anyQuery( QueryContext.builder().language( QueryLanguage.from( "mql" ) ).query( query ).origin( "Backup Manager" ).transactionManager( transactionManager ).namespaceId( namespaceId ).build(), statement ).get( 0 );
                        //transaction.commit();

                        if (batchCounter == BackupManager.batchSize) {
                            // remove the last ", " from the string
                            //statement = transaction.createStatement();
                            docValues = docValues.substring( 0, docValues.length() - 2 );

                            query = String.format( "db.%s.insertMany([%s])", entityName, docValues );
                            log.info( query );
                            try {
                                ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                                        .anyQuery(
                                                QueryContext.builder()
                                                        .language( QueryLanguage.from( "mql" ) )
                                                        .query( query ).origin( "Backup Manager" )
                                                        .transactionManager( transactionManager )
                                                        .namespaceId( namespaceId )
                                                        //.statement( statement )
                                                        .build()
                                                        .addTransaction( transaction ) ).get( 0 );
                            } catch ( Exception e ) {
                                throw new GenericRuntimeException("Could not insert document backup data from query: " + query + " with error message:" + e.getMessage());

                            }

                            batchCounter = 0;
                            docValues = "";
                            query = "";
                        }

                    }

                    if (batchCounter != 0) {
                        //statement = transaction.createStatement();
                        // remove the last ", " from the string
                        docValues = docValues.substring( 0, docValues.length() - 2 );

                        query = String.format( "db.%s.insertMany([%s])", entityName, docValues );
                        log.info( "rest: " + query );
                        try {
                            ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                                    .anyQuery(
                                            QueryContext.builder()
                                                    .language( QueryLanguage.from( "mql" ) )
                                                    .query( query ).origin( "Backup Manager" )
                                                    .transactionManager( transactionManager )
                                                    .namespaceId( namespaceId )
                                                    //.statement( statement )
                                                    .build().addTransaction( transaction ) ).get( 0 );
                        } catch ( Exception e ) {
                            throw new GenericRuntimeException("Could not insert document backup data from query: " + query + " with error message:" + e.getMessage());

                        }

                        batchCounter = 0;
                        docValues = "";
                        query = "";
                    }

                    transaction.commit();
                    break;
                case GRAPH:
                    // not batched
                    transaction = transactionManager.startTransaction( Catalog.defaultUserId, false, "Backup Entry-Inserter" );
                    String graphValues = "";
                    while ( (inLine = in.readLine()) != null ) {
                        PolyValue deserialized = PolyValue.fromTypedJson( inLine, PolyGraph.class ); //--> deserialized is null??
                        String value = deserialized.toJson();
                        graphValues += value + ", ";
                        int nodeCounter = 0;
                        // id of the node and the created label of the node
                        HashMap<PolyString, String> nodeMap = new HashMap<>();
                        batchCounter++;
                        String nodesString = "";
                        String edgesString = "";
                        //query = String.format( "db.%s.insertOne(%s)", entityName, value );

                        @NotNull PolyGraph graph = deserialized.asGraph();
                        @NotNull PolyMap<PolyString, PolyNode> nodes = graph.getNodes();
                        @NotNull PolyMap<PolyString, PolyEdge> edges = graph.getEdges();
                        //List<PolyNode> nodes = deserialized.asList().stream().filter( v -> v.isNode() ).map( v -> v.asNode() ).collect( Collectors.toList() );

                        // go through and create all nodes
                        for (PolyNode node : nodes.values()) {
                            String labels = getLabels( node.labels );
                            String properties = getProperties( node.properties );

                            // remove the "-" from the id
                            String nString = "n" + String.valueOf( nodeCounter );
                            nodeMap.put( node.id, nString );
                            nodeCounter++;

                            nodesString += String.format( "(%s:%s {_id:'%s', %s}), ", nString, labels, node.id, properties );

                        }

                        // go through all edges
                        for ( PolyEdge edge : edges.values() ) {
                            String labels = getLabels( edge.labels );
                            String properties = getProperties( edge.properties );
                            String source = nodeMap.get( edge.source );
                            String target = nodeMap.get( edge.target );

                            if ( !properties.isEmpty() ) {
                                properties = String.format( "'%s', %s", edge.id, properties );
                            } else {
                                properties = String.format( "'%s'", edge.id );
                            }

                            edgesString += String.format( "(%s)-[:%s {_id:%s}]->(%s), ", source, labels, properties, target );

                        }

                        // remove the last ", " from the string
                        nodesString = nodesString.substring( 0, nodesString.length() - 2 );
                        query = String.format( "CREATE %s", nodesString );
                        if ( !edgesString.isEmpty() ) {
                            // remove the last ", " from the string
                            edgesString = edgesString.substring( 0, edgesString.length() - 2 );
                            query += ", " + edgesString;
                        }

                        log.info( query );
                        ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                                .anyQuery(
                                        QueryContext.builder()
                                                .language( QueryLanguage.from( "cypher" ) )
                                                .query( query ).origin( "Backup - Insert Graph Entries" )
                                                .transactionManager( transactionManager )
                                                .namespaceId( namespaceId )
                                                .build()
                                                .addTransaction( transaction ) ).get( 0 );


                        /*
                        if (batchCounter == BackupManager.batchSize) {
                            // remove the last ", " from the string
                            graphValues = graphValues.substring( 0, graphValues.length() - 2 );
                            log.info( graphValues );

                            query = String.format( "db.%s.insertMany([%s])", entityName, graphValues );
                            log.info( query );
                            ExecutedContext executedQuery2 = LanguageManager.getINSTANCE()
                                    .anyQuery(
                                            QueryContext.builder()
                                                    .language( QueryLanguage.from( "mql" ) )
                                                    .query( query ).origin( "Backup Manager" )
                                                    .transactionManager( transactionManager )
                                                    .namespaceId( namespaceId )
                                                    //.statement( statement )
                                                    .build()
                                                    .addTransaction( transaction ) ).get( 0 );
                            batchCounter = 0;
                            graphValues = "";
                            query = "";
                        }

                         */

                    }

                    /*
                    if (batchCounter != 0) {
                        //statement = transaction.createStatement();
                        // remove the last ", " from the string
                        graphValues = graphValues.substring( 0, graphValues.length() - 2 );

                        query = String.format( "db.%s.insertMany([%s])", entityName, graphValues );
                        log.info( "rest: " + query );
                        ExecutedContext executedQuery = LanguageManager.getINSTANCE()
                                .anyQuery(
                                        QueryContext.builder()
                                                .language( QueryLanguage.from( "mql" ) )
                                                .query( query ).origin( "Backup Manager" )
                                                .transactionManager( transactionManager )
                                                .namespaceId( namespaceId )
                                                //.statement( statement )
                                                .build().addTransaction( transaction ) ).get( 0 );
                        batchCounter = 0;
                        graphValues = "";
                        query = "";
                    }

                     */

                    transaction.commit();
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
            throw new GenericRuntimeException( "Error while inserting entries: " + e.getMessage() );
        } catch ( TransactionException e ) {
            throw new GenericRuntimeException( "Error while inserting entries: " + e.getMessage() );
        }
    }


    /**
     * Gets the labels of a node as a list of strings seperated by a comma
     * @param labels a PolyList<PolyString> of labels
     * @return String of labels, seperated by a comma
     */
    String getLabels( PolyList<PolyString> labels ) {
        String labelsString = "";
        for ( PolyString label : labels ) {
            labelsString += label + ", ";
        }
        if ( !labelsString.isEmpty() ) {
            labelsString = labelsString.substring( 0, labelsString.length() - 2 );
        }
        return labelsString;
    }


    /**
     * Gets the properties of a node as a string
     * @param properties a PolyDictionary of properties
     * @return String of properties
     */
    String getProperties( PolyDictionary properties ) {
        String propsString = "";
        for ( PolyString key : properties.keySet() ) {
            propsString += String.format( "%s: '%s', ", key, properties.get( key ) );
        }

        if ( !propsString.isEmpty() ) {
            propsString = propsString.substring( 0, propsString.length() - 2 );
        }

        return propsString;
    }


}
