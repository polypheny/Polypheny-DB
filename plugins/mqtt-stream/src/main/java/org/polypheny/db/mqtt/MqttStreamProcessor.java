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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.stream.StreamProcessor;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;

@Slf4j
public class MqttStreamProcessor implements StreamProcessor {
    private final String filterQuery;
    private final StreamProcessor streamProcessor;
    private final Statement statement;


    public MqttStreamProcessor( MqttMessage mqttMessage, String filterQuery, Statement statement ) {
        this.filterQuery = filterQuery;
        this.streamProcessor = statement.getStreamProcessor( mqttMessage.getMessage() );
        this.statement = statement;
    }


    public boolean applyFilter() {
        AlgRoot root = processMqlQuery();
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
        log.info( res.toString() );
        return !res.isEmpty();

    }


    private AlgRoot processMqlQuery() {
        AlgBuilder algBuilder = AlgBuilder.create( this.statement );
        Processor mqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "mongo" ) );
        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        final AlgOptCluster cluster = AlgOptCluster.createDocument( statement.getQueryProcessor().getPlanner(), algBuilder.getRexBuilder() );
        MqlToAlgConverter mqlConverter = new MqlToAlgConverter( mqlProcessor, catalogReader, cluster );

        MqlFind find = (MqlFind) mqlProcessor.parse( String.format( "db.%s.find(%s)", "null", this.filterQuery ) ).get( 0 );
        String msg = getStream();
        AlgNode input;
        if ( msg.contains( "{" ) && msg.contains( "}" ) ) {
            // msg is in JSON format
            BsonDocument msgDoc = BsonDocument.parse(msg);
            input = LogicalDocumentValues.create( cluster, ImmutableList.of( msgDoc ) );
        } else if ( msg.contains( "[" ) && msg.contains( "]" ) ) {
            // msg is an array
            List<BsonValue> values = arrayToBsonList(msg);
            BsonDocument msgDoc = new BsonDocument( "$$ROOT", new BsonArray( values ) );
            input = LogicalDocumentValues.create( cluster, ImmutableList.of( msgDoc ) );
        } else {
            // msg is single value
            BsonDocument msgDoc = new BsonDocument( "$$ROOT", new BsonInt32( Integer.parseInt( msg ) ) );
            input = LogicalDocumentValues.create( cluster, ImmutableList.of( msgDoc ) );
        }


        return mqlConverter.convert( find, input );
    }


    /**
     * converts the array, currently a String into a list of BSON values
     * @param msg
     */
    private List<BsonValue> arrayToBsonList( String msg ) {
        msg = msg.replace( "[", "" ).replace( "]", "" );
        String[] array = msg.split( "," );
        List<BsonValue> list = new ArrayList<>(array.length);
        for ( String stringValue : array ) {
            //TODO: if BsonString dont work -> try with BsonInt
            BsonString bsonValue = new BsonString( stringValue.trim() );
            list.add( bsonValue );
        }
        return list;
    }


    @Override
    public String getStream() {
        return streamProcessor.getStream();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validateContent( String msg ) {
        //TODO: Implement
        return true;
    }


    List<List<Object>> executeAndTransformPolyAlg( AlgRoot algRoot, Statement statement, final Context ctx ) {

        try {
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, false );
            log.debug( "AlgRoot was prepared." );
            List<List<Object>> rows = result.getRows( statement, -1 );
            statement.getTransaction().commit();
            return rows;
        } catch ( Throwable e ) {
            log.error( "Error during execution of stream processor query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
    }

}