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
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonString;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.MqlFind;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.stream.StreamProcessorImpl;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;

@Slf4j
public class MqttStreamProcessor extends StreamProcessorImpl {

    private final String filterQuery;
    private final Statement statement;


    public MqttStreamProcessor( FilteringMqttMessage filteringMqttMessage, Statement statement ) {
        super( filteringMqttMessage.getMessage() );
        this.filterQuery = filteringMqttMessage.getQuery();
        this.statement = statement;
    }


    public boolean applyFilter() {
        AlgRoot root = processMqlQuery();
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement );
        log.info( res.toString() );
        return res.size() != 0;
    }


    private AlgRoot processMqlQuery() {
        AlgBuilder algBuilder = AlgBuilder.create( this.statement );
        Processor mqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "mongo" ) );
        PolyphenyDbCatalogReader catalogReader = statement.getTransaction().getCatalogReader();
        final AlgOptCluster cluster = AlgOptCluster.createDocument( statement.getQueryProcessor().getPlanner(), algBuilder.getRexBuilder() );
        MqlToAlgConverter mqlConverter = new MqlToAlgConverter( mqlProcessor, catalogReader, cluster );

        MqlFind find = (MqlFind) mqlProcessor.parse( String.format( "db.%s.find(%s)", "collection", this.filterQuery ) ).get( 0 );
        String msg = getStream();
        BsonDocument msgDoc;
        AlgNode input;
        if ( msg.contains( "{" ) && msg.contains( "}" ) ) {
            // msg is in JSON format
            msgDoc = BsonDocument.parse( msg );
        } else if ( msg.contains( "[" ) && msg.contains( "]" ) ) {
            // msg is an array
            msgDoc = BsonDocument.parse( "{\"$$ROOT\":" + msg + "}" );
        } else if ( isNumber( msg ) ) {
            double value = Double.parseDouble( msg );
            msgDoc = new BsonDocument( "$$ROOT", new BsonDouble( value ) );
        } else if ( isBoolean( msg ) ) {
            boolean value = Boolean.parseBoolean( msg );
            msgDoc = new BsonDocument( "$$ROOT", new BsonBoolean( value ) );
        } else {
            // msg is String
            msgDoc = new BsonDocument( "$$ROOT", new BsonString( msg ) );
        }
        input = LogicalDocumentValues.create( cluster, ImmutableList.of( msgDoc ) );
        return mqlConverter.convert( find, input );
    }

}