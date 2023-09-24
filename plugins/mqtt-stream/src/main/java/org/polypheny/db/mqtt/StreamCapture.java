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

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.bson.BsonArray;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.PolyphenyHomeDirManager;

@Slf4j
public class StreamCapture {

    Transaction transaction;
    PolyphenyHomeDirManager homeDirManager;
    StoringMqttMessage storingMqttMessage;


    StreamCapture( final Transaction transaction ) {
        this.transaction = transaction;
    }


    public void insert( StoringMqttMessage storingMqttMessage ) {
        this.storingMqttMessage = storingMqttMessage;
        insertMessage();
    }


    private void insertMessage() {
        String sqlCollectionName = this.storingMqttMessage.getNamespaceName() + "." + this.storingMqttMessage.getEntityName();
        Statement statement = transaction.createStatement();

        // Builder which allows to construct the algebra tree which is equivalent to query and is executed
        AlgBuilder builder = AlgBuilder.createDocumentBuilder( statement );

        BsonDocument document = new BsonDocument();
        document.put( "source", new BsonString( this.storingMqttMessage.getUniqueNameOfInterface() ) );
        document.put( "topic", new BsonString( this.storingMqttMessage.getTopic() ) );
        String msg = this.storingMqttMessage.getMessage();
        BsonValue value;
        if ( msg.contains( "{" ) && msg.contains( "}" ) ) {
            value = BsonDocument.parse( msg );
        } else if ( msg.contains( "[" ) && msg.contains( "]" ) ) {
            BsonArray bsonArray = new BsonArray();
            msg = msg.replace( "[", "" ).replace( "]", "" );
            String[] msglist = msg.split( "," );
            for ( String stringValue : msglist ) {
                stringValue = stringValue.trim();
                bsonArray.add( getBsonValue( stringValue ) );
            }
            value = bsonArray;
        } else {
            // msg is a single value
            value = getBsonValue( msg );
        }
        document.put( "payload", value );

        AlgNode algNode = builder.docInsert( statement, sqlCollectionName, document ).build();

        AlgRoot root = AlgRoot.of( algNode, Kind.INSERT );
        // for inserts and all DML queries only a number is returned
        List<List<Object>> res = executeAndTransformPolyAlg( root, statement, statement.getPrepareContext() );
        try {
            transaction.commit();
        } catch ( TransactionException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * turns one single value into the corresponding BsonValue
     * @param value value that has to be casted as String
     * @return
     */
    protected BsonValue getBsonValue(String value) {
        if ( isInteger( value ) ) {
            return new BsonInt32(Integer.parseInt( value ) );
        } else if ( isDouble( value ) ) {
            return new BsonDouble(Double.parseDouble( value ) );
        } else if ( isBoolean( value ) ) {
            return new BsonBoolean( Boolean.parseBoolean( value ) );
        } else {
            return new BsonString( value );
        }
    }


    public boolean isDouble( String value ) {
        try {
            Double.parseDouble( value );
        } catch ( NumberFormatException e ) {
            return false;
        }
        return true;
    }


    protected boolean isInteger( String value ) {
        try {
            int intNumber = Integer.parseInt( value );
            double doubleNumber = Double.parseDouble( value );
            return intNumber == doubleNumber;
        } catch ( NumberFormatException e ) {
            return false;
        }
    }


    public boolean isBoolean( String value ) {
        return value.equals( "true" ) || value.equals( "false" );
    }



    List<List<Object>> executeAndTransformPolyAlg( AlgRoot algRoot, Statement statement, final Context ctx ) {

        try {
            // Prepare
            PolyImplementation result = statement.getQueryProcessor().prepareQuery( algRoot, false );
            log.debug( "AlgRoot was prepared." );

            List<List<Object>> rows = result.getRows( statement, -1 );
            statement.getTransaction().commit();
            return rows;
        } catch ( Throwable e ) {
            log.error( "Error during execution of stream capture query", e );
            try {
                statement.getTransaction().rollback();
            } catch ( TransactionException transactionException ) {
                log.error( "Could not rollback", e );
            }
            return null;
        }
    }

}
