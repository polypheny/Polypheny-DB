/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.adapter.mongodb.rules;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.bson.BsonDocument;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.rules.MongoRules.MongoDocuments;
import org.polypheny.db.adapter.mongodb.util.RexToMongoTranslator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.document.DocumentModify;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyValue;

public class MongoDocumentModify extends DocumentModify<MongoEntity> implements MongoAlg {


    protected MongoDocumentModify(
            AlgTraitSet traits,
            MongoEntity collection,
            AlgNode input,
            @NonNull Operation operation,
            Map<String, ? extends RexNode> updates,
            List<String> removes,
            Map<String, String> renames ) {
        super( traits, collection, input, operation, updates, removes, renames );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.setDML( true );

        implementor.setEntity( entity );
        implementor.setOperation( this.getOperation() );

        switch ( this.getOperation() ) {
            case INSERT:
                handleInsert( implementor, ((MongoDocuments) input) );
                break;
            case UPDATE:
                handleUpdate( implementor );
                break;
            case MERGE:
                break;
            case DELETE:
                handleDelete( implementor );
                break;
        }
    }


    private void handleDelete( Implementor implementor ) {
        Implementor condImplementor = new Implementor( true );
        input.unwrap( MongoAlg.class ).orElseThrow().implement( condImplementor );

        // carry over the onlyOne flag
        if ( condImplementor.onlyOne ) {
            implementor.onlyOne = true;
        }

        implementor.filter = condImplementor.filter;

        if ( updates.isEmpty() ) {
            implementor.add( null, "{}" );
        } else {
            throw new NotImplementedException();
        }
    }


    private void handleUpdate( Implementor implementor ) {
        Implementor condImplementor = new Implementor( true );
        input.unwrap( MongoAlg.class ).orElseThrow().implement( condImplementor );

        implementor.filter = condImplementor.filter;

        final RexToMongoTranslator translator = new RexToMongoTranslator( List.of(), implementor, DataModel.DOCUMENT );
        for ( Entry<String, ? extends RexNode> entry : updates.entrySet() ) {
            String key = entry.getKey();
            String value = entry.getValue().accept( translator );
            implementor.operations.add( BsonDocument.parse( "{ $set:{" + key + ":" + value + "}}" ) );
        }
        if ( !removes.isEmpty() ) {
            implementor.operations.add( BsonDocument.parse( "{ $unset:{ " + removes.stream().map( r -> r + ":\"\"" ).collect( Collectors.joining( "," ) ) + "}" ) );
        }

        if ( !renames.isEmpty() ) {
            implementor.operations.add( BsonDocument.parse( "{ $rename:{ " + renames.entrySet().stream().map( r -> r.getKey() + ": \"" + r.getValue() + "\"" ).collect( Collectors.joining( "," ) ) + "}" ) );
        }
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new MongoDocumentModify(
                traitSet,
                entity,
                inputs.get( 0 ),
                operation,
                updates,
                removes,
                renames );
    }


    private void handleInsert( Implementor implementor, MongoDocuments documents ) {
        if ( documents.isPrepared() ) {
            implementor.operations = documents.dynamicDocuments
                    .stream()
                    .map( rexNode -> (BsonDocument) new BsonDynamic( rexNode ) )
                    .toList();
            return;
        }
        implementor.operations = documents.documents
                .stream()
                .filter( PolyValue::isDocument )
                .map( d -> BsonDocument.parse( d.toJson() ) )
                .toList();
    }

}
