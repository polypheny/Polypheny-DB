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

package org.polypheny.db.algebra.logical.document;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.bson.BsonDocument;
import org.bson.BsonObjectId;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.core.relational.RelationalTransformable;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BsonUtil;


public class LogicalDocumentValues extends DocumentValues implements RelationalTransformable {


    /**
     * Java representation of multiple documents, which can be retrieved in the original BSON format form
     * or in the substantiated relational form, where the documents are bundled into a BSON string
     *
     * BSON format
     * <pre><code>
     *     "_id": ObjectId(23kdf232123)
     *     "key": "value",
     *     "key1": "value"
     * </pre></code>
     *
     * becomes
     *
     * Column format
     * <pre><code>
     *     "_id": ObjectId(23kdf232123)
     *     "_data": {
     *         "key": "value",
     *         "key1": "value"
     *     }
     *
     * </pre></code>
     *
     * @param cluster the cluster, which holds the information regarding the ongoing operation
     * @param traitSet the used traitSet
     * @param tuples the documents in their native BSON format
     */
    public LogicalDocumentValues( AlgOptCluster cluster, AlgTraitSet traitSet, ImmutableList<BsonValue> tuples ) {
        super( cluster, traitSet, tuples );
    }


    public static AlgNode create( AlgOptCluster cluster, ImmutableList<BsonValue> tuples ) {
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalDocumentValues( cluster, traitSet, tuples );
    }


    public static AlgNode create( LogicalValues input ) {
        return create( input.getCluster(), bsonify( input.getTuples(), input.getRowType() ) );
    }


    private static ImmutableList<BsonValue> bsonify( ImmutableList<ImmutableList<RexLiteral>> tuples, AlgDataType rowType ) {
        List<BsonValue> docs = new ArrayList<>();

        for ( ImmutableList<RexLiteral> values : tuples ) {
            BsonDocument doc = new BsonDocument();
            int pos = 0;
            for ( RexLiteral value : values ) {
                AlgDataTypeField field = rowType.getFieldList().get( pos );

                if ( field.getName().equals( "_id" ) ) {
                    String _id = value.getValueAs( String.class );
                    ObjectId objectId;
                    if ( _id.matches( "ObjectId\\([0-9abcdef]{24}\\)" ) ) {
                        objectId = new ObjectId( _id.substring( 9, 33 ) );
                    } else {
                        objectId = ObjectId.get();
                    }
                    doc.put( "_id", new BsonObjectId( objectId ) );
                } else if ( field.getName().equals( "_data" ) ) {
                    BsonDocument docVal = new BsonDocument();
                    if ( !value.isNull() && value.getValueAs( String.class ).length() != 0 ) {
                        String data = BsonUtil.fixBson( value.getValueAs( String.class ) );
                        if ( data.matches( "[{].*[}]" ) ) {
                            docVal = BsonDocument.parse( data );
                        } else {
                            throw new RuntimeException( "The inserted document is not valid." );
                        }
                    }
                    doc.put( "_data", docVal );
                } else {
                    doc.put( field.getName(), BsonUtil.getAsBson( value, null ) );
                }

                pos++;
            }
            docs.add( doc );
        }
        return ImmutableList.copyOf( docs );
    }


    public static LogicalDocumentValues createOneRow( AlgOptCluster cluster ) {
        final AlgDataType rowType =
                cluster.getTypeFactory()
                        .builder()
                        .add( "ZERO", null, PolyType.INTEGER )
                        .nullable( false )
                        .build();
        return new LogicalDocumentValues( cluster, cluster.traitSet(), ImmutableList.<BsonValue>builder().build() );
    }


    @Override
    public NamespaceType getModel() {
        return NamespaceType.DOCUMENT;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert traitSet.containsIfApplicable( Convention.NONE );
        assert inputs.isEmpty();
        return new LogicalDocumentValues( getCluster(), traitSet, documentTuples );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
