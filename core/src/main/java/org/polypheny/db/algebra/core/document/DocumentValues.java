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

package org.polypheny.db.algebra.core.document;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalValues;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.Snapshot;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;


public abstract class DocumentValues extends AbstractAlgNode implements DocumentAlg {

    @Getter
    public final List<PolyDocument> documents;


    /**
     * Creates a {@link DocumentValues}.
     * {@link ModelTrait#DOCUMENT} node, which contains values.
     */
    public DocumentValues( AlgOptCluster cluster, AlgTraitSet traitSet, List<PolyDocument> documents ) {
        super( cluster, traitSet );
        this.rowType = DocumentType.ofId();
        this.documents = validate( documents );
    }


    protected static List<PolyDocument> validate( List<PolyDocument> docs ) {

        for ( PolyDocument doc : docs ) {
            PolyString id = PolyString.of( DocumentType.DOCUMENT_ID );
            if ( !doc.containsKey( id ) ) {
                doc.put( id, PolyString.of( ObjectId.get().toString() ) );
            } else {
                if ( doc.get( id ).isString() && doc.get( id ).asString().value.length() > 24 ) {
                    throw new RuntimeException( "ObjectId was malformed." );
                }
            }

        }

        return docs;
    }


    protected static ImmutableList<ImmutableList<RexLiteral>> relationalize( List<PolyDocument> tuples, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();

        List<RexLiteral> normalizedTuple = new ArrayList<>();
        for ( PolyDocument tuple : tuples ) {
            PolyString id;
            if ( tuple.isDocument() && tuple.asDocument().containsKey( PolyString.of( "_id" ) ) ) {
                PolyString bsonId = tuple.asDocument().get( PolyString.of( "_id" ) ).asString();
                if ( bsonId.isString() ) {
                    id = bsonId.asString();
                } else {
                    throw new RuntimeException( "Error while transforming document to relational values" );
                }

                normalizedTuple.add( 0, rexBuilder.makeLiteral( id.value ) );
                String parsed = tuple.serialize();
                normalizedTuple.add( 1, rexBuilder.makeLiteral( parsed ) );
                normalized.add( ImmutableList.copyOf( normalizedTuple ) );
            }
        }

        return ImmutableList.copyOf( normalized );
    }


    @Override
    public String algCompareString() {
        return getClass().getCanonicalName() + "$" + documents.hashCode() + "$";
    }


    @Override
    public DocType getDocType() {
        return DocType.VALUES;
    }


    public LogicalValues getRelationalEquivalent() {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );
        AlgOptCluster cluster = AlgOptCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder(), traitSet, getCluster().getSnapshot() );

        return new LogicalValues( cluster, out, ((DocumentType) rowType).asRelational(), relationalize( documents, cluster.getRexBuilder() ) );
    }


    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<CatalogEntity> entities, Snapshot snapshot ) {
        return List.of( getRelationalEquivalent() );
    }

}
