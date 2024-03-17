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

package org.polypheny.db.algebra.core.document;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.Getter;
import org.bson.types.ObjectId;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;


@Getter
public abstract class DocumentValues extends AbstractAlgNode implements DocumentAlg {

    public final List<PolyDocument> documents;

    public final List<RexDynamicParam> dynamicDocuments;


    /**
     * Creates a {@link DocumentValues}.
     * {@link ModelTrait#DOCUMENT} node, which contains values.
     */
    public DocumentValues( AlgCluster cluster, AlgTraitSet traitSet, List<PolyDocument> documents ) {
        this( cluster, traitSet, documents, new ArrayList<>() );
    }


    public DocumentValues( AlgCluster cluster, AlgTraitSet traitSet, List<PolyDocument> documents, List<RexDynamicParam> dynamicDocuments ) {
        super( cluster, traitSet );
        this.rowType = DocumentType.ofId();
        this.documents = validate( documents );
        this.dynamicDocuments = dynamicDocuments;
    }


    protected static List<PolyDocument> validate( List<PolyDocument> docs ) {

        for ( PolyDocument doc : docs ) {
            PolyString id = PolyString.of( DocumentType.DOCUMENT_ID );
            if ( !doc.containsKey( id ) ) {
                doc.put( id, PolyString.of( ObjectId.get().toString() ) );
            } else {
                if ( doc.get( id ).isString() && doc.get( id ).asString().value.length() > 24 ) {
                    throw new GenericRuntimeException( "ObjectId was malformed." );
                }
            }

        }

        return docs;
    }


    public boolean isPrepared() {
        return !dynamicDocuments.isEmpty();
    }


    protected static ImmutableList<ImmutableList<RexLiteral>> relationalize( List<PolyDocument> tuples, RexBuilder rexBuilder ) {
        List<ImmutableList<RexLiteral>> normalized = new ArrayList<>();

        List<RexLiteral> normalizedTuple = new ArrayList<>();
        for ( PolyDocument tuple : tuples ) {
            PolyString id;
            if ( tuple.isDocument() && tuple.asDocument().containsKey( PolyString.of( DocumentType.DOCUMENT_ID ) ) ) {
                PolyString bsonId = tuple.asDocument().get( PolyString.of( DocumentType.DOCUMENT_ID ) ).asString();
                if ( bsonId.isString() ) {
                    id = bsonId.asString();
                } else {
                    throw new GenericRuntimeException( "Error while transforming document to relational values" );
                }

                normalizedTuple.add( 0, rexBuilder.makeLiteral( PolyBinary.of( id.serialize().getBytes() ), AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT ), PolyType.TEXT ) );
                byte[] parsed = tuple.serialize().getBytes();
                normalizedTuple.add( 1, rexBuilder.makeLiteral( PolyBinary.of( parsed ), AlgDataTypeFactory.DEFAULT.createPolyType( PolyType.TEXT ), PolyType.TEXT ) );
                normalized.add( ImmutableList.copyOf( normalizedTuple ) );
            }
        }

        return ImmutableList.copyOf( normalized );
    }


    @Override
    public String algCompareString() {
        return getClass().getCanonicalName() + "$"
                + (dynamicDocuments == null && documents != null ? documents.stream().map( PolyDocument::hashCode ).map( Objects::toString ).collect( Collectors.joining( "$" ) ) : "")
                + (dynamicDocuments != null ? dynamicDocuments.stream().map( d -> d.name ).collect( Collectors.joining( "$" ) ) : "") + "&";
    }


    @Override
    public DocType getDocType() {
        return DocType.VALUES;
    }


    public LogicalRelValues getRelationalEquivalent() {
        AlgTraitSet out = traitSet.replace( ModelTrait.RELATIONAL );
        AlgCluster cluster = AlgCluster.create( getCluster().getPlanner(), getCluster().getRexBuilder(), traitSet, getCluster().getSnapshot() );

        return new LogicalRelValues( cluster, out, DocumentType.ofRelational(), relationalize( documents, cluster.getRexBuilder() ) );
    }


    public List<AlgNode> getRelationalEquivalent( List<AlgNode> values, List<Entity> entities, Snapshot snapshot ) {
        return List.of( getRelationalEquivalent() );
    }

}
