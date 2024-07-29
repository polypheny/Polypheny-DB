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
import com.google.common.collect.ImmutableMap;
import com.mongodb.lang.Nullable;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;

@Getter
@SuperBuilder(toBuilder = true)
public abstract class DocumentModify<E extends Entity> extends Modify<E> implements DocumentAlg {

    @NonNull
    public ImmutableMap<String, ? extends RexNode> updates;
    @NonNull
    public ImmutableList<String> removes;
    @NonNull
    public ImmutableMap<String, String> renames;
    @NonNull
    public Operation operation;


    /**
     * Creates a {@link DocumentModify}.
     * {@link ModelTrait#DOCUMENT} node, which modifies a collection.
     */
    protected DocumentModify(
            AlgTraitSet traits,
            E collection,
            AlgNode input,
            @NonNull Operation operation,
            @Nullable Map<String, ? extends RexNode> updates,
            @Nullable List<String> removes,
            @Nullable Map<String, String> renames ) {
        super( input.getCluster(), input.getTraitSet().replace( ModelTrait.DOCUMENT ), collection, input );
        this.operation = operation;
        this.updates = ImmutableMap.copyOf( updates == null ? Map.of() : updates );
        this.removes = ImmutableList.copyOf( removes == null ? List.of() : removes );
        this.renames = ImmutableMap.copyOf( renames == null ? Map.of() : renames );
        this.traitSet = traits;
    }


    @Override
    public AlgDataType deriveRowType() {
        return AlgOptUtil.createDmlRowType( Kind.INSERT, getCluster().getTypeFactory() );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$" +
                entity.id + "$" +
                entity.getLayer() + "$" +
                operation + "$" +
                input.algCompareString() + "$" +
                updates.hashCode() + "$" +
                removes.hashCode() + "$" +
                renames.hashCode() + "&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "entity", entity.id )
                .item( "layer", entity.getLayer() )
                .item( "operation", getOperation() )
                .item( "updates", updates )
                .item( "removes", removes )
                .item( "renames", renames );
    }


    @Override
    public DocType getDocType() {
        return DocType.MODIFY;
    }


    public boolean isInsert() {
        return operation == Modify.Operation.INSERT;
    }


    public boolean isDelete() {
        return operation == Modify.Operation.DELETE;
    }

}
