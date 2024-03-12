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
import org.bson.BsonDocument;
import org.bson.BsonElement;
import org.bson.BsonString;
import org.polypheny.db.adapter.mongodb.MongoAlg;
import org.polypheny.db.adapter.mongodb.MongoEntity;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Scan;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.util.Pair;


/**
 * Relational expression representing a relScan of a MongoDB collection.
 * <p>
 * Additional operations might be applied, using the "find" or "aggregate" methods.</p>
 */
public class MongoScan extends Scan<MongoEntity> implements MongoAlg {


    /**
     * Creates a MongoScan.
     *
     * @param cluster Cluster
     * @param traitSet Traits
     * @param table Table
     */
    public MongoScan( AlgCluster cluster, AlgTraitSet traitSet, MongoEntity table ) {
        super( cluster, traitSet, table );
        this.rowType = table.getTupleType( cluster.getTypeFactory() );

        assert getConvention() == CONVENTION;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return this;
    }


    @Override
    public AlgDataType deriveRowType() {
        return entity.getTupleType( getCluster().getTypeFactory() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        // scans with a small project list are cheaper
        final float f = getTupleType().getFieldCount() / 100f;
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 * f );
    }


    @Override
    public void register( AlgPlanner planner ) {
        for ( AlgOptRule rule : MongoRules.RULES ) {
            planner.addRuleDuringRuntime( rule );
        }
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$"
                + entity.id + "$"
                + entity.getLayer() + "&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "collection", entity.id )
                .item( "layer", entity.getLayer() );
    }


    @Override
    public void implement( Implementor implementor ) {
        implementor.setEntity( entity );
        //implementor.setStaticRowType( (AlgRecordType) rowType );
        //implementor.physicalMapper.addAll( rowType.getFieldNames() );

        if ( implementor.isDML() ) {
            return;
        }
        if ( traitSet.getTrait( ModelTraitDef.INSTANCE ).dataModel() == DataModel.RELATIONAL ) {
            implementor.list.add( Pair.of( null, new BsonDocument( "$project", new BsonDocument( rowType.getFields().stream().map( p -> new BsonElement( MongoRules.maybeQuote( p.getName() ), new BsonString( "$" + p.getPhysicalName() ) ) ).toList() ) ).toJson() ) );
        }
    }

}

