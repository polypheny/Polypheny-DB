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

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.core.document.DocumentProject;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.ValidatorUtil;


public class LogicalDocumentProject extends DocumentProject {

    /**
     * Subclass of {@link DocumentProject} not targeted at any particular engine or calling convention.
     */
    public LogicalDocumentProject( AlgOptCluster cluster, AlgTraitSet traits, AlgNode input, List<? extends RexNode> projects, AlgDataType rowType ) {
        super( cluster, traits, input, new ArrayList<>( projects ), rowType );
    }


    public static LogicalDocumentProject create( AlgNode node, List<? extends RexNode> ids, List<String> fieldNames ) {
        assert ids.size() == fieldNames.size() : "Ids and field names need to be the same size";
        final AlgMetadataQuery mq = node.getCluster().getMetadataQuery();
        final AlgDataType rowType = RexUtil.createStructType( node.getCluster().getTypeFactory(), ids, fieldNames, ValidatorUtil.F_SUGGESTER );
        AlgTraitSet traitSet = node.getCluster().traitSet().replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.project( mq, node, ids ) );
        return new LogicalDocumentProject( node.getCluster(), traitSet, node, ids, rowType );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalDocumentProject( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), projects, rowType );
    }


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
