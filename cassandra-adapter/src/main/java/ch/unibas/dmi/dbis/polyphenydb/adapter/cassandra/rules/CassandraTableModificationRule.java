/*
 * Copyright 2019-2020 The Polypheny Project
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

package ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.rules;


import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraConvention;
import ch.unibas.dmi.dbis.polyphenydb.adapter.cassandra.CassandraTableModify;
import ch.unibas.dmi.dbis.polyphenydb.plan.Convention;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRule;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelOptRuleCall;
import ch.unibas.dmi.dbis.polyphenydb.plan.RelTraitSet;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify;
import ch.unibas.dmi.dbis.polyphenydb.rel.core.TableModify.Operation;
import ch.unibas.dmi.dbis.polyphenydb.schema.ModifiableTable;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelBuilderFactory;
import lombok.extern.slf4j.Slf4j;


@Slf4j
public class CassandraTableModificationRule extends CassandraConverterRule {

    CassandraTableModificationRule( CassandraConvention out, RelBuilderFactory relBuilderFactory ) {
        super( TableModify.class, r -> true, Convention.NONE, out, relBuilderFactory, "CassandraTableModificationRule" );
    }


    @Override
    public boolean matches( RelOptRuleCall call ) {
        final TableModify tableModify = call.rel( 0 );
        return tableModify.getOperation() != Operation.MERGE;
    }


    @Override
    public RelNode convert( RelNode rel ) {
        final TableModify modify = (TableModify) rel;
        log.debug( "Converting to a {} CassandraTableModify", ((TableModify) rel).getOperation() );
        final ModifiableTable modifiableTable = modify.getTable().unwrap( ModifiableTable.class );
        if ( modifiableTable == null ) {
            return null;
        }
        final RelTraitSet traitSet = modify.getTraitSet().replace( out );
        return new CassandraTableModify(
                modify.getCluster(),
                traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                RelOptRule.convert( modify.getInput(), traitSet ),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened()
        );
    }
}
