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

package org.polypheny.db.algebra.logical;

import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.ProcedureExecution;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LogicalProcedureExecution extends ProcedureExecution {

    private Map<String, Object> parameters;

    protected LogicalProcedureExecution(AlgOptCluster cluster, AlgTraitSet traits, Map<String, Object> parameters, AlgNode input) {
        super(cluster, traits, input);
        this.parameters = parameters;
    }

    /**
     * Creates a LogicalProcedureExecution.
     */
    public static LogicalProcedureExecution create(AlgNode input, Map<String, Object> parameters) {
        AlgOptCluster cluster = input.getCluster();
        final AlgTraitSet traitSet = cluster.traitSetOf( Convention.NONE );
        return new LogicalProcedureExecution( cluster, traitSet, parameters, input );
    }

    @Override
    public AlgNode copy(AlgTraitSet traitSet, List<AlgNode> inputs) {
        final AlgNode input = inputs.get(0);
        final AlgOptCluster cluster = input.getCluster();
        return new LogicalProcedureExecution(cluster, traitSet, Collections.unmodifiableMap(this.parameters), input);
    }

    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" + input.algCompareString() + "&";
    }

    @Override
    public boolean isImplementationCacheable() {
        return super.isImplementationCacheable();
    }

    @Override
    public boolean hasView() {
        return super.hasView();
    }

    @Override
    public void tryExpandView(AlgNode input) {
        super.tryExpandView(input);
    }

    @Override
    public AlgNode tryParentExpandView(AlgNode input) {
        return super.tryParentExpandView(input);
    }

    @Override
    public Catalog.SchemaType getModel() {
        return super.getModel();
    }

    public boolean hasMapping(String key) {
        return parameters.containsKey(key);
    }

    public Object getMapping(String key) {
        return parameters.get(key);
    }
}
