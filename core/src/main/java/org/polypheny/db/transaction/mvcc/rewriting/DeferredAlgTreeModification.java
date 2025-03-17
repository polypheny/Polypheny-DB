/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.mvcc.rewriting;

import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.transaction.Statement;

public abstract class DeferredAlgTreeModification<T extends AlgNode, U extends AlgNode> implements AlgTreeModification<T, U> {

    protected final T target;
    protected final Statement statement;


    protected DeferredAlgTreeModification( T target, Statement statement ) {
        this.target = target;
        this.statement = statement;
    }


    public <V extends AlgNode> boolean notTargetsChildOf( V parent ) {
        return !parent.getInputs().contains( target );
    }


    public <V extends AlgNode> boolean notTargets( V node ) {
        return !target.equals( node );
    }


    public <V extends AlgNode> V applyToChild( V parent ) {
        AlgNode newInput = apply( target );
        return replaceInput( parent, newInput );
    }


    private <V extends AlgNode> V replaceInput( V node, AlgNode newInput ) {
        List<AlgNode> inputs = node.getInputs().stream()
                .map( input -> input == target ? newInput : input )
                .toList();
        return (V) node.copy( node.getTraitSet(), inputs );
    }
}
