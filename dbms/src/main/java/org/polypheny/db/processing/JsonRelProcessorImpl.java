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

package org.polypheny.db.processing;

import java.util.List;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.QueryPlanBuilder;

public class JsonRelProcessorImpl extends Processor {


    @Override
    public List<? extends Node> parse( String query ) {
        throw new RuntimeException( "JsonProcessor does not support string representation!" );
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new RuntimeException( "JsonProcessor does not support validation!" );
    }


    @Override
    public AlgRoot translate( Statement statement, Node query, QueryParameters parameters ) {
        return AlgRoot.of( QueryPlanBuilder.buildFromJsonRel( statement, parameters.getQuery() ), Kind.SELECT );
    }


    @Override
    public PolyImplementation prepareDdl( Statement statement, Node parsed, QueryParameters parameters ) {
        throw new RuntimeException( "JsonProcessor does not support DDLs!" );
    }


    @Override
    public void unlock( Statement statement ) {
        throw new RuntimeException( "The JsonRelProcessor does not support DML or DDLs and should therefore not lock." );
    }


    @Override
    public void lock( Statement statement ) throws DeadlockException {
        throw new RuntimeException( "The JsonRelProcessor does not support DML or DDLs and should therefore not lock." );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return parameters.getQuery();
    }


    @Override
    public AlgDataType getParameterRowType( Node left ) {
        throw new RuntimeException( "JsonProcessor does not support getParameterRowType!" );
    }

}
