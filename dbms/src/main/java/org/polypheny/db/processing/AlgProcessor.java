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

package org.polypheny.db.processing;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.QueryPlanBuilder;

public class AlgProcessor extends Processor {


    @Override
    public List<? extends Node> parse( String query ) {
        throw new GenericRuntimeException( AlgProcessor.class.getSimpleName() + " does not support string representation!" );
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new GenericRuntimeException( AlgProcessor.class.getSimpleName() + " does not support validation!" );
    }


    @Override
    public AlgRoot translate( Statement statement, ParsedQueryContext context ) {
        try {
            return AlgRoot.of( QueryPlanBuilder.buildFromJsonAlg( statement, context.getQuery() ), Kind.SELECT );
        } catch ( JsonProcessingException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public PolyImplementation prepareDdl( Statement statement, ExecutableStatement node, ParsedQueryContext context ) {
        throw new GenericRuntimeException( AlgProcessor.class.getSimpleName() + " AlgProcessor does not support DDLs!" );
    }


    @Override
    public void unlock( Statement statement ) {
        throw new GenericRuntimeException( AlgProcessor.class.getSimpleName() + " does not support DML or DDLs and should therefore not lock." );
    }


    @Override
    public void lock( Statement statement ) throws DeadlockException {
        throw new GenericRuntimeException( AlgProcessor.class.getSimpleName() + " does not support DML or DDLs and should therefore not lock." );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return parameters.getQuery();
    }


    @Override
    public AlgDataType getParameterRowType( Node left ) {
        throw new GenericRuntimeException( AlgProcessor.class.getSimpleName() + " does not support getParameterRowType!" );
    }


    @Override
    public List<String> splitStatements( String statements ) {
        throw new GenericRuntimeException( "not implemented" );
    }

}
