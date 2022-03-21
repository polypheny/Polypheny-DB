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

package org.polypheny.db.webui.crud;

import io.javalin.http.Context;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.adaptiveness.policy.Policies.Target;
import org.polypheny.db.adaptiveness.policy.PoliciesManager;
import org.polypheny.db.adaptiveness.exception.PolicyRuntimeException;
import org.polypheny.db.adaptiveness.models.PolicyChangeRequest;
import org.polypheny.db.adaptiveness.models.UiPolicies;
import org.polypheny.db.adaptiveness.models.UiPolicy;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.requests.UIRequest;

@Slf4j
public class PolicyCrud {

    @Getter
    private static Crud crud;

    private final PoliciesManager policiesManager = PoliciesManager.getInstance();


    public PolicyCrud( Crud crud ) {
        PolicyCrud.crud = crud;
    }


    public void getClauses( final Context ctx ) {
        try {
            List<UiPolicy> policies = policiesManager.getClause( findTarget( ctx ) );

            if ( policies.isEmpty() ) {
                log.warn( "There are no policies for this target." );
                ctx.json( new UiPolicies( null, "No policies are set for this target." ) );
            } else {
                ctx.json( new UiPolicies( policies, null ) );
            }
        } catch ( PolicyRuntimeException e ) {
            ctx.json( new UiPolicies( null, e.getMessage() ) );
        }


    }


    public void getAllPossibleClauses( final Context ctx ) {
        try {
            List<UiPolicy> policies = policiesManager.getPossibleClause( findTarget( ctx ) );
            if ( policies.isEmpty() ) {
                log.warn( "There are no clauses for this target." );
                ctx.json( new UiPolicies( null, "There are no clauses for this target." ) );
            } else {
                ctx.json( new UiPolicies( policies, null ) );
            }
        } catch ( PolicyRuntimeException e ) {
            ctx.json( new UiPolicies( null, e.getMessage() ) );
        }


    }


    public void setClauses( final Context ctx ) {
        try {
            policiesManager.updateClauses( ctx.bodyAsClass( PolicyChangeRequest.class ) );
        } catch ( PolicyRuntimeException e ) {
            ctx.json( new UiPolicies( null, e.getMessage() ) );
        }

    }


    public void addClause( final Context ctx ) {
        try {
            policiesManager.addClause( ctx.bodyAsClass( PolicyChangeRequest.class ) );
        } catch ( PolicyRuntimeException e ) {
            ctx.json( new UiPolicies( null, e.getMessage() ) );
        }

    }


    public void deleteClause( Context ctx ) {
        try {
            policiesManager.deleteClause( ctx.bodyAsClass( PolicyChangeRequest.class ) );
        } catch ( PolicyRuntimeException e ) {
            ctx.json( new UiPolicies( null, e.getMessage() ) );
        }

    }


    private Pair<Target, Long> findTarget( Context ctx ) {
        UIRequest request = ctx.bodyAsClass( UIRequest.class );

        Long schemaId = null;
        Long tableId = null;
        String polypheny = null;

        try {
            if ( request.tableId.equals( "polypheny" ) ) {
                polypheny = request.tableId;
            } else if ( !request.tableId.contains( "." ) ) {
                schemaId = Catalog.getInstance().getSchema( 1, request.tableId ).id;
            } else {
                schemaId = Catalog.getInstance().getSchema( 1, request.tableId.split( "\\." )[0] ).id;
                tableId = Catalog.getInstance().getTable( schemaId, request.tableId.split( "\\." )[1] ).id;
            }
            return policiesManager.findTarget( polypheny, schemaId, tableId );
        } catch ( UnknownTableException | UnknownSchemaException e ) {
            throw new PolicyRuntimeException( "Schema: " + request.tableId.split( "\\." )[0] + " or Table: "
                    + request.tableId.split( "\\." )[1] + "is unknown." );
        }
    }


}
