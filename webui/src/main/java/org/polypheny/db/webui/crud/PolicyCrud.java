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
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownTableException;
import org.polypheny.db.policies.policy.PolicyManager;
import org.polypheny.db.policies.policy.models.PolicyChangedRequest;
import org.polypheny.db.webui.Crud;
import org.polypheny.db.webui.models.requests.UIRequest;

public class PolicyCrud {

    @Getter
    private static Crud crud;

    private final PolicyManager policyManager = PolicyManager.getInstance();


    public PolicyCrud( Crud crud ) {
        PolicyCrud.crud = crud;
    }


    public void getPolicies( final Context ctx ) {

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

            ctx.json( policyManager.getPolicies( polypheny, schemaId, tableId ) );
        } catch ( UnknownTableException | UnknownSchemaException e ) {
            throw new RuntimeException( "Schema: " + request.tableId.split( "\\." )[0] + " or Table: "
                    + request.tableId.split( "\\." )[1] + "is unknown." );
        }
    }


    public void setPolicies( final Context ctx ) {

        policyManager.updatePolicies( ctx.bodyAsClass( PolicyChangedRequest.class ) );

    }


    public void getAllPossiblePolicies( final Context ctx ) {

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

            policyManager.getPossiblePolicies( polypheny, schemaId, tableId );

        } catch ( UnknownTableException | UnknownSchemaException e ) {
            throw new RuntimeException( "Schema: " + request.tableId.split( "\\." )[0] + " or Table: "
                    + request.tableId.split( "\\." )[1] + "is unknown." );
        }

    }

}
