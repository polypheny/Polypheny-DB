/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.hsqldb.stores;


import org.polypheny.db.algebra.json.JsonExistsErrorBehavior;
import org.polypheny.db.algebra.json.JsonValueEmptyOrErrorBehavior;
import org.polypheny.db.functions.Functions;
import org.polypheny.db.functions.PathContext;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;

/**
 * HSQLDB Java routines for SQL/JSON pushdown.
 */
public final class HsqldbJsonFunctions {

    private HsqldbJsonFunctions() {
    }


    @SuppressWarnings("unused")
    public static String jsonValue( String document, String path ) {
        if ( document == null || path == null ) {
            return null;
        }

        PolyValue value = Functions.jsonValueAny(
                jsonPathContext( document, path ),
                JsonValueEmptyOrErrorBehavior.NULL,
                null,
                JsonValueEmptyOrErrorBehavior.NULL,
                null );
        return value == null ? null : PolyString.convert( value ).value;
    }


    @SuppressWarnings("unused")
    public static Boolean jsonExists( String document, String path ) {
        if ( document == null || path == null ) {
            return null;
        }
        return Functions.jsonExists( jsonPathContext( document, path ), JsonExistsErrorBehavior.FALSE );
    }


    @SuppressWarnings("unused")
    public static String jsonQuery( String document, String path ) {
        if ( document == null || path == null ) {
            return null;
        }

        PathContext context = jsonPathContext( document, path );
        if ( context.exc != null || context.pathReturned == null || context.emptyResultSequence ) {
            return null;
        }
        return context.pathReturned.toJson();
    }


    private static PathContext jsonPathContext( String document, String path ) {
        Object expression = Functions.jsonValueExpression( PolyString.of( document ) );
        if ( expression instanceof Exception exception ) {
            return PathContext.withUnknownException( exception );
        }
        return Functions.jsonApiCommonSyntax( (PolyValue) expression, PolyString.of( path ) );
    }

}
