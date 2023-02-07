/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.sql.language.validate;


import java.util.List;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.util.Util;


/**
 * Fully-qualified identifier.
 *
 * The result of calling {@link SqlValidatorScope#fullyQualify(SqlIdentifier)}, a fully-qualified identifier contains the name (in correct case),
 * parser position, type, and scope of each component of the identifier.
 *
 * It is immutable.
 */
public class SqlQualified {

    public final int prefixLength;
    public final SqlValidatorNamespace namespace;
    public final SqlIdentifier identifier;


    private SqlQualified( SqlValidatorScope scope, int prefixLength, SqlValidatorNamespace namespace, SqlIdentifier identifier ) {
        Util.discard( scope );
        this.prefixLength = prefixLength;
        this.namespace = namespace;
        this.identifier = identifier;
    }


    @Override
    public String toString() {
        return "{id: " + identifier.toString() + ", prefix: " + prefixLength + "}";
    }


    public static SqlQualified create( SqlValidatorScope scope, int prefixLength, SqlValidatorNamespace namespace, SqlIdentifier identifier ) {
        return new SqlQualified( scope, prefixLength, namespace, identifier );
    }


    public final List<String> prefix() {
        return identifier.names.subList( 0, prefixLength );
    }


    public final List<String> suffix() {
        return Util.skip( identifier.names, prefixLength );
    }

}

