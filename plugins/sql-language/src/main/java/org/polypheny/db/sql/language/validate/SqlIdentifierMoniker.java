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
import java.util.Objects;
import org.polypheny.db.algebra.constant.MonikerType;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.util.Moniker;


/**
 * An implementation of {@link Moniker} that encapsulates the normalized name information of a {@link SqlIdentifier}.
 */
public class SqlIdentifierMoniker implements Moniker {

    private final SqlIdentifier id;


    /**
     * Creates an SqlIdentifierMoniker.
     */
    public SqlIdentifierMoniker( SqlIdentifier id ) {
        this.id = Objects.requireNonNull( id );
    }


    @Override
    public MonikerType getType() {
        return MonikerType.COLUMN;
    }


    @Override
    public List<String> getFullyQualifiedNames() {
        return id.names;
    }


    public String toString() {
        return id.toString();
    }


    @Override
    public String id() {
        return id.toString();
    }

}

