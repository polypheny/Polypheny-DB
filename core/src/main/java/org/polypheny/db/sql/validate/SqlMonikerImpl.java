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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.validate;


import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.util.Util;


/**
 * A generic implementation of {@link SqlMoniker}.
 */
public class SqlMonikerImpl implements SqlMoniker {

    private final ImmutableList<String> names;
    private final SqlMonikerType type;


    /**
     * Creates a moniker with an array of names.
     */
    public SqlMonikerImpl( List<String> names, SqlMonikerType type ) {
        this.names = ImmutableList.copyOf( names );
        this.type = Objects.requireNonNull( type );
    }


    /**
     * Creates a moniker with a single name.
     */
    public SqlMonikerImpl( String name, SqlMonikerType type ) {
        this( ImmutableList.of( name ), type );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof SqlMonikerImpl
                && type == ((SqlMonikerImpl) obj).type
                && names.equals( ((SqlMonikerImpl) obj).names );
    }


    @Override
    public int hashCode() {
        return Objects.hash( type, names );
    }


    @Override
    public SqlMonikerType getType() {
        return type;
    }


    @Override
    public List<String> getFullyQualifiedNames() {
        return names;
    }


    @Override
    public SqlIdentifier toIdentifier() {
        return new SqlIdentifier( names, ParserPos.ZERO );
    }


    public String toString() {
        return Util.sepList( names, "." );
    }


    @Override
    public String id() {
        return type + "(" + this + ")";
    }
}

