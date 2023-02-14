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


import org.polypheny.db.util.Conformance;

/**
 * Implementation of {@link Conformance} that delegates all methods to another object. You can create a sub-class that overrides particular methods.
 */
public class SqlDelegatingConformance extends SqlAbstractConformance {

    private final Conformance delegate;


    /**
     * Creates a SqlDelegatingConformance.
     */
    protected SqlDelegatingConformance( Conformance delegate ) {
        this.delegate = delegate;
    }


    @Override
    public boolean isGroupByAlias() {
        return delegate.isGroupByAlias();
    }


    @Override
    public boolean isGroupByOrdinal() {
        return delegate.isGroupByOrdinal();
    }


    @Override
    public boolean isHavingAlias() {
        return delegate.isGroupByAlias();
    }


    @Override
    public boolean isSortByOrdinal() {
        return delegate.isSortByOrdinal();
    }


    @Override
    public boolean isSortByAlias() {
        return delegate.isSortByAlias();
    }


    @Override
    public boolean isSortByAliasObscures() {
        return delegate.isSortByAliasObscures();
    }


    @Override
    public boolean isFromRequired() {
        return delegate.isFromRequired();
    }


    @Override
    public boolean isBangEqualAllowed() {
        return delegate.isBangEqualAllowed();
    }


    @Override
    public boolean isMinusAllowed() {
        return delegate.isMinusAllowed();
    }


    @Override
    public boolean isInsertSubsetColumnsAllowed() {
        return delegate.isInsertSubsetColumnsAllowed();
    }


    @Override
    public boolean allowNiladicParentheses() {
        return delegate.allowNiladicParentheses();
    }

}

