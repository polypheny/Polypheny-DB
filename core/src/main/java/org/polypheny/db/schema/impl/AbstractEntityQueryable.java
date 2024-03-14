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

package org.polypheny.db.schema.impl;


import java.lang.reflect.Type;
import java.util.Iterator;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.tree.Expression;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableEntity;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.schema.types.QueryableEntity;


/**
 * Abstract implementation of {@link org.apache.calcite.linq4j.Queryable} for {@link QueryableEntity}.
 *
 * Not to be confused with {@link AbstractQueryableEntity}.
 *
 * @param <T> element type
 */
public abstract class AbstractEntityQueryable<T, K extends Entity & QueryableEntity> extends AbstractQueryable<T> {

    public final DataContext dataContext;
    public final Snapshot snapshot;
    public final K entity;


    public AbstractEntityQueryable( DataContext dataContext, Snapshot snapshot, K entity ) {
        this.dataContext = dataContext;
        this.snapshot = snapshot;
        assert entity.unwrap( QueryableEntity.class ).isPresent();
        this.entity = entity;
    }


    @Override
    public Expression getExpression() {
        return entity.asExpression();
    }


    @Override
    public QueryProvider getProvider() {
        return dataContext.getQueryProvider();
    }


    @Override
    public Type getElementType() {
        return entity.getElementType();
    }


    @Override
    public @NotNull Iterator<T> iterator() {
        return Linq4j.enumeratorIterator( enumerator() );
    }
}

