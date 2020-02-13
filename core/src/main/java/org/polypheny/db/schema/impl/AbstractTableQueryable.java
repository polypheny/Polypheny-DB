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

package org.polypheny.db.schema.impl;


import java.lang.reflect.Type;
import java.util.Iterator;
import org.apache.calcite.linq4j.AbstractQueryable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.java.AbstractQueryableTable;
import org.polypheny.db.schema.QueryableTable;
import org.polypheny.db.schema.SchemaPlus;


/**
 * Abstract implementation of {@link org.apache.calcite.linq4j.Queryable} for {@link QueryableTable}.
 *
 * Not to be confused with {@link AbstractQueryableTable}.
 *
 * @param <T> element type
 */
public abstract class AbstractTableQueryable<T> extends AbstractQueryable<T> {

    public final DataContext dataContext;
    public final SchemaPlus schema;
    public final QueryableTable table;
    public final String tableName;


    public AbstractTableQueryable( DataContext dataContext, SchemaPlus schema, QueryableTable table, String tableName ) {
        this.dataContext = dataContext;
        this.schema = schema;
        this.table = table;
        this.tableName = tableName;
    }


    @Override
    public Expression getExpression() {
        return table.getExpression( schema, tableName, Queryable.class );
    }


    @Override
    public QueryProvider getProvider() {
        return dataContext.getQueryProvider();
    }


    @Override
    public Type getElementType() {
        return table.getElementType();
    }


    @Override
    public Iterator<T> iterator() {
        return Linq4j.enumeratorIterator( enumerator() );
    }
}

