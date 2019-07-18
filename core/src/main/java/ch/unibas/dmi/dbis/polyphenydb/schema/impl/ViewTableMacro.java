/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.schema.impl;


import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.AnalyzeViewResult;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.embedded.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeImpl;
import ch.unibas.dmi.dbis.polyphenydb.schema.FunctionParameter;
import ch.unibas.dmi.dbis.polyphenydb.schema.Schemas;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableMacro;
import ch.unibas.dmi.dbis.polyphenydb.schema.TranslatableTable;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.List;


/**
 * Table function that implements a view. It returns the operator tree of the view's SQL query.
 */
public class ViewTableMacro implements TableMacro {

    protected final String viewSql;
    protected final PolyphenyDbSchema schema;
    private final Boolean modifiable;
    /**
     * Typically null. If specified, overrides the path of the schema as the context for validating {@code viewSql}.
     */
    protected final List<String> schemaPath;
    protected final List<String> viewPath;


    /**
     * Creates a ViewTableMacro.
     *
     * @param schema Root schema
     * @param viewSql SQL defining the view
     * @param schemaPath Schema path relative to the root schema
     * @param viewPath View path relative to the schema path
     * @param modifiable Request that a view is modifiable (dependent on analysis of {@code viewSql})
     */
    public ViewTableMacro( PolyphenyDbSchema schema, String viewSql, List<String> schemaPath, List<String> viewPath, Boolean modifiable ) {
        this.viewSql = viewSql;
        this.schema = schema;
        this.viewPath = viewPath == null ? null : ImmutableList.copyOf( viewPath );
        this.modifiable = modifiable;
        this.schemaPath = schemaPath == null ? null : ImmutableList.copyOf( schemaPath );
    }


    public List<FunctionParameter> getParameters() {
        return Collections.emptyList();
    }


    public TranslatableTable apply( List<Object> arguments ) {
        final PolyphenyDbEmbeddedConnection connection = MaterializedViewTable.MATERIALIZATION_CONNECTION;
        AnalyzeViewResult parsed = Schemas.analyzeView( connection, schema, schemaPath, viewSql, viewPath, modifiable != null && modifiable );
        final List<String> schemaPath1 = schemaPath != null ? schemaPath : schema.path( null );
        if ( (modifiable == null || modifiable) && parsed.modifiable && parsed.table != null ) {
            return modifiableViewTable( parsed, viewSql, schemaPath1, viewPath, schema );
        } else {
            return viewTable( parsed, viewSql, schemaPath1, viewPath );
        }
    }


    /**
     * Allows a sub-class to return an extension of {@link ModifiableViewTable} by overriding this method.
     */
    protected ModifiableViewTable modifiableViewTable( AnalyzeViewResult parsed, String viewSql, List<String> schemaPath, List<String> viewPath, PolyphenyDbSchema schema ) {
        final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
        final Type elementType = typeFactory.getJavaClass( parsed.rowType );
        return new ModifiableViewTable( elementType, RelDataTypeImpl.proto( parsed.rowType ), viewSql, schemaPath, viewPath, parsed.table, Schemas.path( schema.root(), parsed.tablePath ), parsed.constraint, parsed.columnMapping );
    }


    /**
     * Allows a sub-class to return an extension of {@link ViewTable} by overriding this method.
     */
    protected ViewTable viewTable( AnalyzeViewResult parsed, String viewSql, List<String> schemaPath, List<String> viewPath ) {
        final JavaTypeFactory typeFactory = (JavaTypeFactory) parsed.typeFactory;
        final Type elementType = typeFactory.getJavaClass( parsed.rowType );
        return new ViewTable( elementType, RelDataTypeImpl.proto( parsed.rowType ), viewSql, schemaPath, viewPath );
    }
}

