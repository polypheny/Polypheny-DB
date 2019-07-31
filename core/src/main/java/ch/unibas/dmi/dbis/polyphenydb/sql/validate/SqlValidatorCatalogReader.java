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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.Wrapper;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import java.util.List;


/**
 * Supplies catalog information for {@link SqlValidator}.
 *
 * This interface only provides a thin API to the underlying repository, and this is intentional. By only presenting the repository information of interest to the validator, we reduce the dependency
 * on exact mechanism to implement the repository. It is also possible to construct mock implementations of this interface for testing purposes.
 */
public interface SqlValidatorCatalogReader extends Wrapper {

    /**
     * Finds a table or schema with the given name, possibly qualified.
     *
     * Uses the case-sensitivity policy of the catalog reader.
     *
     * If not found, returns null. If you want a more descriptive error message or to override the case-sensitivity of the match, use {@link SqlValidatorScope#resolveTable}.
     *
     * @param names Name of table, may be qualified or fully-qualified
     * @return Table with the given name, or null
     */
    SqlValidatorTable getTable( List<String> names );

    /**
     * Finds a user-defined type with the given name, possibly qualified.
     *
     * NOTE jvs 12-Feb-2005: the reason this method is defined here instead of on RelDataTypeFactory is that it has to take into account context-dependent information such as SQL schema path,
     * whereas a type factory is context-independent.
     *
     * @param typeName Name of type
     * @return named type, or null if not found
     */
    RelDataType getNamedType( SqlIdentifier typeName );

    /**
     * Given fully qualified schema name, returns schema object names as specified. They can be schema, table, function, view.
     * When names array is empty, the contents of root schema should be returned.
     *
     * @param names the array contains fully qualified schema name or empty list for root schema
     * @return the list of all object (schema, table, function, view) names under the above criteria
     */
    List<SqlMoniker> getAllSchemaObjectNames( List<String> names );

    /**
     * Returns the paths of all schemas to look in for tables.
     *
     * @return paths of current schema and root schema
     */
    List<List<String>> getSchemaPaths();

    /**
     * @deprecated Use {@link #nameMatcher()}.{@link SqlNameMatcher#field(RelDataType, String)}
     */
    @Deprecated
    // to be removed before 2.0
    RelDataTypeField field( RelDataType rowType, String alias );

    /**
     * Returns an implementation of {@link SqlNameMatcher} that matches the case-sensitivity policy.
     */
    SqlNameMatcher nameMatcher();

    /**
     * @deprecated Use {@link #nameMatcher()}.{@link SqlNameMatcher#matches(String, String)}
     */
    @Deprecated
    // to be removed before 2.0
    boolean matches( String string, String name );

    RelDataType createTypeFromProjection( RelDataType type, List<String> columnNameList );

    /**
     * @deprecated Use {@link #nameMatcher()}.{@link SqlNameMatcher#isCaseSensitive()}
     */
    @Deprecated
    // to be removed before 2.0
    boolean isCaseSensitive();

    /**
     * Returns the root namespace for name resolution.
     */
    PolyphenyDbSchema getRootSchema();

}

