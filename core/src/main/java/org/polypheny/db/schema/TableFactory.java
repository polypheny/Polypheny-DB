/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.schema;


import java.util.Map;
import org.polypheny.db.algebra.type.AlgDataType;


/**
 * Factory for {@link Table} objects.
 *
 * A table factory allows you to include custom tables in a model file.
 * For example, here is a model that contains a custom table that generates a range of integers.
 *
 * <blockquote><pre>{
 *   version: '1.0',
 *   defaultSchema: 'MATH',
 *   schemas: [
 *     {
 *       name: 'MATH',
 *       tables: [
 *         {
 *           name: 'INTEGERS',
 *           type: 'custom',
 *           factory: 'com.acme.IntegerTable',
 *           operand: {
 *             start: 3,
 *             end: 7,
 *             column: 'N'
 *           }
 *         }
 *       ]
 *     }
 *   ]
 * }</pre></blockquote>
 *
 * Given that schema, the query
 *
 * <blockquote><pre>SELECT * FROM math.integers</pre></blockquote>
 *
 * returns
 *
 * <blockquote><pre>
 * +---+
 * | N |
 * +---+
 * | 3 |
 * | 4 |
 * | 5 |
 * | 6 |
 * +---+
 * </pre></blockquote>
 *
 * A class that implements TableFactory specified in a schema must have a public default constructor.
 *
 * @param <T> Sub-type of table created by this factory
 */
public interface TableFactory<T extends Table> {

    /**
     * Creates a Table.
     *
     * @param schema Schema this table belongs to
     * @param name Name of this table
     * @param operand The "operand" JSON property
     * @param rowType Row type. Specified if the "columns" JSON property.
     */
    T create( SchemaPlus schema, String name, Map<String, Object> operand, AlgDataType rowType );

}

