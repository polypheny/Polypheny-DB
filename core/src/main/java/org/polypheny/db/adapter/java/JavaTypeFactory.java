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

package org.polypheny.db.adapter.java;


import java.lang.reflect.Type;
import java.util.List;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;


/**
 * Type factory that can register Java classes as record types.
 */
public interface JavaTypeFactory extends AlgDataTypeFactory {

    /**
     * Creates a record type based upon the public fields of a Java class.
     *
     * @param clazz Java class
     * @return Record type that remembers its Java class
     */
    AlgDataType createStructType( Class<?> clazz );

    /**
     * Creates a type, deducing whether a record, scalar or primitive type is needed.
     *
     * @param type Java type, such as a {@link Class}
     * @return Record or scalar type
     */
    AlgDataType createType( Type type );

    Type getJavaClass( AlgDataType type );

    /**
     * Creates a synthetic Java class whose fields have the given Java types.
     */
    Type createSyntheticType( List<Type> types );

    /**
     * Converts a type in Java format to a SQL-oriented type.
     */
    AlgDataType toSql( AlgDataType type );

}

