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

package org.polypheny.db.algebra.type;


import org.polypheny.db.catalog.impl.Expressible;

/**
 * RelDataTypeField represents the definition of a field in a structured {@link AlgDataType}.
 * <p>
 * Extends the {@link java.util.Map.Entry} interface to allow convenient inter-operation with Java collections classes.
 * In any implementation of this interface, {@link #getName()} must be equivalent to {@link #getName()} and {@link #getName()} ()}
 * must be equivalent to {@link #getType()}.
 */
public interface AlgDataTypeField extends Expressible {

    /**
     * Gets the name of this field, which is unique within its containing type.
     *
     * @return field name
     */
    String getName();

    /**
     * Gets the physical name of this field
     *
     * @return physical field name
     */
    String getPhysicalName();

    /**
     * Gets the ordinal of this field within its containing type.
     *
     * @return 0-based ordinal
     */
    int getIndex();

    Long getId();

    /**
     * Gets the type of this field.
     *
     * @return field type
     */
    AlgDataType getType();

    /**
     * Returns true if this is a dynamic star field.
     */
    boolean isDynamicStar();

}

