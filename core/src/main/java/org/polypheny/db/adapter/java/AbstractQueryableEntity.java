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
import org.polypheny.db.schema.Entity;
import org.polypheny.db.schema.impl.AbstractEntity;
import org.polypheny.db.schema.types.QueryableEntity;


/**
 * Abstract base class for implementing {@link Entity}.
 */
public abstract class AbstractQueryableEntity extends AbstractEntity implements QueryableEntity {

    protected final Type elementType;


    protected AbstractQueryableEntity( Type elementType, Long id, Long partitionId, Long adapterId ) {
        super( id, partitionId, adapterId );
        this.elementType = elementType;
    }


    @Override
    public Type getElementType() {
        return elementType;
    }

}

