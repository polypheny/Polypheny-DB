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

package org.polypheny.db.algebra.metadata;


import org.polypheny.db.catalog.entity.Entity;


/**
 * {@link AlgColumnOrigin} is a data structure describing one of the origins of an output column produced by a relational expression.
 */
public class AlgColumnOrigin {

    private final Entity originTable;

    private final int iOriginColumn;

    private final boolean isDerived;


    public AlgColumnOrigin( Entity origin, int iOriginColumn, boolean isDerived ) {
        this.originTable = origin;
        this.iOriginColumn = iOriginColumn;
        this.isDerived = isDerived;
    }


    /**
     * @return table of origin
     */
    public Entity getOriginTable() {
        return originTable;
    }


    /**
     * @return 0-based index of column in origin table; whether this ordinal is flattened or unflattened depends on whether UDT flattening has already been performed on the relational expression which produced this description
     */
    public int getOriginColumnOrdinal() {
        return iOriginColumn;
    }


    /**
     * Consider the query <code>select a+b as c, d as e from t</code>. The output column c has two origins (a and b), both of them derived. The output column d as one origin (c), which is not derived.
     *
     * @return false if value taken directly from column in origin table; true otherwise
     */
    public boolean isDerived() {
        return isDerived;
    }


    // override Object
    public boolean equals( Object obj ) {
        if ( !(obj instanceof AlgColumnOrigin) ) {
            return false;
        }
        AlgColumnOrigin other = (AlgColumnOrigin) obj;
        return originTable.id == other.originTable.id
                && (iOriginColumn == other.iOriginColumn)
                && (isDerived == other.isDerived);
    }


    // override Object
    public int hashCode() {
        return originTable.hashCode() + iOriginColumn + (isDerived ? 313 : 0);
    }

}

