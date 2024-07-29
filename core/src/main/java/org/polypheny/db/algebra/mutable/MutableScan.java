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

package org.polypheny.db.algebra.mutable;


import org.polypheny.db.algebra.core.relational.RelScan;


/**
 * Mutable equivalent of {@link RelScan}.
 */
public class MutableScan extends MutableLeafAlg {

    private MutableScan( RelScan<?> alg ) {
        super( MutableAlgType.TABLE_SCAN, alg );
    }


    /**
     * Creates a MutableScan.
     *
     * @param scan The underlying Scan object
     */
    public static MutableScan of( RelScan<?> scan ) {
        return new MutableScan( scan );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableScan
                && alg.equals( ((MutableScan) obj).alg );
    }


    @Override
    public int hashCode() {
        return alg.hashCode();
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "Scan(table: " ).append( alg.getEntity().name ).append( ")" );
    }


    @Override
    public MutableAlg clone() {
        return MutableScan.of( (RelScan<?>) alg );
    }

}

