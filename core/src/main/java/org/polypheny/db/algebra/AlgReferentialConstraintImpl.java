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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.util.mapping.IntPair;


/**
 * RelOptReferentialConstraint base implementation.
 */
public class AlgReferentialConstraintImpl implements AlgReferentialConstraint {

    private final List<String> sourceQualifiedName;
    private final List<String> targetQualifiedName;
    private final List<IntPair> columnPairs;


    private AlgReferentialConstraintImpl( List<String> sourceQualifiedName, List<String> targetQualifiedName, List<IntPair> columnPairs ) {
        this.sourceQualifiedName = ImmutableList.copyOf( sourceQualifiedName );
        this.targetQualifiedName = ImmutableList.copyOf( targetQualifiedName );
        this.columnPairs = ImmutableList.copyOf( columnPairs );
    }


    @Override
    public List<String> getSourceQualifiedName() {
        return sourceQualifiedName;
    }


    @Override
    public List<String> getTargetQualifiedName() {
        return targetQualifiedName;
    }


    @Override
    public List<IntPair> getColumnPairs() {
        return columnPairs;
    }


    @Override
    public int getNumColumns() {
        return columnPairs.size();
    }


    public static AlgReferentialConstraintImpl of( List<String> sourceQualifiedName, List<String> targetQualifiedName, List<IntPair> columnPairs ) {
        return new AlgReferentialConstraintImpl( sourceQualifiedName, targetQualifiedName, columnPairs );
    }


    @Override
    public String toString() {
        return "{ " + sourceQualifiedName + ", " + targetQualifiedName + ", " + columnPairs + " }";
    }

}

