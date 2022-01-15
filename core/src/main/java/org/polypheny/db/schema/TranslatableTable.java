/*
 * Copyright 2019-2022 The Polypheny Project
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


import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.AlgOptTable.ToAlgContext;


/**
 * Extension to {@link Table} that specifies how it is to be translated to a {@link AlgNode relational expression}.
 *
 * It is optional for a Table to implement this interface. If Table does not implement this interface, it will be converted
 * to a  {@link org.polypheny.db.adapter.enumerable.EnumerableTableScan}. Generally a Table will implement this interface to
 * create a particular subclass of AlgNode, and also register rules that act on that particular subclass of AlgNode.
 */
public interface TranslatableTable extends Table {

    /**
     * Converts this table into a {@link AlgNode relational expression}.
     */
    AlgNode toAlg( ToAlgContext context, AlgOptTable algOptTable );

}
