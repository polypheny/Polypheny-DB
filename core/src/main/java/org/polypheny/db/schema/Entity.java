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

package org.polypheny.db.schema;

import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.util.Wrapper;


/**
 * Table.
 *
 * The typical way for a table to be created is when Polypheny-DB interrogates a user-defined schema in order to validate
 * names appearing in a SQL query.
 *
 * Note that a table does not know its name. It is in fact possible for a table to be used more than once, perhaps under
 * multiple names or under multiple schemas. (Compare with the <a href="http://en.wikipedia.org/wiki/Inode">i-node</a> concept
 * in the UNIX filesystem.)
 *
 * A particular table instance may also implement {@link Wrapper}, to give access to sub-objects.
 *
 * @see TableMacro
 */
public interface Entity {

    /**
     * Returns this table's row type.
     *
     * This is a struct type whose fields describe the names and types of the columns in this table.
     *
     * The implementer must use the type factory provided. This ensures that the type is converted into a canonical form;
     * other equal types in the same query will use the same object.
     *
     * @param typeFactory Type factory with which to create the type
     * @return Row type
     */
    AlgDataType getTupleType( AlgDataTypeFactory typeFactory );

    default AlgDataTypeFactory getTypeFactory() {
        return new JavaTypeFactoryImpl();
    }

    /**
     * Returns a provider of statistics about this table.
     */
    Statistic getStatistic();

    /**
     * Returns the tableId of this table.
     */
    Long getId();

    Long getPartitionId();

    Long getAdapterId();

    /**
     * Determines whether the given {@code column} has been rolled up.
     */
    boolean isRolledUp( String column );

    /**
     * Determines whether the given rolled up column can be used inside the given aggregate function.
     * You can assume that {@code isRolledUp(column)} is {@code true}.
     *
     * @param column The column name for which {@code isRolledUp} is true
     * @param call The aggregate call
     * @param parent Parent node of {@code call} in the {@link Node} tree
     * @return true iff the given aggregate call is valid
     */
    boolean rolledUpColumnValidInsideAgg( String column, Call call, Node parent );


    default DataModel getNamespaceType() {
        return DataModel.RELATIONAL;
    }

    interface Table {

    }


    interface Collection {

    }

}

