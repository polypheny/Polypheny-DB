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

package org.polypheny.db.adapter.csv;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;


/**
 * Table based on a CSV file.
 */
public class CsvTranslatableTable extends CsvTable implements TranslatableEntity {

    /**
     * Creates a CsvTable.
     */
    CsvTranslatableTable( long id, Source source, PhysicalTable table, List<CsvFieldType> fieldTypes, int[] fields, CsvSource csvSource ) {
        super( id, source, table, fieldTypes, fields, csvSource );
    }



    /**
     * Returns an enumerable over a given projection of the fields.
     *
     * Called from generated code.
     */
    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( csvSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new CsvEnumerator( source, cancelFlag, fieldTypes, fields );
            }
        };
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        // Request all fields.
        return new CsvScan( cluster, this, this, fields );
    }


}
