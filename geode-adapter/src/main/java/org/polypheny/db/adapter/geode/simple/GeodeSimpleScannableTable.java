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

package org.polypheny.db.adapter.geode.simple;


import static org.polypheny.db.adapter.geode.util.GeodeUtils.convertToRowValues;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.geode.cache.client.ClientCache;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.schema.ScannableTable;
import org.polypheny.db.schema.impl.AbstractTable;


/**
 * Geode Simple Scannable Table Abstraction
 */
public class GeodeSimpleScannableTable extends AbstractTable implements ScannableTable {

    private final AlgDataType algDataType;
    private String regionName;
    private ClientCache clientCache;


    public GeodeSimpleScannableTable( String regionName, AlgDataType algDataType, ClientCache clientCache ) {
        super();
        this.regionName = regionName;
        this.clientCache = clientCache;
        this.algDataType = algDataType;
    }


    @Override
    public String toString() {
        return "GeodeSimpleScannableTable";
    }


    @Override
    public AlgDataType getRowType( AlgDataTypeFactory typeFactory ) {
        return algDataType;
    }


    @Override
    public Enumerable<Object[]> scan( DataContext root ) {
        return new AbstractEnumerable<Object[]>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new GeodeSimpleEnumerator<Object[]>( clientCache, regionName ) {
                    @Override
                    public Object[] convert( Object obj ) {
                        Object values = convertToRowValues( algDataType.getFieldList(), obj );
                        if ( values instanceof Object[] ) {
                            return (Object[]) values;
                        }
                        return new Object[]{ values };
                    }
                };
            }
        };
    }

}

