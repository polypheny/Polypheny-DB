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

package org.polypheny.db.schema.impl;


import com.google.common.collect.ImmutableList;
import java.util.HashMap;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.nodes.Call;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.schema.Schema;
import org.polypheny.db.schema.Schema.TableType;
import org.polypheny.db.schema.Statistic;
import org.polypheny.db.schema.Statistics;
import org.polypheny.db.schema.Table;
import org.polypheny.db.schema.Wrapper;
import org.polypheny.db.type.PolyType;


/**
 * Abstract base class for implementing {@link Table}.
 *
 * Sub-classes should override {@link #isRolledUp} and {@link Table#rolledUpColumnValidInsideAgg(String, Call, Node)} if
 * their table can potentially contain rolled up values. This information is used by the validator to check for illegal uses
 * of these columns.
 */
@Getter
public abstract class AbstractTable implements Table, Wrapper {

    @Setter
    private boolean substitutionNeeded = false;

    @Setter
    private ImmutableList<PolyType> unsupportedTypes = ImmutableList.of();

    @Setter
    private PolyType substitutionType;

    private Boolean needsTypeSubstitution = null;

    private final HashMap<Integer, AlgDataType> replacedTypes = new HashMap<>();

    private AlgRecordType substitutedRowType;


    protected AbstractTable() {
    }


    // Default implementation. Override if you have statistics.
    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }


    @Override
    public TableType getJdbcTableType() {
        return Schema.TableType.TABLE;
    }


    @Override
    public <C> C unwrap( Class<C> aClass ) {
        if ( aClass.isInstance( this ) ) {
            return aClass.cast( this );
        }
        return null;
    }


    @Override
    public boolean isRolledUp( String column ) {
        return false;
    }


    @Override
    public boolean rolledUpColumnValidInsideAgg( String column, Call call, Node parent ) {
        return true;
    }


    @Override
    public boolean needsTypeSubstitution() {
        if ( needsTypeSubstitution != null ) {
            return needsTypeSubstitution;
        }

        if ( getRowType( getTypeFactory() )
                .getFieldList()
                .stream()
                .anyMatch( f -> unsupportedTypes.contains( f.getType().getPolyType() ) ) ) {

            buildSubstitutedRowType();
            needsTypeSubstitution = true;
            return true;

        }
        needsTypeSubstitution = false;
        return false;
    }


    private void buildSubstitutedRowType() {
        AlgDataType rowType = getRowType( getTypeFactory() );
        rowType.getFieldList().forEach( f -> {
            if ( unsupportedTypes.contains( f.getType().getPolyType() ) ) {
                replacedTypes.put( f.getIndex(), f.getType() );
            }
        } );

        this.substitutedRowType = new AlgRecordType( rowType.getFieldList().stream().map( f -> {
            if ( replacedTypes.containsKey( f.getIndex() ) ) {
                return new AlgDataTypeFieldImpl( f.getName(), f.getPhysicalName(), f.getIndex(), getTypeFactory().createPolyType( substitutionType, 2024 ) );
            }
            return f;
        } ).collect( Collectors.toList() ) );

    }

}

