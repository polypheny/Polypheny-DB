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


import java.io.Serializable;
import lombok.Getter;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Triple;


/**
 * Default implementation of {@link AlgDataTypeField}.
 */
@Value
@Getter
public class AlgDataTypeFieldImpl extends Triple<String, Long, AlgDataType> implements AlgDataTypeField, Serializable {

    AlgDataType type;
    String name;
    String physicalName;
    int index;
    Long id;


    /**
     * Creates a RelDataTypeFieldImpl.
     */
    public AlgDataTypeFieldImpl( Long id, String name, int index, AlgDataType type ) {
        this( id, name, null, index, type );
    }


    public AlgDataTypeFieldImpl( Long id, String name, String physicalName, int index, AlgDataType type ) {
        super( name, id, type );
        assert name != null;
        assert type != null;
        this.id = id;
        this.name = name;
        this.index = index;
        this.type = type;
        this.physicalName = physicalName;
    }


    @Override
    public int hashCode() {
        return index ^ name.hashCode() ^ type.hashCode();
    }


    @Override
    public boolean equals( Object obj ) {
        if ( this == obj ) {
            return true;
        }
        if ( !(obj instanceof AlgDataTypeFieldImpl that) ) {
            return false;
        }
        return this.index == that.index
                && this.name.equals( that.name )
                && this.type.equals( that.type );
    }


    // for debugging
    public String toString() {
        return "#" + index + ": " + name + " (" + physicalName + ") " + type;
    }


    @Override
    public boolean isDynamicStar() {
        return type.getPolyType() == PolyType.DYNAMIC_STAR;
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( AlgDataTypeField.class, Expressions.constant( id ), Expressions.constant( name ), Expressions.constant( physicalName ), Expressions.constant( index ), type.asExpression() );
    }

}

