/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.rel.type;


import java.io.Serializable;
import org.polypheny.db.type.PolyType;


/**
 * Default implementation of {@link RelDataTypeField}.
 */
public class RelDataTypeFieldImpl implements RelDataTypeField, Serializable {

    private final RelDataType type;
    private final String name;
    private String physicalName;
    private final int index;


    /**
     * Creates a RelDataTypeFieldImpl.
     */
    public RelDataTypeFieldImpl( String name, int index, RelDataType type ) {
        this( name, null, index, type );
    }


    public RelDataTypeFieldImpl( String name, String physicalName, int index, RelDataType type ) {
        assert name != null;
        assert type != null;
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
        if ( !(obj instanceof RelDataTypeFieldImpl) ) {
            return false;
        }
        RelDataTypeFieldImpl that = (RelDataTypeFieldImpl) obj;
        return this.index == that.index
                && this.name.equals( that.name )
                && this.type.equals( that.type );
    }


    // implement RelDataTypeField
    @Override
    public String getName() {
        return name;
    }


    // implement RelDataTypeField
    @Override
    public String getPhysicalName() {
        return physicalName;
    }


    // implement RelDataTypeField
    @Override
    public int getIndex() {
        return index;
    }


    // implement RelDataTypeField
    @Override
    public RelDataType getType() {
        return type;
    }


    // implement Map.Entry
    @Override
    public final String getKey() {
        return getName();
    }


    // implement Map.Entry
    @Override
    public final RelDataType getValue() {
        return getType();
    }


    // implement Map.Entry
    @Override
    public RelDataType setValue( RelDataType value ) {
        throw new UnsupportedOperationException();
    }


    // for debugging
    public String toString() {
        return "#" + index + ": " + name + " (" + physicalName + ") " + type;
    }


    @Override
    public boolean isDynamicStar() {
        return type.getPolyType() == PolyType.DYNAMIC_STAR;
    }

}

