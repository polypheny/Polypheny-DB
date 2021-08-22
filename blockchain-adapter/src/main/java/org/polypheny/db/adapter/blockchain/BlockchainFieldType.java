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

package org.polypheny.db.adapter.blockchain;


import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.type.PolyType;

import java.util.HashMap;
import java.util.Map;


/**
 * Type of a field in a CSV file.
 * <p>
 * Usually, and unless specified explicitly in the header row, a field is of type {@link #STRING}. But specifying the field type in the header row makes it easier to write SQL.
 */
enum BlockchainFieldType {
    STRING(String.class, "string"),
    BOOLEAN(Primitive.BOOLEAN),
    BYTE(Primitive.BYTE),
    CHAR(Primitive.CHAR),
    SHORT(Primitive.SHORT),
    INT(Primitive.INT),
    LONG(Primitive.LONG),
    FLOAT(Primitive.FLOAT),
    DOUBLE(Primitive.DOUBLE),
    DATE(java.sql.Date.class, "date"),
    TIME(java.sql.Time.class, "time"),
    TIMESTAMP(java.sql.Timestamp.class, "timestamp");

    private static final Map<String, BlockchainFieldType> MAP = new HashMap<>();

    static {
        for (BlockchainFieldType value : values()) {
            MAP.put(value.simpleName, value);
        }
    }

    private final Class clazz;
    private final String simpleName;


    BlockchainFieldType(Primitive primitive) {
        this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
    }


    BlockchainFieldType(Class clazz, String simpleName) {
        this.clazz = clazz;
        this.simpleName = simpleName;
    }


    public static BlockchainFieldType getBlockchainFieldType(PolyType type) {
        switch (type) {
            case BOOLEAN:
                return BlockchainFieldType.BOOLEAN;
            case VARBINARY:
                return BlockchainFieldType.BYTE;
            case INTEGER:
                return BlockchainFieldType.INT;
            case BIGINT:
                return BlockchainFieldType.LONG;
            case REAL:
                return BlockchainFieldType.FLOAT;
            case DOUBLE:
                return BlockchainFieldType.DOUBLE;
            case VARCHAR:
                return BlockchainFieldType.STRING;
            case DATE:
                return BlockchainFieldType.DATE;
            case TIME:
                return BlockchainFieldType.TIME;
            case TIMESTAMP:
                return BlockchainFieldType.TIMESTAMP;
            default:
                throw new RuntimeException("Unsupported datatype: " + type.name());
        }
    }

    public static BlockchainFieldType of(String typeString) {
        return MAP.get(typeString);
    }

    public RelDataType toType(JavaTypeFactory typeFactory) {
        RelDataType javaType = typeFactory.createJavaType(clazz);
        RelDataType sqlType = typeFactory.createPolyType(javaType.getPolyType());
        return typeFactory.createTypeWithNullability(sqlType, true);
    }
}
