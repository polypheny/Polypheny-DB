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

package org.polypheny.db.type;


import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;


/**
 * Reusable {@link AlgDataType} fixtures for tests.
 */
class SqlTypeFixture {

    PolyTypeFactoryImpl typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
    final AlgDataType sqlBoolean = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BOOLEAN ), false );
    final AlgDataType sqlBigInt = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BIGINT ), false );
    final AlgDataType sqlBigIntNullable = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.BIGINT ), true );
    final AlgDataType sqlInt = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.INTEGER ), false );
    final AlgDataType sqlDate = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.DATE ), false );
    final AlgDataType sqlVarchar = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.VARCHAR ), false );
    final AlgDataType sqlChar = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.CHAR ), false );
    final AlgDataType sqlVarcharNullable = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.VARCHAR ), true );
    final AlgDataType sqlNull = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.NULL ), false );
    final AlgDataType sqlAny = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.ANY ), false );
    final AlgDataType sqlFloat = typeFactory.createTypeWithNullability( typeFactory.createPolyType( PolyType.FLOAT ), false );
    final AlgDataType arrayFloat = typeFactory.createTypeWithNullability( typeFactory.createArrayType( sqlFloat, -1 ), false );
    final AlgDataType arrayBigInt = typeFactory.createTypeWithNullability( typeFactory.createArrayType( sqlBigIntNullable, -1 ), false );
    final AlgDataType multisetFloat = typeFactory.createTypeWithNullability( typeFactory.createMultisetType( sqlFloat, -1 ), false );
    final AlgDataType multisetBigInt = typeFactory.createTypeWithNullability( typeFactory.createMultisetType( sqlBigIntNullable, -1 ), false );
    final AlgDataType arrayBigIntNullable = typeFactory.createTypeWithNullability( typeFactory.createArrayType( sqlBigIntNullable, -1 ), true );
    final AlgDataType arrayOfArrayBigInt = typeFactory.createTypeWithNullability( typeFactory.createArrayType( arrayBigInt, -1 ), false );
    final AlgDataType arrayOfArrayFloat = typeFactory.createTypeWithNullability( typeFactory.createArrayType( arrayFloat, -1 ), false );

}

