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

package org.polypheny.db.test.catalog;


import java.util.Arrays;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeComparability;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeFieldImpl;
import org.polypheny.db.rel.type.StructKind;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.type.ObjectPolyType;
import org.polypheny.db.type.PolyType;


/**
 * Types used during initialization.
 */
final class Fixture {

    final RelDataType intType;
    final RelDataType intTypeNull;
    final RelDataType varchar10Type;
    final RelDataType varchar10TypeNull;
    final RelDataType varchar20Type;
    final RelDataType varchar20TypeNull;
    final RelDataType timestampType;
    final RelDataType timestampTypeNull;
    final RelDataType dateType;
    final RelDataType booleanType;
    final RelDataType booleanTypeNull;
    final RelDataType rectilinearCoordType;
    final RelDataType rectilinearPeekCoordType;
    final RelDataType rectilinearPeekNoExpandCoordType;
    final RelDataType abRecordType;
    final RelDataType skillRecordType;
    final RelDataType empRecordType;
    final RelDataType empListType;
    final ObjectPolyType addressType;


    Fixture( RelDataTypeFactory typeFactory ) {
        intType = typeFactory.createPolyType( PolyType.INTEGER );
        intTypeNull = typeFactory.createTypeWithNullability( intType, true );
        varchar10Type = typeFactory.createPolyType( PolyType.VARCHAR, 10 );
        varchar10TypeNull = typeFactory.createTypeWithNullability( varchar10Type, true );
        varchar20Type = typeFactory.createPolyType( PolyType.VARCHAR, 20 );
        varchar20TypeNull = typeFactory.createTypeWithNullability( varchar20Type, true );
        timestampType = typeFactory.createPolyType( PolyType.TIMESTAMP );
        timestampTypeNull = typeFactory.createTypeWithNullability( timestampType, true );
        dateType = typeFactory.createPolyType( PolyType.DATE );
        booleanType = typeFactory.createPolyType( PolyType.BOOLEAN );
        booleanTypeNull = typeFactory.createTypeWithNullability( booleanType, true );
        rectilinearCoordType =
                typeFactory.builder()
                        .add( "X", null, intType )
                        .add( "Y", null, intType )
                        .build();
        rectilinearPeekCoordType =
                typeFactory.builder()
                        .add( "X", null, intType )
                        .add( "Y", null, intType )
                        .kind( StructKind.PEEK_FIELDS )
                        .build();
        rectilinearPeekNoExpandCoordType =
                typeFactory.builder()
                        .add( "M", null, intType )
                        .add( "SUB",
                                null,
                                typeFactory.builder()
                                        .add( "A", null, intType )
                                        .add( "B", null, intType )
                                        .kind( StructKind.PEEK_FIELDS_NO_EXPAND )
                                        .build() )
                        .kind( StructKind.PEEK_FIELDS_NO_EXPAND )
                        .build();
        abRecordType =
                typeFactory.builder()
                        .add( "A", null, varchar10Type )
                        .add( "B", null, varchar10Type )
                        .build();
        skillRecordType =
                typeFactory.builder()
                        .add( "TYPE", null, varchar10Type )
                        .add( "DESC", null, varchar20Type )
                        .add( "OTHERS", null, abRecordType )
                        .build();
        empRecordType =
                typeFactory.builder()
                        .add( "EMPNO", null, intType )
                        .add( "ENAME", null, varchar10Type )
                        .add( "DETAIL",
                                null,
                                typeFactory.builder()
                                        .add( "SKILLS", null, typeFactory.createArrayType( skillRecordType, -1 ) )
                                        .build() )
                        .build();
        empListType = typeFactory.createArrayType( empRecordType, -1 );
        addressType =
                new ObjectPolyType( PolyType.STRUCTURED,
                        new SqlIdentifier( "ADDRESS", SqlParserPos.ZERO ),
                        false,
                        Arrays.asList(
                                new RelDataTypeFieldImpl( "STREET", 0, varchar20Type ),
                                new RelDataTypeFieldImpl( "CITY", 1, varchar20Type ),
                                new RelDataTypeFieldImpl( "ZIP", 2, intType ),
                                new RelDataTypeFieldImpl( "STATE", 3, varchar20Type ) ),
                        RelDataTypeComparability.NONE );
    }
}

