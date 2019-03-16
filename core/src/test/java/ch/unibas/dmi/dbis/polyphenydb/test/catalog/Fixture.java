/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test.catalog;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeComparability;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFieldImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.StructKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.ObjectSqlType;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;
import java.util.Arrays;


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
    final ObjectSqlType addressType;


    Fixture( RelDataTypeFactory typeFactory ) {
        intType = typeFactory.createSqlType( SqlTypeName.INTEGER );
        intTypeNull = typeFactory.createTypeWithNullability( intType, true );
        varchar10Type = typeFactory.createSqlType( SqlTypeName.VARCHAR, 10 );
        varchar10TypeNull = typeFactory.createTypeWithNullability( varchar10Type, true );
        varchar20Type = typeFactory.createSqlType( SqlTypeName.VARCHAR, 20 );
        varchar20TypeNull = typeFactory.createTypeWithNullability( varchar20Type, true );
        timestampType = typeFactory.createSqlType( SqlTypeName.TIMESTAMP );
        timestampTypeNull = typeFactory.createTypeWithNullability( timestampType, true );
        dateType = typeFactory.createSqlType( SqlTypeName.DATE );
        booleanType = typeFactory.createSqlType( SqlTypeName.BOOLEAN );
        booleanTypeNull = typeFactory.createTypeWithNullability( booleanType, true );
        rectilinearCoordType =
                typeFactory.builder()
                        .add( "X", intType )
                        .add( "Y", intType )
                        .build();
        rectilinearPeekCoordType =
                typeFactory.builder()
                        .add( "X", intType )
                        .add( "Y", intType )
                        .kind( StructKind.PEEK_FIELDS )
                        .build();
        rectilinearPeekNoExpandCoordType =
                typeFactory.builder()
                        .add( "M", intType )
                        .add( "SUB",
                                typeFactory.builder()
                                        .add( "A", intType )
                                        .add( "B", intType )
                                        .kind( StructKind.PEEK_FIELDS_NO_EXPAND )
                                        .build() )
                        .kind( StructKind.PEEK_FIELDS_NO_EXPAND )
                        .build();
        abRecordType =
                typeFactory.builder()
                        .add( "A", varchar10Type )
                        .add( "B", varchar10Type )
                        .build();
        skillRecordType =
                typeFactory.builder()
                        .add( "TYPE", varchar10Type )
                        .add( "DESC", varchar20Type )
                        .add( "OTHERS", abRecordType )
                        .build();
        empRecordType =
                typeFactory.builder()
                        .add( "EMPNO", intType )
                        .add( "ENAME", varchar10Type )
                        .add( "DETAIL",
                                typeFactory.builder().add( "SKILLS",
                                        typeFactory.createArrayType( skillRecordType, -1 ) ).build() )
                        .build();
        empListType = typeFactory.createArrayType( empRecordType, -1 );
        addressType =
                new ObjectSqlType( SqlTypeName.STRUCTURED,
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

