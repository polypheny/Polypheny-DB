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
 */

package org.polypheny.db.catalog;


import java.util.Arrays;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeComparability;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.type.ObjectPolyType;
import org.polypheny.db.type.PolyType;


/**
 * Types used during initialization.
 */
final class Fixture {

    final AlgDataType intType;
    final AlgDataType intTypeNull;
    final AlgDataType varchar10Type;
    final AlgDataType varchar10TypeNull;
    final AlgDataType varchar20Type;
    final AlgDataType varchar20TypeNull;
    final AlgDataType timestampType;
    final AlgDataType timestampTypeNull;
    final AlgDataType dateType;
    final AlgDataType booleanType;
    final AlgDataType booleanTypeNull;
    final AlgDataType rectilinearCoordType;
    final AlgDataType rectilinearPeekCoordType;
    final AlgDataType rectilinearPeekNoExpandCoordType;
    final AlgDataType abRecordType;
    final AlgDataType skillRecordType;
    final AlgDataType empRecordType;
    final AlgDataType empListType;
    final ObjectPolyType addressType;


    Fixture( AlgDataTypeFactory typeFactory ) {
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
                        .add(
                                "SUB",
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
                        .add(
                                "DETAIL",
                                null,
                                typeFactory.builder()
                                        .add( "SKILLS", null, typeFactory.createArrayType( skillRecordType, -1 ) )
                                        .build() )
                        .build();
        empListType = typeFactory.createArrayType( empRecordType, -1 );
        addressType =
                new ObjectPolyType(
                        PolyType.STRUCTURED,
                        new MockIdentifier( "ADDRESS", ParserPos.ZERO ),
                        false,
                        Arrays.asList(
                                new AlgDataTypeFieldImpl( "STREET", 0, varchar20Type ),
                                new AlgDataTypeFieldImpl( "CITY", 1, varchar20Type ),
                                new AlgDataTypeFieldImpl( "ZIP", 2, intType ),
                                new AlgDataTypeFieldImpl( "STATE", 3, varchar20Type ) ),
                        AlgDataTypeComparability.NONE );
    }

}

