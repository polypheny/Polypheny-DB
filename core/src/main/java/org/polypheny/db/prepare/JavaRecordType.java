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
 */

package org.polypheny.db.prepare;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgRecordType;


/**
 * Record type based on a Java class. The fields of the type are the fields of the class.
 *
 * <strong>NOTE: This class is experimental and subject to change/removal without notice</strong>.
 */
public class JavaRecordType extends AlgRecordType {

    final Class clazz;


    public JavaRecordType( List<AlgDataTypeField> fields, Class clazz ) {
        super( fields );
        this.clazz = Objects.requireNonNull( clazz );
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof JavaRecordType
                && fields.equals( ((JavaRecordType) obj).fields )
                && clazz == ((JavaRecordType) obj).clazz;
    }


    @Override
    public int hashCode() {
        return Objects.hash( fields, clazz );
    }

}
