/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet;

import java.util.List;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.execution.ParquetPredicateBuilder;
import org.polypheny.db.adapter.parquet.model.FilterInfo;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;

import static org.junit.jupiter.api.Assertions.assertThrows;

class ParquetPredicateBuilderTest {

    @Test
    void rejectsInt96TimestampPredicatePushdown() {
        MessageType schema = Types.buildMessage()
                .optional( PrimitiveTypeName.INT96 )
                .named( "ts" )
                .named( "test_schema" );

        FilterInfo filter = new FilterInfo( 0, Kind.EQUALS, PolyTimestamp.of( 1_700_000_000_000L ) );

        assertThrows(
                IllegalArgumentException.class,
                () -> new ParquetPredicateBuilder().translate( schema, List.of( filter ) ) );
    }
}
