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

package org.polypheny.db.core;

import javax.annotation.Nonnull;
import lombok.Getter;

public interface Function {

    default FunctionType getFunctionType() {
        throw new RuntimeException( "Not Implemented Yet" );
    }

    @Nonnull
    FunctionCategory getFunctionCategory();

    enum FunctionType {
        COUNT( true ),
        SINGLE_VALUE( true ),
        JSON_VALUE( false ),
        DOC_EQUAL( false ),
        CAST( false );

        @Getter
        private final boolean isAgg;


        FunctionType( boolean isAgg ) {
            this.isAgg = isAgg;
        }
    }

}
