/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.languages;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;

public class DefaultRegisterer {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;


    public static void registerOperators() {
        if ( isInit ) {
            throw new RuntimeException( "Mql operators were already registered." );
        }

        register( OperatorName.DESERIALIZE_DIRECTORY, new LangFunctionOperator( "DESERIALIZE_DIRECTORY", Kind.DESERIALIZE ) );

        register( OperatorName.DESERIALIZE_LIST, new LangFunctionOperator( "DESERIALIZE_LIST", Kind.DESERIALIZE ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( null, name, operator );
    }

}
