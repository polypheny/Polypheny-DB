/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.processing;


import lombok.Value;
import org.polypheny.db.ResultIterator;

@Value
public class ResultContext {

    ResultIterator iterator;

    String error;

    QueryContext query;

    double executionTime;


    public static ResultContext ofError( String error, QueryContext context, double executionTime ) {
        return new ResultContext( null, error, context, executionTime );
    }

}
