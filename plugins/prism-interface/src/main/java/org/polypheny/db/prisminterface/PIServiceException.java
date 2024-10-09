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

package org.polypheny.db.prisminterface;

import java.sql.SQLException;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;

public class PIServiceException extends GenericRuntimeException {


    public PIServiceException( String reason ) {
        super( reason );
    }


    public PIServiceException( SQLException sqlException ) {
        super( sqlException.getMessage(), sqlException );
    }


    public PIServiceException( Throwable cause ) {
        super( cause );
    }


    public PIServiceException( String reason, Throwable cause ) {
        super( reason, cause );
    }

}
