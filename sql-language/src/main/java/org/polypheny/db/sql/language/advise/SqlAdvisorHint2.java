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

package org.polypheny.db.sql.language.advise;


import org.polypheny.db.util.Moniker;


/**
 * This class is used to return values for {@link SqlAdvisor#getCompletionHints (String, int, String[])}.
 */
public class SqlAdvisorHint2 extends SqlAdvisorHint {

    /**
     * Replacement string
     */
    public final String replacement;


    public SqlAdvisorHint2( String id, String[] names, String type, String replacement ) {
        super( id, names, type );
        this.replacement = replacement;
    }


    public SqlAdvisorHint2( Moniker id, String replacement ) {
        super( id );
        this.replacement = replacement;
    }

}

