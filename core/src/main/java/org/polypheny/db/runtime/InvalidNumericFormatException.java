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

package org.polypheny.db.runtime;


/**
 * Marks a failed numeric cast while preserving the original diagnostic.
 */
public final class InvalidNumericFormatException extends NumberFormatException implements SqlStateProvider {

    public InvalidNumericFormatException( NumberFormatException cause ) {
        super( cause.getMessage() );
        initCause( cause );
    }


    @Override
    public String getSqlState() {
        return "22018";
    }


    @Override
    public String toString() {
        // Cause-only wrappers use the original exception's class name in their message.
        return getCause().toString();
    }

}
