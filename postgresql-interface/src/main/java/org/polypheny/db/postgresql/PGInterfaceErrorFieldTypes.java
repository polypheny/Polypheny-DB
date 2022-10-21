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

package org.polypheny.db.postgresql;

/**
 * The Fields which appear in Error and Notice Messages. They are usually followed by a String which contains the field value
 * For more information see: https://www.postgresql.org/docs/current/protocol-error-fields.html
 */
public enum PGInterfaceErrorFieldTypes {

    /**
     * Severity: the field contents are ERROR, FATAL, or PANIC (in an error message), or WARNING, NOTICE, DEBUG, INFO,
     * or LOG (in a notice message), or a localized translation of one of these. Always present.
     */
    S,

    /**
     * Code: the SQLSTATE code for the error. Not localizable. Always present.
     */
    C,

    /**
     * Message: the primary human-readable error message. This should be accurate but terse (typically one line). Always present.
     */
    M


}
