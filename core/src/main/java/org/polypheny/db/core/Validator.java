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

import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeFactory;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.Resources;
import org.polypheny.db.runtime.Resources.ExInst;

public interface Validator {

    /**
     * Returns whether a field is a system field. Such fields may have particular properties such as sortedness and nullability.
     *
     * In the default implementation, always returns {@code false}.
     *
     * @param field Field
     * @return whether field is a system field
     */
    boolean isSystemField( RelDataTypeField field );

    /**
     * Derives the type of a node in a given scope. If the type has already been inferred, returns the previous type.
     *
     * @param scope Syntactic scope
     * @param operand Parse tree node
     * @return Type of the SqlNode. Should never return <code>NULL</code>
     */
    RelDataType deriveType( ValidatorScope scope, Node operand );

    /**
     * Returns the type factory used by this validator.
     *
     * @return type factory
     */
    RelDataTypeFactory getTypeFactory();

    /**
     * Adds "line x, column y" context to a validator exception.
     *
     * Note that the input exception is checked (it derives from {@link Exception}) and the output exception is unchecked (it derives from {@link RuntimeException}). This is intentional -- it should remind code
     * authors to provide context for their validation errors.
     *
     * @param node The place where the exception occurred, not null
     * @param e The validation error
     * @return Exception containing positional information, never null
     */
    <T extends Exception & ValidatorException> PolyphenyDbContextException newValidationError( Node node, Resources.ExInst<T> e );

}
