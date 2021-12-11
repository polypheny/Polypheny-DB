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

package org.polypheny.db.nodes.validate;

import java.util.List;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.runtime.PolyphenyDbContextException;
import org.polypheny.db.runtime.Resources;

public interface Validator {

    /**
     * Returns whether a field is a system field. Such fields may have particular properties such as sortedness and nullability.
     *
     * In the default implementation, always returns {@code false}.
     *
     * @param field Field
     * @return whether field is a system field
     */
    boolean isSystemField( AlgDataTypeField field );

    /**
     * Derives the type of a node in a given scope. If the type has already been inferred, returns the previous type.
     *
     * @param scope Syntactic scope
     * @param operand Parse tree node
     * @return Type of the SqlNode. Should never return <code>NULL</code>
     */
    AlgDataType deriveType( ValidatorScope scope, Node operand );

    /**
     * Returns the type factory used by this validator.
     *
     * @return type factory
     */
    AlgDataTypeFactory getTypeFactory();

    Node validate( Node node );

    /**
     * Adds "line x, column y" context to a validator exception.
     *
     * Note that the input exception is checked (it derives from {@link Exception}) and the output exception is unchecked
     * (it derives from {@link RuntimeException}). This is intentional -- it should remind code authors to provide context
     * for their validation errors.
     *
     * @param node The place where the exception occurred, not null
     * @param e The validation error
     * @return Exception containing positional information, never null
     */
    PolyphenyDbContextException newValidationError( Node node, Resources.ExInst<ValidatorException> e );

    /**
     * Returns the type assigned to a node by validation.
     *
     * @param node the node of interest
     * @return validated type, never null
     */
    AlgDataType getValidatedNodeType( Node node );

    /**
     * Enables or disables expansion of identifiers other than column references.
     *
     * @param expandIdentifiers new setting
     */
    void setIdentifierExpansion( boolean expandIdentifiers );

    /**
     * Sets how NULL values should be collated if an ORDER BY item does not contain NULLS FIRST or NULLS LAST.
     */
    void setDefaultNullCollation( NullCollation nullCollation );

    /**
     * Returns a description of how each field in the row type maps to a catalog, schema, table and column in the schema.
     *
     * The returned list is never null, and has one element for each field in the row type. Each element is a list of four
     * elements (catalog, schema, table, column), or may be null if the column is an expression.
     *
     * @param sqlQuery Query
     * @return Description of how each field in the row type maps to a schema object
     */
    List<List<String>> getFieldOrigins( Node sqlQuery );

    /**
     * Returns a record type that contains the name and type of each parameter.
     * Returns a record type with no fields if there are no parameters.
     *
     * @param sqlQuery Query
     * @return Record type
     */
    AlgDataType getParameterRowType( Node sqlQuery );

    /**
     * Returns an object representing the "unknown" type.
     *
     * @return unknown type
     */
    AlgDataType getUnknownType();

    ValidatorNamespace getNamespace( Node node );

}
