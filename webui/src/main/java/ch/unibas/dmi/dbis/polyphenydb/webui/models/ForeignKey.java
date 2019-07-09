/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui.models;


import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.CatalogTransactionException;
import ch.unibas.dmi.dbis.polyphenydb.webui.transactionmanagement.LocalTransactionHandler;
import java.sql.SQLException;
import lombok.Builder;


/**
 * Model for a ForeignKey
 */
@Builder
public class ForeignKey {

    String fkName;
    String pkName;

    String pkTableSchema;
    String pkTableName;
    String pkColumnName;

    String fkTableSchema;
    String fkTableName;
    String fkColumnName;

    String update;
    String delete;


    /**
     * Generates and executes the query to create the ForeignKey in the DBMS.
     */
    public void create ( final LocalTransactionHandler handler ) throws SQLException, CatalogTransactionException {
        String sql = String.format( "ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s) ON UPDATE %s ON DELETE %s",
                this.fkTableName, this.fkName, this.fkColumnName, this.pkTableName, this.pkColumnName, this.update, this.delete );
        System.out.println(sql);
        handler.executeUpdate( sql );
        handler.commit();
    }

}
