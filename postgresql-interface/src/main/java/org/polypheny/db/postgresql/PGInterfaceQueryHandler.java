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

import io.netty.channel.ChannelHandlerContext;

public class PGInterfaceQueryHandler {
    String query;
    ChannelHandlerContext ctx;

    public PGInterfaceQueryHandler (String query, ChannelHandlerContext ctx) {
        this.query = query;
        this.ctx = ctx;
    }

    public void sendQueryToPolypheny() {
        String type = ""; //query type according to answer tags
        //get result from polypheny

        //how to handle reusable queries?? (do i have to safe them/check if it is reusable?)

        //handle result --> depending on query type, prepare answer message accordingly here (flush it)
        switch (type) {
            case "INSERT":
                //INSERT oid rows (oid=0, rows = #rows inserted)
                //1....2....n....C....INSERT 0 1.Z....I
                break;

            case "SELECT" :
                //SELECT rows (rows = #rows retrieved --> used for SELECT and CREATE TABLE AS commands)
                //1....2....T.....  lolid...@  .  . .  ... .  . .  .  .  .  .  ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
                //1....2....T.....1 lolid...40 02 . 01 ... 17 . 04 ff ff ff ff ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
                break;

            case "DELETE" :
                //DELETE rows (rows = #rows deleted)
                break;

            case "MOVE":
                //MOVE rows (rows = #rows the cursor's position has been changed by (??))
                break;

            case "FETCH":
                //FETCH rows (rows = #rows that have been retrieved from cursor)
                break;

            case "COPY":
                //COPY rows (rows = #rows copied --> only on PSQL 8.2 and later)
                break;

        }
    }

    //(SELECT empid FROM public.emps LIMIT 1) in postgres
    /*
1....2....T......empid...@...............D..........100C....SELECT 1.Z....I

1...  .  2...  .  T ...  . .  . empid...  @  . .  . ...  . .  .  .  .  .  . ..  D ...  . .  . ...  .  1  0  0  C ...  . SELECT 1.  Z ... .  I
1... 04 32... 04 54 ... 1e . 01 empid... 40 0c . 01 ... 17 . 04 ff ff ff ff .. 44 ... 0d . 01 ... 03 31 30 30 43 ... 0d SELECT 1. 5a ...05 49

empid = 65 6d 70 69 64
SELECT 1 = 53 45 4c 45 43 54 20 31
(select_abst._1)
     */



    //Example of server answer to simple select query (from real server)
    /*
    1....2....T......lolid...@...............D..........1D..........2D..........3D..........3D..........3D..........3C...
SELECT 6.Z....I

(result: 1,2,3,3,3,3)
1: ParseComplete indicator
2: BindComplete indicator
T: RowDescription - specifies the number of fields in a row (can be 0) - then for each field:
	field name (string),  lolid
	ObjectID of table (if field can be id'd as col of specific table, otherwise 0) (Int32), 40
	attributeNbr of col (if field can be id'd as col of specific table, otherwise 0) (Int16), 2
	ObjectID of fields data type (Int32), 1
	data type size (negative vals = variable-width types) (see pg_type.typlen) (Int16), 17
	type modifier (meaning of modifier is type-specific) (see pg_attribute.atttypmod) (Int32), 4
	Format code used for the field (zero(text) or one(binary)) --> if rowDescription is returned from statement variant of DESCRIBE: format code not yet known (always zero) (Int16)

D: DataRow - length - nbr of col values that follow (possible 0) - then for each column the pair of fields:
	length of the column value (not includes itself) (zero possible, -1: special case - NULL col val (no value bytes follow in the NULL case),
	value of the col (in format indicated by associated format code)
T: RowDescription - specifies the number of fields in a row (can be 0) (as message content!!) - then for each field:
	field name (string),  lolid
	ObjectID of table (if field can be id'd as col of specific table, otherwise 0) (Int32), 40 --> kompliziert
	attributeNbr of col (if field can be id'd as col of specific table, otherwise 0) (Int16), 2
	ObjectID of fields data type (Int32), 1

	Specifies the object ID of the parameter data type. Placing a zero here is equivalent to leaving the type unspecified.
	--> apparently specified in parse message (at the end, if 0, then unspecified...)

	data type size (negative vals = variable-width types) (see pg_type.typlen) (Int16), 17 --> polypheny website, typedokumentation, mit länge
	real and double in polypheny s gliiche --> luege was postgres macht, mind. länge aaluege --> postgresqlStore schauen welche grössen wie gemappt
	gibt methode um sql type zu holen dort --> luege wies dbms meta macht (avatica generall interface hauptklasse)

	type modifier (meaning of modifier is type-specific) (see pg_attribute.atttypmod) (Int32), 4

	Format code used for the field (zero(text) or one(binary)) --> if rowDescription is returned from statement variant of DESCRIBE: format code not yet known (always zero) (Int16)

D: DataRow - length - nbr of col values that follow (possible 0) - then for each column the pair of fields: (int16)
	length of the column value (not includes itself) (zero possible, -1: special case - NULL col val (no value bytes follow in the NULL case), (int32)
	value of the col (in format indicated by associated format code) (string)00

C: CommandComplete - msgBody is commandTag (which sql command was completed)
	SET (not in list on website, but "observed in the wild"),
	INSERT oid rows (oid=0, rows = #rows inserted),
	SELECT rows (rows = #rows retrieved --> used for SELECT and CREATE TABLE AS commands),
	UPDATE rows (rows = #rows updated),
	DELETE rows (rows = #rows deleted),
	MOVE rows (rows = #rows the cursor's position has been changed by (??)),
	FETCH rows (rows = #rows that have been retrieved from cursor),
	COPY rows (rows = #rows copied --> only on PSQL 8.2 and later)

Z: Ready for query (tags)
	I: idle


1....2....T.....  lolid...@  .  . .  ... .  . .  .  .  .  .  ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
1....2....T.....1 lolid...40 02 . 01 ... 17 . 04 ff ff ff ff ..D..........1D..........2D..........3D..........3D..........3D..........3C...SELECT 6.Z....I
differences from wireshark Data.data (diffs at right position)
ff's format code?


from website:
atttypmod int4
atttypmod records type-specific data supplied at table creation time (for example, the maximum length of a varchar column).
It is passed to type-specific input functions and length coercion functions. The value will generally be -1 for types that do not need atttypmod.

typlen int2
For a fixed-size type, typlen is the number of bytes in the internal representation of the type. But for a variable-length type, typlen is negative.
-1 indicates a “varlena” type (one that has a length word), -2 indicates a null-terminated C string.

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
1....2....T...S..albumid...@...............title...@...............artistid...@...............D..........1....Hello....1D..........2....Hello....2D..........3....lol....3C...SELECT 3.Z....I
1....2....T...S..albumid...@...............title...@...............artistid...@...............D..........1....Hello....1D..........2....Hello....2D..........3....lol....3C...SELECT 3.Z....I

---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
insert:

P...).INSERT INTO lol(LolId) VALUES (4)...B............D....P.E...	.....S....
1....2....n....C....INSERT 0 1.Z....I
X....

n: noData indicator
C: CommandComplete


     */
}
