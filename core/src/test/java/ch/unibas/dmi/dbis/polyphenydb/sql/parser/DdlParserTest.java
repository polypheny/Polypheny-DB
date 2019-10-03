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

package ch.unibas.dmi.dbis.polyphenydb.sql.parser;


import ch.unibas.dmi.dbis.polyphenydb.sql.parser.impl.SqlParserImpl;
import org.junit.Test;


/**
 * Tests SQL parser extensions for DDL.
 *
 * Remaining tasks:
 *
 * <ul>
 * <li>"create table x (a int) as values 1, 2" should fail validation; data type not allowed in "create table ... as".<li>
 *
 * <li>"create table x (a int, b int as (a + 1)) stored" should not allow b to be specified in insert; should generate check constraint on b; should populate b in insert as if it had a default<li>
 *
 * <li>"create table as select" should store constraints deduced by planner<li>
 *
 * <li>during CREATE VIEW, check for a table and a materialized view with the same name (they have the same namespace)<li>
 * </ul>
 */
public class DdlParserTest extends SqlParserTest {

    @Override
    protected SqlParserImplFactory parserImplFactory() {
        return SqlParserImpl.FACTORY;
    }


    @Override
    public void testGenerateKeyWords() {
        // by design, method only works in base class; no-ops in this sub-class
    }


    @Test
    public void testCreateSchema() {
        sql( "create schema x" ).ok( "CREATE SCHEMA `X`" );
    }


    @Test
    public void testCreateOrReplaceSchema() {
        sql( "create or replace schema x" ).ok( "CREATE OR REPLACE SCHEMA `X`" );
    }


    @Test
    public void testCreateTypeWithAttributeList() {
        sql( "create type x.mytype1 as (i int not null, j varchar(5) null)" )
                .ok( "CREATE TYPE `X`.`MYTYPE1` AS (`I` INTEGER NOT NULL, `J` VARCHAR(5))" );
    }


    @Test
    public void testCreateTypeWithBaseType() {
        sql( "create type mytype1 as varchar(5)" )
                .ok( "CREATE TYPE `MYTYPE1` AS VARCHAR(5)" );
    }


    @Test
    public void testCreateOrReplaceTypeWith() {
        sql( "create or replace type mytype1 as varchar(5)" )
                .ok( "CREATE OR REPLACE TYPE `MYTYPE1` AS VARCHAR(5)" );
    }


    @Test
    public void testCreateTable() {
        sql( "create table x (i int not null, j varchar(5) null)" )
                .ok( "CREATE TABLE `X` (`I` INTEGER NOT NULL, `J` VARCHAR(5))" );
    }


    @Test
    public void testCreateTableAsSelect() {
        final String expected = "CREATE TABLE `X` AS\n"
                + "SELECT *\n"
                + "FROM `EMP`";
        sql( "create table x as select * from emp" )
                .ok( expected );
    }


    @Test
    public void testCreateTableIfNotExistsAsSelect() {
        final String expected = "CREATE TABLE IF NOT EXISTS `X`.`Y` AS\n"
                + "SELECT *\n"
                + "FROM `EMP`";
        sql( "create table if not exists x.y as select * from emp" )
                .ok( expected );
    }


    @Test
    public void testCreateTableAsValues() {
        final String expected = "CREATE TABLE `X` AS\n"
                + "VALUES (ROW(1)),\n"
                + "(ROW(2))";
        sql( "create table x as values 1, 2" )
                .ok( expected );
    }


    @Test
    public void testCreateTableAsSelectColumnList() {
        final String expected = "CREATE TABLE `X` (`A`, `B`) AS\n"
                + "SELECT *\n"
                + "FROM `EMP`";
        sql( "create table x (a, b) as select * from emp" )
                .ok( expected );
    }


    @Test
    public void testCreateTableCheck() {
        final String expected = "CREATE TABLE `X` (`I` INTEGER NOT NULL, CONSTRAINT `C1` CHECK (`I` < 10), `J` INTEGER)";
        sql( "create table x (i int not null, constraint c1 check (i < 10), j int)" ).ok( expected );
    }


    @Test
    public void testCreateTableVirtualColumn() {
        final String sql = "create table if not exists x (\n"
                + " i int not null,\n"
                + " j int generated always as (i + 1) stored,\n"
                + " k int as (j + 1) virtual,\n"
                + " m int as (k + 1))";
        final String expected = "CREATE TABLE IF NOT EXISTS `X` "
                + "(`I` INTEGER NOT NULL,"
                + " `J` INTEGER AS (`I` + 1) STORED,"
                + " `K` INTEGER AS (`J` + 1) VIRTUAL,"
                + " `M` INTEGER AS (`K` + 1) VIRTUAL)";
        sql( sql ).ok( expected );
    }


    @Test
    public void testCreateView() {
        final String sql = "create or replace view v as\n"
                + "select * from (values (1, '2'), (3, '45')) as t (x, y)";
        final String expected = "CREATE OR REPLACE VIEW `V` AS\n"
                + "SELECT *\n"
                + "FROM (VALUES (ROW(1, '2')),\n"
                + "(ROW(3, '45'))) AS `T` (`X`, `Y`)";
        sql( sql ).ok( expected );
    }


    @Test
    public void testCreateOrReplaceFunction() {
        final String sql = "create or replace function if not exists x.udf\n"
                + " as 'ch.unibas.dmi.dbis.polyphenydb.udf.TableFun.demoUdf'\n"
                + "using jar 'file:/path/udf/udf-0.0.1-SNAPSHOT.jar',\n"
                + " jar 'file:/path/udf/udf2-0.0.1-SNAPSHOT.jar',\n"
                + " file 'file:/path/udf/logback.xml'";
        final String expected = "CREATE OR REPLACE FUNCTION"
                + " IF NOT EXISTS `X`.`UDF`"
                + " AS 'ch.unibas.dmi.dbis.polyphenydb.udf.TableFun.demoUdf'"
                + " USING JAR 'file:/path/udf/udf-0.0.1-SNAPSHOT.jar',"
                + " JAR 'file:/path/udf/udf2-0.0.1-SNAPSHOT.jar',"
                + " FILE 'file:/path/udf/logback.xml'";
        sql( sql ).ok( expected );
    }


    @Test
    public void testCreateOrReplaceFunction2() {
        final String sql = "create function \"my Udf\"\n"
                + " as 'ch.unibas.dmi.dbis.polyphenydb.udf.TableFun.demoUdf'";
        final String expected = "CREATE FUNCTION `my Udf`"
                + " AS 'ch.unibas.dmi.dbis.polyphenydb.udf.TableFun.demoUdf'";
        sql( sql ).ok( expected );
    }


    @Test
    public void testDropSchema() {
        sql( "drop schema x" ).ok( "DROP SCHEMA `X`" );
    }


    @Test
    public void testDropSchemaIfExists() {
        sql( "drop schema if exists x" ).ok( "DROP SCHEMA IF EXISTS `X`" );
    }


    @Test
    public void testDropType() {
        sql( "drop type X" ).ok( "DROP TYPE `X`" );
    }


    @Test
    public void testDropTypeIfExists() {
        sql( "drop type if exists X" ).ok( "DROP TYPE IF EXISTS `X`" );
    }


    @Test
    public void testDropTypeTrailingIfExistsFails() {
        sql( "drop type X ^if^ exists" ).fails( "(?s)Encountered \"if\" at.*" );
    }


    @Test
    public void testDropTable() {
        sql( "drop table x" ).ok( "DROP TABLE `X`" );
    }


    @Test
    public void testDropTableComposite() {
        sql( "drop table x.y" ).ok( "DROP TABLE `X`.`Y`" );
    }


    @Test
    public void testDropTableIfExists() {
        sql( "drop table if exists x" ).ok( "DROP TABLE IF EXISTS `X`" );
    }


    @Test
    public void testDropView() {
        sql( "drop view x" ).ok( "DROP VIEW `X`" );
    }


    @Test
    public void testDropFunction() {
        final String sql = "drop function x.udf";
        final String expected = "DROP FUNCTION `X`.`UDF`";
        sql( sql ).ok( expected );
    }


    @Test
    public void testDropFunctionIfExists() {
        final String sql = "drop function if exists \"my udf\"";
        final String expected = "DROP FUNCTION IF EXISTS `my udf`";
        sql( sql ).ok( expected );
    }

}

