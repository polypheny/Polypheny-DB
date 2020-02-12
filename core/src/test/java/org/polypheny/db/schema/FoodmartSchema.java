/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.schema;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import lombok.AllArgsConstructor;


public class FoodmartSchema {

    public final DateFormat dateFormat = new SimpleDateFormat( "dd.MM.yyyy" );

    public final SalesFact[] sales_fact_1997 = {
            new SalesFact( 12321, 1, 1, 22, 12, 1, 67, 1 ),
    };

    public final Product[] product = {
            new Product( 1, 12321, "The fancy soap company", "Dispenser 101", 21, 89, 1020, 950, true, false, 1, 42, 12, 10, 5 ),
    };

    public final Customer[] customer = {
            new Customer( 1, 1221, "Mayer", "Hans-Rudi", "Jacob", "Im Gässli 2", "", "", "", "Basel", "Basel-Stadt", 4001, "Switzerland", 24, "053151662", "07151562", parseDate( "03.06.1981" ), 1, 180000, 0, 2, 2, 3, parseDate( "05.01.2018" ), true, 4, true, 2, "Hans-Rudi Mayer" ),
    };

    public final ProductClass[] product_class = {
            new ProductClass( 1, 1, 3, 1, 1 ),
    };

    public final Store[] store = {
            new Store( 1, 3, 2, "Basel Mega Store", 1, "Mega Store Strasse 1", "Basel", "Basel-Stadt", 4001, "Switzerland", 1, "0622625512", "05236462", parseDate( "01.01.2005" ), parseDate( "01.01.2017" ), 1500, 800, 200, 500, true, true, true, true, true ),
    };

    public final Department[] department = {
            new Department( 1, "IT" ),
    };

    public final Employee[] employee = {
            new Employee( 1, "Hans Müller", "Hans", "Müller", 23, "Head of something", 1, 1, parseDate( "23.06.1979" ), parseDate( "05.09.1999" ), null, 6500, 2, 3, 1, 0, "Mgn" ),
    };

    public final ReserveEmployee[] reserve_employee = {
            new ReserveEmployee( 1, "y", 2, "Müller", "uni", 2, parseDate( "05.09.1999" ), 1, 1, "m", "Hans Müller", parseDate( "23.6.1979" ), 6500, "Head of something", null, "Hans" ),
    };

    public final Salary[] salary = {
            new Salary( 11, 2, 1, parseDate( "01.01.2019" ), 12, 6500, 500, 1 ),
    };

    public final WarehouseClass[] warehouse_class = {
            new WarehouseClass( 1, "Foo" ),
    };

    public final ExpenseFact[] expense_fact = {
            new ExpenseFact( 1, 140, 1, "2", 1, parseDate( "03.04.2018" ), 145 ),
    };


    private Date parseDate( String str ) {
        try {
            return dateFormat.parse( str );
        } catch ( ParseException e ) {
            e.printStackTrace();
        }
        return null;
    }


    @AllArgsConstructor
    public static class SalesFact {

        public final int product_id;
        public final int time_id;
        public final int customer_id;
        public final int promotion_id;
        public final int store_id;
        public final int store_sales;
        public final int store_cost;
        public final int unit_sales;
    }


    @AllArgsConstructor
    public static class Customer {

        public final int customer_id;
        public final int account_num;
        public final String lname;
        public final String fname;
        public final String mi;
        public final String address1;
        public final String address2;
        public final String address3;
        public final String address4;
        public final String city;
        public final String state_province;
        public final int postal_code;
        public final String country;
        public final int customer_region_id;
        public final String phone1;
        public final String phone2;
        public final Date birthdate;
        public final int marital_status;
        public final int yearly_income;
        public final int gender;
        public final int total_children;
        public final int num_children_at_home;
        public final int education;
        public final Date date_accnt_opened;
        public final boolean member_card;
        public final int occupation;
        public final boolean houseowner;
        public final int num_cars_owned;
        public final String fullname;
    }


    @AllArgsConstructor
    public static class Product {

        public final int product_class_id;
        public final int product_id;
        public final String brand_name;
        public final String product_name;
        public final int SKU; // Stock Keeping Unit
        public final int SRP; // Suggested Retail Price
        public final int gross_weight;
        public final int net_weight;
        public final boolean recyclable_package;
        public final boolean low_fat;
        public final int units_per_case;
        public final int cases_per_pallet;
        public final int shelf_width;
        public final int shelf_height;
        public final int shelf_depth;
    }


    @AllArgsConstructor
    public static class ProductClass {

        public final int product_class_id;
        public final int product_subcategory;
        public final int product_category;
        public final int product_department;
        public final int product_family;
    }


    @AllArgsConstructor
    public static class Store {

        public final int store_id;
        public final int store_type;
        public final int region_id;
        public final String store_name;
        public final int store_number;
        public final String store_street_address;
        public final String store_city;
        public final String store_state;
        public final int store_postal_code;
        public final String store_country;
        public final int store_manager;
        public final String store_phone;
        public final String store_fax;
        public final Date first_opened_date;
        public final Date last_remodel_date;
        public final int store_sqft;
        public final int grocery_sqft;
        public final int frozen_sqft;
        public final int meat_sqft;
        public final boolean coffee_bar;
        public final boolean video_store;
        public final boolean salad_bar;
        public final boolean prepared_food;
        public final boolean florist;
    }


    @AllArgsConstructor
    public static class Department {

        public final int department_id;
        public final String department_description;
    }


    @AllArgsConstructor
    public static class Employee {

        public final int employee_id;
        public final String full_name;
        public final String first_name;
        public final String last_name;
        public final int position_id;
        public final String position_title;
        public final int store_id;
        public final int department_id;
        public final Date birth_date;
        public final Date hire_date;
        public final Date end_date;
        public final int salary;
        public final int supervisor_id;
        public final int education_level;
        public final int marital_status;
        public final int gender;
        public final String management_role;
    }


    @AllArgsConstructor
    public static class ReserveEmployee {

        public final int store_id;
        public final String marital_status;
        public final int position_id;
        public final String last_name;
        public final String education_level;
        public final int supervisor_id;
        public final Date hire_date;
        public final int employee_id;
        public final int department_id;
        public final String gender;
        public final String full_name;
        public final Date birth_date;
        public final float salary;
        public final String position_title;
        public final Date end_date;
        public final String first_name;
    }


    @AllArgsConstructor
    public static class Salary {

        public final int vacation_used;
        public final int currency_id;
        public final int employee_id;
        public final Date pay_date;
        public final int vacation_accrued;
        public final float salary_paid;
        public final float overtime_paid;
        public final int department_id;
    }


    @AllArgsConstructor
    public static class WarehouseClass {

        public final int warehouse_class_id;
        public final String description;
    }


    @AllArgsConstructor
    public static class ExpenseFact {

        public final int time_id;
        public final float amount;
        public final int currency_id;
        public final String category_id;
        public final int store_id;
        public final Date exp_date;
        public final int account_id;
    }

}

