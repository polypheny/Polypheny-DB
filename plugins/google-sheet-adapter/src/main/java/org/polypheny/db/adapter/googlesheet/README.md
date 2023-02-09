# For user website

---
layout: plain
title: CSV Adapter
---

The Google Sheet Adapter is a data-source adapter that allows data to be sourced from any Google Sheet URL into Polypheny
as relational tables. The adapter is read-only, with DML queries not supported. 
The content of the google sheets can be changed in the background as long
as the names of the files and the columns itself don't change. 

The first column will always be the primary key. The Adapter also does not
support null values

For formatting, the adapter itself adjusts to different formatting, but keep in mind there may be issues in the following cases:

- There are two tables nested within the same sheet in the URL (will cause error)
- There is a gap between the table rows (the adapter won't read after the gap)

Otherwise, as long as the email used has access to the sheet, there should be no issues.

## Adapter settings

The Google Sheet Source Adapter has the following settings:

| Name | Description                                                                                                                                                                           |
|---------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Name  | Unique name for Google Sheet URL tables                                                                                                                                               |
| sheetsURL | the Google Sheet URL to source from                                                                                                                                                   |
| maxStringLength | the maximum size of the strings in all tables                                                                                                                                         |
| querySize | the number of rows scanned per network-call with the GoogleSheet API                                                                                                                  |
| resetRefreshToken | type "Yes" to delete the current refresh token associated to the current email used to read from Google Sheets and use another account for the Google Sheet Adapter. "No" is default. |



## Supported Data Types

It should be noted that all columns in the GoogleSheetAdapters are of type string, unless the columns of the sheet
are specially formatted. More specifically, all columns will have type string unless the column name is formatted as
`columnName:type`, in which case the type of column will be the specified type. Types can be one of the following: `int, string, boolean, long, float, double, date, time, timestamp`. 

For example: 
- If the Google Sheet has `user_id:int` as the column name, the imported column name would be `user_id` and the column type would be `int`
- If the Google Sheet has `birthday:date` as the column, then the imported column name would be `birthday` and the column type would be `date`
- If the Google Sheet has `age` as the column name, then even if all values in the column are of type `int`, it would still be a `string` when imported
into Polypheny

## Deployment

The adapter can be deployed using Polypheny-UI (Adapters -> Sources), or using the following SQL statement:


{% highlight sql %} ALTER ADAPTERS ADD unique_name USING 'org.polypheny.db.adapter.googlesheet.GoogleSheetSource' WITH 
'{maxStringLength: "255", querySize: "1000", sheetsURL: "https://docs.google.com/spreadsheets/d/1-int7xwx0UyyigB4FLGMOxaCiuHXSNhi09fYSuAIX2Q/edit#gid=0",
mode: "remote", resetRefreshToken: "No"}' {% highlight sql %}

Please refer to the settings above and alter the maxStringLength, querySize, ... as needed.  After successful deployment, all 
Google Sheets are mapped as a table in the public schema. 


---

# For future development

**Token management**

- The application currently runs using the Google Cloud Console under a private email's project. Please create a new project 
under the Polypheny email, enable the Google Sheet API, and add an OAUTH credential under the "Credentials" section. The credential
should be configured with origins as needed, with a http://localhost:8888/Callback and http://localhost:8888 redirect URL.
  (http://localhost:8888 is used because this is the port in which the GoogleSheetReader opens up to receive the access tokens 
from OAUTH. This can be changed as needed.) Then, download the credentials and add it to `google-sheet-adapter/src/main/resources/credentials.json`.

- Finally, there is the unhandled issue of expiring refresh tokens roughly after 1 week  (which can cause errors to the users). 
Users can fix this manually with the resetRefreshToken option  in the settings. However, to improve the experience and 
unlimit the number of API calls to Google Sheets servers, it is necessary to go to OAuth Consent Screen, and "Publish App"
which then extends refresh tokens for infinite use along with removes the API call limit.

**Improvements which can be made**

- Splitting GoogleSheetTableScanProject -> Scan and Project classes separately.

- Moving the GoogleSheetSourceTest from `dbms/src/test/java/org.polypheny.db/adapter/` to google-sheet-adapter package.
