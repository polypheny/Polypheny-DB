### For users

The Google Sheet Adapter is a data-source adapter that allows data to be sourced from any Google Sheet URL into Polypheny. To do so, please go into the 
Adapters section, choose data source, then Google Sheets. Here, you will be presented with multiple values to fill out:

- Name: name of the schema
- sheetsURL: the Google Sheet URL to source from.
- MaxStringLength: max length of a string in all sheets in the sheetsURL
- querySize: the number of rows scanned per query on the GoogleSheet.
- resetRefreshToken: type "YES" to delete the current refresh token and use another account for the sheet adapter. 

 The adapter adjusts itself to different formats of the sheet, but keep in mind there may be issues in the following cases:

- There are two tables nested within the same sheet in the URL 
- There is a gap between the table rows (the adapter won't read after the gap)

Otherwise, as long as the email used has access to the sheet, there should be no issues. 


### For future development

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