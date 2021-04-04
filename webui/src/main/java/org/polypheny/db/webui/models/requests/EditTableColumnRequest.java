package org.polypheny.db.webui.models.requests;

/**
 * Model for a request to edit the columns of a table
 */
public class EditTableColumnRequest extends EditTableRequest{
    /* column which is supposed to be moved */
    public String requestColumn;

    /* column next to which the requested column is to be placed */
    public String targetColumn;

    /* indicates if the requested column is to be placed above or below the target column */
    public boolean isRequestColumnAboveTarget;
}
