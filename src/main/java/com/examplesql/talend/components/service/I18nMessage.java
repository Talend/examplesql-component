package com.examplesql.talend.components.service;

import org.talend.sdk.component.api.internationalization.Internationalized;

@Internationalized
public interface I18nMessage {

        String errorDriverDeregister(String type);

        String warnDriverClose(String type);

        String errorDriverLoad(String driverId, String missingJars);

        String errorEmptyJdbcURL();

        String errorCantLoadDriver(String dbType);

        String errorUnsupportedSubProtocol();

        String errorInvalidConnection();

        String errorDriverNotFound(String dbType);

        String errorSQL(int errorCode, String message);

        String errorDriverInstantiation(String message);

        String successConnection();

        String errorUnauthorizedQuery();

        String errorEmptyQuery();

        String warnResultSetCantBeClosed();

        String warnStatementCantBeClosed();

        String warnConnectionCantBeClosed();

        String warnReadOnlyOptimisationFailure();

        String errorNoKeyForDeleteQuery();

        String errorNoKeyForUpdateQuery();

        String errorNoFieldForQueryParam(String field);

        String errorNoUpdatableColumnWasDefined();

        String errorUnsupportedDatabaseAction();

        String errorCantClearPreparedStatement();

        String errorCantClosePreparedStatement();

        String errorCantCloseJdbcConnectionProperly();

        String errorCantLoadTableSuggestions();

        String errorTaberDoesNotExists(String tableName);


        String errorUnsupportedDatabase(String dbType);

        String errorUnsupportedType(String type, String field);

        //
        String actionOnDataInsert();

        String actionOnDataUpdate();

        String actionOnDataUpsert();

        String actionOnDataDelete();

        String actionOnDataBulkLoad();

        String errorSingleSortKeyInvalid();

        String errorVacantAccountKey();

        String errorNoRecordReceived();


}
