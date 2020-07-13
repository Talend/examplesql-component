package com.examplesql.talend.components.output.statement;

import com.examplesql.talend.components.output.OutputConfiguration;
import com.examplesql.talend.components.output.statement.operations.*;
import com.examplesql.talend.components.service.I18nMessage;

public final class QueryManagerFactory {

    private QueryManagerFactory() {
    }

    public static QueryManagerImpl getQueryManager(final I18nMessage i18n,
                                                   final OutputConfiguration configuration) {
        switch (configuration.isActionOnData()) {
            case INSERT:
                return new Insert(configuration, i18n);
            case UPDATE:
                return new Update(configuration, i18n);
            case DELETE:
                return new Delete(configuration, i18n);
            case UPSERT:
                return new UpsertDefault(configuration, i18n);
            case BULK_LOAD:
                return new BulkLoad(configuration, i18n);
            default:
                throw new IllegalStateException(i18n.errorUnsupportedDatabaseAction());
        }

    }

}
