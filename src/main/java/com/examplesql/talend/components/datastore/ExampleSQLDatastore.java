package com.examplesql.talend.components.datastore;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;


@DataStore("ExampleSQLDatastore")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "jdbcUrl" }),
    @GridLayout.Row({ "user" }),
    @GridLayout.Row({ "password" })
})
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row("connectionTimeOut"),
        @GridLayout.Row("connectionValidationTimeOut") })
@Checkable("validateConnection")
@Documentation("Connection to a ExampleSQL Database")
public class ExampleSQLDatastore implements Serializable {

    @Option
    @Required
    @Documentation("jdbc connection url")
    private String jdbcUrl;

    @Option
    @Required
    @Documentation("database user")
    private String user;

    @Credential
    @Option
    @Documentation("database password")
    private String password;

    @Min(0)
    @Option
    @Documentation("Set the maximum number of seconds that a client will wait for a connection from the pool. "
            + "If this time is exceeded without a connection becoming available, a SQLException will be thrown from DataSource.getConnection().")
    private long connectionTimeOut = 30;

    @Min(0)
    @Option
    @Documentation("Sets the maximum number of seconds that the pool will wait for a connection to be validated as alive.")
    private long connectionValidationTimeOut = 10;

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public ExampleSQLDatastore setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
        return this;
    }

    public String getUser() {
        return user;
    }

    public ExampleSQLDatastore setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ExampleSQLDatastore setPassword(String password) {
        this.password = password;
        return this;
    }


}