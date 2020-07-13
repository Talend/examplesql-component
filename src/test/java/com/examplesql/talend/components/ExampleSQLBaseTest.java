package com.examplesql.talend.components;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.examplesql.talend.components.datastore.ExampleSQLDatastore;
import com.examplesql.talend.components.service.I18nMessage;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.ProgressHandler;
import com.spotify.docker.client.DockerClient.ListContainersParam;
import com.spotify.docker.client.exceptions.DockerCertificateException;
import com.spotify.docker.client.exceptions.DockerException;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.Image;
import com.spotify.docker.client.messages.PortBinding;
import com.spotify.docker.client.messages.ProgressMessage;

import org.apache.ibatis.jdbc.ScriptRunner;
import org.talend.sdk.component.junit5.WithMavenServers;
//import org.talend.sdk.component.maven.Server;

import lombok.extern.slf4j.Slf4j;


@Slf4j
@WithMavenServers
public class ExampleSQLBaseTest {
    private  static DockerClient dockerClient = null;
    private  static ContainerInfo info = null;
    private  static String id = null;
    private static String label = null;

    protected Properties ExampleSQLProps() {
        try (InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("ExampleSQLConfig.properties")) {
            Properties ExampleSQLprops = new Properties();
            ExampleSQLprops.load(in);
            return ExampleSQLprops;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    protected boolean initializeExampleSQL(Properties props)
    {
        try {
            InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("ExampleSQL_config.sql");
            InputStreamReader reader = new InputStreamReader(in);
            
            
            Class.forName("org.mariadb.jdbc.Driver");
            Connection connect = null;
            short retry = 0;
            do {
                try {
                    connect = DriverManager.getConnection(props.getProperty("ExampleSQL.url.init"),props.getProperty("ExampleSQL.user"), props.getProperty("ExampleSQL.pwd"));
                    System.out.println("Connection to ExampleSQL Docker Successful");
                } catch(SQLException e)
                {
                    System.out.println("Unable to connect to ExampleSQL Docker. Retry in 2 seconds");
                    try {
                        Thread.sleep(2000);
                    } catch(InterruptedException ie)
                    {

                    }
                }
              }  while(connect == null && retry < 5);
            if (retry == 5 && connect == null) {
              System.err.println("Unable to connect to ExampleSQL Docker Instance");
              return false;
            }
            ScriptRunner sr = new ScriptRunner(connect);
            sr.runScript(reader);
            connect.commit();
            connect.close();
            
        } catch (ClassNotFoundException | SQLException ex) {
            System.err.println(ex.getMessage());
            return false;
        }
        return true;
    }
    protected void shutdownDockerConfig() throws DockerException, InterruptedException, DockerCertificateException
    {
        Properties props = ExampleSQLProps();
        dockerClient = DefaultDockerClient.fromEnv().build();
        label = props.getProperty("docker.name")+":"+props.getProperty("docker.tag");
        ListContainersParam params = new ListContainersParam("image", label);
        List<Container> containers = dockerClient.listContainers(params);
        id = containers.get(0).id();
            info = dockerClient.inspectContainer(id);
            if (info.state().running()) {
                System.out.println("Attempting to stop container " + label);
                dockerClient.stopContainer(id, 10);
                System.out.println("Container " + label + " stopped.");
            }
                //dockerClient.killContainer(id);
            System.out.println("Removing Container " + label);
            dockerClient.removeContainer(id);

            System.out.println("Docker Client Shutting Down");
            dockerClient.close();
    }
    
    protected boolean ExampleSQLDockerConfig(Properties props)
    {
        try {
            dockerClient = DefaultDockerClient.fromEnv().build();
            if (!dockerClient.ping().equalsIgnoreCase("OK")) throw new DockerException("Start Docker!");
            label = props.getProperty("docker.name")+":"+props.getProperty("docker.tag");
            List<Image> images = getImages(label);


            if (images.size() == 0) {
                dockerClient.pull(label, new ProgressHandler(){
                    @Override
                    public void progress(ProgressMessage progressMessage) throws DockerException {
                        System.out.println("Status :" + progressMessage.status() +
                                " Message :" + progressMessage.progress());
                    }
                });
            }
            final Map<String, List<PortBinding>> portBindings = new HashMap<>();
            List<PortBinding> hostPorts3306 = new ArrayList<>();
            hostPorts3306.add(PortBinding.of("0.0.0.0", props.getProperty("ExampleSQL.jdbc.port")));
            List<PortBinding> hostPorts8080 = new ArrayList<>();
            hostPorts8080.add(PortBinding.of("0.0.0.0", props.getProperty("ExampleSQL.admin.port")));
            portBindings.put(props.getProperty("ExampleSQL.jdbc.port"), hostPorts3306);
            portBindings.put(props.getProperty("ExampleSQL.admin.port"), hostPorts8080);
            HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
            ContainerConfig containerConfig = ContainerConfig.builder()
                    .hostConfig(hostConfig)
                    .env("LICENSE_KEY=" + props.getProperty("ExampleSQL.lic"))
                    .image(label).exposedPorts("3306","8080")
                    .openStdin(true).attachStdin(true).build();

           
            final ContainerCreation creation = dockerClient.createContainer(containerConfig);
            id = creation.id();
            System.out.println(containerConfig.toString());
            System.out.println(id);
            info = dockerClient.inspectContainer(id);
            dockerClient.startContainer(id);


            if (!info.state().running()) {
                System.out.println("Docker container " + label + " done init. Starting....");
                dockerClient.restartContainer(id);
                
            }
            System.out.println("Initializing ExampleSQL for Unit Tests");
            System.out.println("Initializing Completed Successfully");
        } catch(InterruptedException | DockerException | DockerCertificateException e)
        {
            System.err.println(e.getMessage());
            return false;
        } 
        return true;
    }

    private List<Image> getImages(String name) throws InterruptedException, DockerException
    {
        return dockerClient.listImages(DockerClient.ListImagesParam.byName(name));
    }

    protected ExampleSQLDatastore buildDataStore(Properties props) {
        
        ExampleSQLDatastore ExampleSQLds = new ExampleSQLDatastore();
        ExampleSQLds.setJdbcUrl(props.getProperty("ExampleSQL.url.test"));
        ExampleSQLds.setUser(props.getProperty("ExampleSQL.user"));
        ExampleSQLds.setPassword(props.getProperty("ExampleSQL.pwd"));
        return ExampleSQLds;
    }

    protected I18nMessage getI18n() {
        I18nMessage i18n = new I18nMessage(){
        
            @Override
            public String warnStatementCantBeClosed() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String warnResultSetCantBeClosed() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String warnReadOnlyOptimisationFailure() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String warnDriverClose(String type) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String warnConnectionCantBeClosed() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String successConnection() {
                // TODO Auto-generated method stub
                return "OK";
            }
        
            @Override
            public String errorVacantAccountKey() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorUnsupportedType(String type, String field) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorUnsupportedSubProtocol() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorUnsupportedDatabaseAction() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorUnsupportedDatabase(String dbType) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorUnauthorizedQuery() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorTaberDoesNotExists(String tableName) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorSingleSortKeyInvalid() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorSQL(int errorCode, String message) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorNoUpdatableColumnWasDefined() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorNoRecordReceived() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorNoKeyForUpdateQuery() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorNoKeyForDeleteQuery() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorNoFieldForQueryParam(String field) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorInvalidConnection() {
                // TODO Auto-generated method stub
                return "KO";
            }
        
            @Override
            public String errorEmptyQuery() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorEmptyJdbcURL() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorDriverNotFound(String dbType) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorDriverLoad(String driverId, String missingJars) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorDriverInstantiation(String message) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorDriverDeregister(String type) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorCantLoadTableSuggestions() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorCantLoadDriver(String dbType) {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorCantClosePreparedStatement() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorCantCloseJdbcConnectionProperly() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String errorCantClearPreparedStatement() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String actionOnDataUpsert() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String actionOnDataUpdate() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String actionOnDataInsert() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String actionOnDataDelete() {
                // TODO Auto-generated method stub
                return null;
            }
        
            @Override
            public String actionOnDataBulkLoad() {
                // TODO Auto-generated method stub
                return null;
            }
        };

        return i18n;
    }
}