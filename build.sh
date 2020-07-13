mvn clean install
#mvn talend-component:deploy-in-studio -Dtalend.component.studioHome="/Applications/TOSDI-7.2.1/studio"
mvn talend-component:deploy-in-studio -Dtalend.component.enforceDeploy=true -Dtalend.component.studioHome="/Applications/TalendStudio-7.2.1/studio"

