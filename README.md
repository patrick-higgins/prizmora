Prizmora
========

A mod_plsql replacement inspired by DBPrism.

Install
-------

You will need to put the Oracle JDBC driver into your local Maven repo. You
have to download a copy from Oracle yourself. After you have downloaded it,
run this command to put it into your repo:

    mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc6 -Dversion=11.2.0.3.0 \
      -Dpackaging=jar -Dfile=/your/path/to/ojdbc6.jar -DgeneratePom=true

After that, use `mvn package` to generate a runnable jar.

Configuration
-------------

Configuration is done with a Java properties file. An example file with
comments can be found in the conf subdirectory.

Running
-------

Build the jar file by running `mvn package`. Then run it with:

    java -Dlog4j.configurationFile=log4j2.xml -jar prizmora-0.1.jar prizmora.properties

Replace `prizmora.properties` and `log4j2.xml` with your own config files.
