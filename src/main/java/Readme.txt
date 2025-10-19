javac -cp .:libs/jdiameter.jar:libs/mysql-connector-java.jar CcTestClientWithDb.java
java -cp .:libs/jdiameter.jar:libs/mysql-connector-java.jar CcTestClientWithDb config.properties
java -cp .:examples/cc -Djava.util.logging.config.file=log.properties cc_test_client foo.boo.goo boo.org isjsys.int.i1.dk 3868
