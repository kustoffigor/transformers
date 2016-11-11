
mvn org.apache.maven.plugins:maven-install-plugin:2.3.1:install-file -Dfile=target/transformers-1.0-SNAPSHOT.jar -DgroupId=cust-transformer -DartifactId=transformers -Dversion=1.0-SNAPSHOT -Dpackaging=jar 


../spark/bin/spark-submit --class org.sparkexample.DataPipeline --master local target/bootstrap-1.0-SNAPSHOT.jar --json-metadata ./src/main/resources/mlAttributes.json

mvn install:install-file -Dfile=target/jpmml-sparkml-1.99.jar -DgroupId=org.jpmml -DartifactId=jpmml-sparkml -Dversion=1.99 -Dpackaging=jar



