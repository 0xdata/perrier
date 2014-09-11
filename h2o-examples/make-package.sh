#!/usr/bin/env bash

rm target/*.jar
(
  cd ..
  #mvn package -pl h2o-examples -DXX:MaxPermSize=128m -DskipTests -Dclean.skip -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dscalastyle.skip=true -Dmaven.scaladoc.skip=true -Dskip=true
  mvn package -pl h2o-examples -DXX:MaxPermSize=128m -DskipTests -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dscalastyle.skip=true -Dmaven.scaladoc.skip=true -Dskip=true
)
