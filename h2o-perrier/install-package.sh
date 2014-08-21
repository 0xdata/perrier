#!/bin/bash
mvn install -DXX:MaxPermSize=128m -DskipTests -Dclean.skip -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dscalastyle.skip=true -Dmaven.scaladoc.skip=true -Dskip=true

