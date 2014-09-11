#!/usr/bin/env bash

mvn install -DskipTests -Dclean.skip -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -Dscalastyle.skip=true -Dmaven.scaladoc.skip=true -Dskip=true -pl $@
