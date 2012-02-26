ifeq ($(origin JAVA_HOME), undefined)
  JAVA_HOME=/usr
endif

ifeq ($(origin NETLOGO), undefined)
  NETLOGO=/media/data/Uni/DA/netlogo-5.0
endif

SRCS=$(wildcard src/org/nlogo/extensions/mapred/*.java)

# why redirect like this? see readme
JAVAC = $(JAVA_HOME)/bin/javac

mapred/mapred.jar: $(SRCS) manifest.txt
	mkdir -p classes
	$(JAVAC) -g -encoding us-ascii -source 1.5 -target 1.5 -classpath $(NETLOGO)/NetLogoLite.jar:/usr/share/java/log4j-1.2.jar -d classes $(SRCS)
	jar cmf manifest.txt mapred/mapred.jar -C classes .


