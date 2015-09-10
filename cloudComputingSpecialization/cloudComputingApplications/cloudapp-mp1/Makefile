JAVA=java
JAVAC=javac
JAR=jar

all: jar
	$(JAVA) MP1 11239605 
jar:MP1
	$(JAR) cf MP1.jar MP1.class input.txt	
MP1:
	$(JAVAC) -d . MP1.java
clean:
	rm -f *.class *.jar
