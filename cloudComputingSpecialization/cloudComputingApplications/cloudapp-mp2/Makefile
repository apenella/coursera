JAVA=java
JAVAC=javac
JAR=jar
USERID=11239605

all: hadoop

hadoop: jar
	hadoop jar $(EXERCISE).jar $(EXERCISE) -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt $(INPUT) $(OUTPUT) 

jar: comp
	$(JAR) -cvf $(EXERCISE).jar -C $(EXERCISE)/ .
comp:
	mkdir -p $(EXERCISE)
	$(JAVAC) -d $(EXERCISE) -classpath ~/lib/commons-logging-1.2/commons-logging-1.2.jar:$(HDPLIB)/common/hadoop-common-2.7.1.jar:$(HDPLIB)/mapreduce/hadoop-mapreduce-client-core-2.7.1.jar:$(HDPLIB)/mapreduce/hadoop-mapreduce-client-jobclient-2.7.1.jar  $(EXERCISE).java
clean:
	rm -f $(EXERCISE)/*  $(EXERCISE).jar
	hadoop fs -rm -r $(OUTPUT)	
