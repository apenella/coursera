JAVA=javaDOOP_CLASSPATH
JAVAC=javac
JAR=jar
USERID=11239605
HADOOP=hadoop


all: hadoop

hadoop: jar
	$(HADOOP) jar $(EXERCISE).jar $(EXERCISE) -D N=5 -D league=/mp2/misc/league.txt -D stopwords=/mp2/misc/stopwords.txt -D delimiters=/mp2/misc/delimiters.txt $(INPUT) $(OUTPUT) 

jar: comp
	$(JAR) -cvf $(EXERCISE).jar -C $(EXERCISE)/ ./
comp: cp
	mkdir -p $(EXERCISE)
	$(HADOOP) com.sun.tools.javac.Main $(EXERCISE).java -d $(EXERCISE) 
cp:
	export HADOOP_CLASSPATH=$(JAVA_HOME)/lib/tools.jar
clean:
	rm -f $(EXERCISE)/*  $(EXERCISE).jar
	hadoop fs -rm -r $(OUTPUT)	
