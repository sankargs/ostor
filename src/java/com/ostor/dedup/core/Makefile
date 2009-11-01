JAVA_CLASSPATH = -cp $(CLASSPATH):../../../../:../../../../../../jars/*

build: 
	javac $(JAVA_FLAGS) $(JAVA_CLASSPATH) *.java 

test:  

clean:  
	rm -f  *.class  *.exe *.jar


# run java class.    example: make testClass
% :  %.class
	java $(JAVA_CLASSPATH) $(JAVA_PACKAGE).$*  $(ARGUMENTS) 

# compile java class.     example: make testClass.class
%.class :  %.java
	javac $(JAVA_FLAGS) $(JAVA_CLASSPATH)  $<


