build: 
	$(MAKE) -C com/ostor/dedup/core build
	$(MAKE) -C com/ostor/dedup/hadoop build
clean: 
	$(MAKE) -C com/ostor/dedup/core clean
	$(MAKE) -C com/ostor/dedup/hadoop clean
	rm *.jar

install:
	$(MAKE) -C com/ostor/dedup/core build
	$(MAKE) -C com/ostor/dedup/hadoop build
	jar -cf ostor.jar com/ostor/dedup/core/*.class com/ostor/dedup/hadoop/*.class



