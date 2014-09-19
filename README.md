###US CPI and GDP Import###
####Automaticate external data import for CPI and GDP into hbase for training models####
====================

Source codes are in: /src/main/java/
			package: com.isdc.neuralytics.infrastructure
		 Java files: EconomicDataImport.java
		 			 CPIImport.java
		 			 GDPImport.java
		 			 ImportData.java  Main drive
		 			 
Unit Test codes are in: /src/test/java/
			   package: com.isdc.neuralytics.infrastructure
		    test files: EconomicDataImportTest.java
		    			CPIImportTest.java
		    			GDPImportTest.java

log4j properties file is in: /src/main/config
						log4j.properties

		    			
Testing resources: /src/test/resources/
	CPI_2014-04-01.csv
	GDP_2014-07-01.csv

Automated downloading script: /src/main/scripts/
	dataImport.php
