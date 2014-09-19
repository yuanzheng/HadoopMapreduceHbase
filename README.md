. CPI and GDP Import###
====================

**Source codes are in:** /src/main/java/

1. **package:**com.isdc.neuralytics.infrastructure
2. **Java files:** 
	- EconomicDataImport.java
		- CPIImport.java
			- GDPImport.java
				- ImportData.java  (Main drive)

				3. **Unit Test codes are in:** /src/test/java/
					- package: com.isdc.neuralytics.infrastructure
						- test files: 
								- EconomicDataImportTest.java
										- CPIImportTest.java
												- GDPImportTest.java

												4. **log4j properties file is in:** /src/main/config
													- log4j.properties
														

														5. **Testing resources:** /src/test/resources/
															- CPI_2014-04-01.csv
																- GDP_2014-07-01.csv

																6. **Automated downloading script:** /src/main/scripts/
																	- dataImport.php
