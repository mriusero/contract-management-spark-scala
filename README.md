# Contract Management Application (Spark/Scala/SBT)

## Overview

This repository contains a Scala-based application developed using Apache Spark for processing and managing contract data. The application supports reading and transforming contract files in multiple formats, including JSON, CSV, Parquet, ORC, and XML. It is designed to perform data extraction, transformation, and analysis while providing key operations, such as calculating total contract costs and determining contract statuses.

> **Note**: This is a **forked learning project** and serves as an educational exercise. It was created by forking an existing project, and the code is owned by the original repository owner, not under the MIT license.

## Features

- **Multi-Format File Reading**: The application can read input files in the following formats:
  - JSON
  - CSV
  - Parquet
  - ORC
  - XML

- **DataFrame Operations**:
  - Implements a class `JsonReader` with functionality similar to `CsvReader`, but tailored for reading JSON files.
  - The `JsonReader` class inherits from the `Reader` trait and reads JSON files based on configuration from `reader_json.json`.
  
- **Total Cost Calculation**:
  - Implements a method `calculTTC()` that calculates the total cost (TTC) using the formula:
    \[
    TTC = HTT + TVA \times HTT
    \]
    The result is rounded to two decimal places.
  - The method also removes the `TVA` and `HTT` columns from the DataFrame.

- **Date and City Extraction**:
  - Creates new columns `Date_End_contrat` and `Ville` by using the `select`, `from_json`, and `regexp_extract` methods to extract the date in the `YYYY-MM-DD` format.

- **Contract Status Determination**:
  - Adds a column `Contrat_Status` with values `"Expired"` if the contract has ended or `"Actif"` if it is still active.

## Technologies Used

- **Development Environment**: IntelliJ IDEA
- **Build Tool**: SBT (Simple Build Tool)
- **Programming Language**: Scala 2.12.15
- **Java Version**: Java 1.8
- **Framework**: Apache Spark

## Detailed Class Implementation

### `JsonReader` Class

- **Inheritance**: Implements the `Reader` trait.
- **Functionality**: Contains a `read` method that reads JSON files based on the schema and attributes provided in the configuration file `reader_json.json`.
- **Return Type**: Outputs a `DataFrame` containing the processed data.

### `calculTTC()` Method

- **Description**: Calculates the total cost (`TTC`) as `HTT + TVA * HTT`, rounding the result to two decimal places.
- **Modifies**: Removes `TVA` and `HTT` columns from the resulting DataFrame.

### Date and City Extraction

- **New Columns**:
  - `Date_End_contrat`: Extracted using `from_json` and `regexp_extract` functions to parse the date.
  - `Ville`: Extracted using similar string parsing methods.

### Contract Status

- **Logic**: Adds a column `Contrat_Status` with the value `"Expired"` if the `Date_End_contrat` is in the past; otherwise, the value is `"Actif"`.

## License

> **Note**: The code in this repository is not licensed under the MIT License. It is owned by the original project owner and subject to their license terms.


=======

## Overview

This repository contains a Scala-based application developed using Apache Spark for processing and managing contract data. The application supports reading and transforming contract files in multiple formats, including JSON, CSV, Parquet, ORC, and XML. It is designed to perform data extraction, transformation, and analysis while providing key operations, such as calculating total contract costs and determining contract statuses.

> **Note**: This is a **forked learning project** and serves as an educational exercise. It was created by forking an existing project, and the code is owned by the original repository owner, not under the MIT license.

## Features

- **Multi-Format File Reading**: The application can read input files in the following formats:
  - JSON
  - CSV
  - Parquet
  - ORC
  - XML

- **DataFrame Operations**:
  - Implements a class `JsonReader` with functionality similar to `CsvReader`, but tailored for reading JSON files.
  - The `JsonReader` class inherits from the `Reader` trait and reads JSON files based on configuration from `reader_json.json`.
  
- **Total Cost Calculation**:
  - Implements a method `calculTTC()` that calculates the total cost (TTC) using the formula:
    \[
    TTC = HTT + TVA \times HTT
    \]
    The result is rounded to two decimal places.
  - The method also removes the `TVA` and `HTT` columns from the DataFrame.

- **Date and City Extraction**:
  - Creates new columns `Date_End_contrat` and `Ville` by using the `select`, `from_json`, and `regexp_extract` methods to extract the date in the `YYYY-MM-DD` format.

- **Contract Status Determination**:
  - Adds a column `Contrat_Status` with values `"Expired"` if the contract has ended or `"Actif"` if it is still active.

## Technologies Used

- **Development Environment**: IntelliJ IDEA
- **Build Tool**: SBT (Simple Build Tool)
- **Programming Language**: Scala 2.12.15
- **Java Version**: Java 1.8
- **Framework**: Apache Spark

## Detailed Class Implementation

### `JsonReader` Class

- **Inheritance**: Implements the `Reader` trait.
- **Functionality**: Contains a `read` method that reads JSON files based on the schema and attributes provided in the configuration file `reader_json.json`.
- **Return Type**: Outputs a `DataFrame` containing the processed data.

### `calculTTC()` Method

- **Description**: Calculates the total cost (`TTC`) as `HTT + TVA * HTT`, rounding the result to two decimal places.
- **Modifies**: Removes `TVA` and `HTT` columns from the resulting DataFrame.

### Date and City Extraction

- **New Columns**:
  - `Date_End_contrat`: Extracted using `from_json` and `regexp_extract` functions to parse the date.
  - `Ville`: Extracted using similar string parsing methods.

### Contract Status

- **Logic**: Adds a column `Contrat_Status` with the value `"Expired"` if the `Date_End_contrat` is in the past; otherwise, the value is `"Actif"`.

## License

> **Note**: The code in this repository is not licensed under the MIT License. It is owned by the original project owner and subject to their license terms.
