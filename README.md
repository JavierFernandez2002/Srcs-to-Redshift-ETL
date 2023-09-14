# ETL (Extract, Transform, Load) Data Pipeline

This project is an ETL (Extract, Transform, Load) data pipeline that extracts data from various sources, including a PostgreSQL database, a JSON file, and a CSV file. It then performs data transformations to optimize it for data analysis before loading it into an Amazon Redshift database on AWS.

## Table of Contents

- [Overview](#overview)
- [Technologies Used](#technologies-used)
- [Project Structure](#project-structure)
- [Usage](#usage)
- [Getting Started](#getting-started)
- [Contributing](#contributing)
- [License](#license)

## Overview

In this project, we build a data pipeline to consolidate data from multiple sources into Amazon Redshift for efficient data analysis. The pipeline consists of the following steps:

1. **Data Extraction**: Data is extracted from the following sources:
   - PostgreSQL Database
   - JSON File
   - CSV File

2. **Data Transformation**: Extracted data undergoes various transformations to ensure consistency, data quality, and suitability for analysis.

3. **Data Loading**: Transformed data is loaded into an Amazon Redshift database for analysis.

## Technologies Used

- PostgreSQL
- Amazon Redshift
- Python
- Pandas
- AWS (Amazon Web Services)

## Project Structure

- `/src`: Contains the Python scripts for data extraction, transformation, and loading.
- `/data`: Store sample data files (JSON, CSV).
- `/config`: Configuration files for connecting to databases and AWS.

## Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/JavierFernandez2002/Srcs-to-Redshift-ETL.git
   ```
1.Install the required dependencies:
   ```bash
    pip install -r requirements.txt
   ```
2.Update the configuration files in the /config directory with your database and AWS credentials.
3.Run the ETL pipeline:
  ```bash
    python ETL-Srcs-to-Redshift.py
   ```
## Getting Started
Follow these steps to get started with the project:

1.Prerequisites
    .Ensure you have Python installed.
    .Set up an Amazon Redshift cluster and configure the necessary AWS credentials.
2.Clone the repository and install dependencies as mentioned in the "Usage" section.

3.Customize the ETL scripts in the /src directory to fit your specific data and transformation requirements.

4.Execute the ETL pipeline as described in the "Usage" section.

## Contributing
Contributions are welcome! Please feel free to submit issues or pull requests to improve this project.

## License
This project is licensed under the MIT License.
