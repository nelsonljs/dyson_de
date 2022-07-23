# dyson_de

## Question

Develop a solution that seeks to join related data together into document-based data structure, saving this to a suitable format (e.g. JSON, AVRO, Parquet) and load into a database of your choice (SQL/NoSQL/SparkSQL/Presto) hosted on Docker.  You may use any programming language of choice for ETL. You will be required to discuss and answer questions about your solution, approach and challenges faced.

## Solution

### Data exploration

Run some basic data exploration to decide how we want to model our information. 

Some key properties for filtering might be:
- Publisher
- Year released
- First appearance

Expectation to store as document store.

### Data types

The received data contains two main sets of distinct data sources.

Detailed Set:
- characters_stats.csv
- marvel_characters_info.csv
- superheroes_power_matrix.csv

Larger General set:
- marvel_dc_characters.csv

Since they both contain information about superheroes, it might be possible to merge sources together into a single collection for reference. 

Comparison within the dataset:

| Detailed Set | General Set | Attribute |
|---|---|---|
| Name | Name | Name |
| Alignment | Alignment | Alignment |
| Gender | Gender | Gender |
| EyeColor | EyeColor | EyeColor |
| Race | | Race |
| HairColor | HairColor | HairColor
| Publisher | Universe | Publisher |
| SkinColor | | SkinColor |
| Height |  | Height |
| Weight |  | Weight |
| Combat Stats |  | Combat Stats |
| Power Matrix |  | Power Matrix |
|  | Identity | Identity Status |
|  | Status | Living Status |
|  | Appearances | Apperances |
|  | FirstAppearance | FirstAppearance (Comic/Year) |
|  | World | World |
|  |  | DataSet |

The above will be combined into a single noSQL collection in mongoDB for reference.

## Data Ingestion

Assuming that **incremental** data will be received as csv files, we can run a daily ingestion script to add the data into our mongodb.

The scripts will be run separately for the 2 distinct types of data received.

For the detailed data set, ingest it as a set with a given ingestion_id for tracking.