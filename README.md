
# Metric Aggregation Job

This project is a Spark job written in Java that aggregates metrics from a CSV file.Input dataset is stored in the `src/main/resources/input` directory. 
The job reads the input dataset, aggregates the metrics, and writes the output to the `src/main/resources/output` directory.

## Prerequisites

- Java 17 or higher
- Apache Maven 3.8.2 or higher

## Setup

1. Clone the repository:

```bash
git clone https://github.com/jegrajas/spark-java-challenge.git
```

2. Navigate to the project directory:

```bash
cd spark-java-challenge
```

3. Build the project:

```bash
mvn clean install
```

4. Run the project 24 hour time bucket by default:

```bash
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -jar target/spark-batch-aggregation-job-0.0.1.jar
```
default is 24 hour is the time bucket for the aggregation. no need to pass any argument.

5 .Run the project with different time bucket:    

 ```bash
java --add-exports java.base/sun.nio.ch=ALL-UNNAMED -jar target/spark-batch-aggregation-job-0.0.1.jar "30 minutes"
```
time bucket can be 15 minutes, 30 minutes,1 hour,4 hour,8 hour, 24 hour.

5 .The Spark UI can be accessible through this link.

[Spark UI link](http://localhost:4040/)

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

