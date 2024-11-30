
# Hadoop MapReduce: Object Storage and Big Data Analysis

## Overview
This repository contains the solutions for a lab, focused on object storage optimization and Big Data analysis using MapReduce. The implementation is written in Java.
1. Analyzing anonymized web server logs to detect patterns leading to purchases.
----
## Big Data Analysis with MapReduce

### Problem
Analyze anonymized server logs to identify user behavior patterns. Specifically, detect sequences of page visits where a purchase occurs on the third visit.

### Input Format
- **Log File (CSV)**:
  - **Timestamp (TS)**: `YYYYMMDDhhmmss.milli`
  - **SessionID (SID)**
  - **ProductPageID (PPID)`
  - **PurchaseIndicator (PI)**: `0` (no purchase) or `1` (purchase)

### Output Format
A sorted list of page pairs leading to purchases, along with their occurrence counts.

### Implementation Steps
1. **Step 1**: Aggregate events by `SessionID` and sort by `Timestamp`.
2. **Step 2**: Generate triples of product page visits for each session.
3. **Step 3**: Count occurrences of page pairs leading to purchases.
4. **Step 4**: Sort results by count in descending order, breaking ties lexicographically.

### Files
- `MapReduceStep1.java`: Aggregation and sorting by `SessionID`.
- `MapReduceStep2.java`: Generate product visit triples.
- `MapReduceStep3.java`: Count purchase sequences.
- `MapReduceStep4.java`: Sort results by count.

### Usage
```bash
# Compile Java files
javac -cp /path/to/hadoop-core.jar:. MapReduceStep1.java MapReduceStep2.java MapReduceStep3.java MapReduceStep4.java

# Execute MapReduce steps
hadoop jar /path/to/hadoop-core.jar MapReduceStep1 /input/path /output/path1
hadoop jar /path/to/hadoop-core.jar MapReduceStep2 /output/path1 /output/path2
hadoop jar /path/to/hadoop-core.jar MapReduceStep3 /output/path2 /output/path3
hadoop jar /path/to/hadoop-core.jar MapReduceStep4 /output/path3 /output/final_output
```

---

## Repository Structure
```plaintext
.
├── javaFiles/
│   ├── MapStep1.java
│   ├── MapStep2.java
│   ├── MapStep3.java
│   ├── MapStep4.java
│   ├── ReduceStep1.java
│   ├── ReduceStep2.java
│   ├── ReduceStep3.java
│   ├── ReduceStep4.java
│   ├── Main.java
└── README.md

```

---

## Notes
1. Ensure the Java code adheres to Hadoop's MapReduce framework requirements.
2. Validate outputs against the provided sample input and other test datasets.

---

## Submission Instructions
1. Place your solutions in the respective `part1/` and `part2/` directories.
2. Ensure all scripts and files follow the specified format.
3. Push the repository to GitHub for submission.

---

## Author
Developed as part of the course's Big Data and Distributed Systems assignment.
