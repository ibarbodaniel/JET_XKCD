# Data Warehouse Model Documentation

## Introduction

This document describes the Data Warehouse (DWH) Model for the XKCD Comics project. The model is designed using the Kimball Dimensional Modeling approach, which involves creating dimension (dim) and fact tables.

## ER Diagram

The ER Diagram visually represents the relationships between the `dim_comic` and `fact_comic` tables.

![ER Diagram](images/ER_DIAGRAM.png)

## Tables

### Dimension Table: `dim_comic`
   Column Name | Data Type   | Description                       |
 |-------------|-------------|-----------------------------------|
 | `comic_id`  | INT         | Primary Key, unique identifier    |
 | `title`     | TEXT        | Title of the comic                |
 | `safe_title`| TEXT        | Safe title of the comic           |
 | `transcript`| TEXT        | Transcript of the comic           |
 | `alt`       | TEXT        | Alt text of the comic             |
 | `image_url` | TEXT        | URL of the comic image            |
 | `date`      | DATE        | Publication date of the comic     |
 | `news`      | TEXT        | News related to the comic         |

### Fact Table: `fact_comic`
 | Column Name       | Data Type  | Description                       |
 |-------------------|------------|-----------------------------------|
 | `comic_id`        | INT        | Primary Key, foreign key to `dim_comic` |
 | `views`           | INT        | Number of views                   |
 | `cost`            | DECIMAL    | Cost to create the comic          |
 | `customer_reviews`| DECIMAL    | Customer reviews rating           |
