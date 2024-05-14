CREATE OR REPLACE TABLE `august-bucksaw-422621-f3`.`dbt_ajithesh`.`ny_archival_cleaned`
OPTIONS()
AS (
  -- Change Schema of pub_date to DATE Type
  SELECT 
      *,
      DATE(pub_date) AS new_pub_date,
      NULL AS new_word_count,  -- Placeholder for the next column
      CAST(word_count AS INT64) AS new_word_count_int64,
      CAST(abstract AS STRING) AS new_abstract,
      CAST(lead_paragraph AS STRING) AS new_lead_paragraph,
      CAST(print_section AS STRING) AS new_print_section,
      CAST(print_page AS STRING) AS new_print_page,
      CAST(document_type AS STRING) AS new_document_type,
      CAST(news_desk AS STRING) AS new_news_desk,
      CAST(section_name AS STRING) AS new_section_name,
      CAST(type_of_material AS STRING) AS new_type_of_material
  FROM 
      `august-bucksaw-422621-f3.dv_demo.nytimes_archeive_data`
  
  UNION ALL
  
  -- Changing Schema of word_count to INT64
  SELECT 
      *,
      NULL AS new_pub_date,  -- Placeholder for the previous column
      CAST(word_count AS INT64) AS new_word_count,
      NULL AS new_word_count_int64,
      CAST(abstract AS STRING) AS new_abstract,
      CAST(lead_paragraph AS STRING) AS new_lead_paragraph,
      CAST(print_section AS STRING) AS new_print_section,
      CAST(print_page AS STRING) AS new_print_page,
      CAST(document_type AS STRING) AS new_document_type,
      CAST(news_desk AS STRING) AS new_news_desk,
      CAST(section_name AS STRING) AS new_section_name,
      CAST(type_of_material AS STRING) AS new_type_of_material
  FROM 
      `august-bucksaw-422621-f3.dv_demo.nytimes_archeive_data`
);


-- Define a new model to clean and transform the data
-- This model will apply various transformations to the raw data and output cleaned data

{{ config(
    materialized='table',
    alias='cleaned_data'
) }}

-- Apply transformations to clean the text columns
with cleaned_text as (
    select
        trim(lower(abstract)) as abstract,
        trim(lower(lead_paragraph)) as lead_paragraph,
        trim(lower(print_section)) as print_section,
        trim(lower(print_page)) as print_page,
        -- Add more text columns to clean as needed
    from 
        `august-bucksaw-422621-f3.dv_demo.nytimes_archeive_data`
),

-- Apply transformations to extract information from text columns
extracted_info as (
    select
        abstract,
        lead_paragraph,
        -- Example: Extract URLs from the abstract column using REGEXP_EXTRACT
        regexp_extract(abstract, r'(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)', 1) as extracted_url,
        -- Add more columns with extracted information as needed
    from
        cleaned_text
),

-- Apply transformations for date manipulation
transformed_dates as (
    select
        *,
        extract(dayofweek from pub_date) as day_of_week,
        extract(month from pub_date) as month,
        extract(year from pub_date) as year,
        -- Calculate age of articles based on pub_date
        date_diff(current_date(), pub_date, year) as article_age_years
    from
        `august-bucksaw-422621-f3.dv_demo.nytimes_archeive_data`
),

-- Apply transformations for categorical encoding
categorical_encoded as (
    select
        *,
        case
            when document_type = 'Article' then 1
            else 0
        end as document_type_article,
        -- Add more categorical encoding columns as needed
    from
        transformed_dates
),

-- Apply transformations for numeric aggregations
numeric_aggregations as (
    select
        *,
        avg(word_count) over () as avg_word_count,
        -- Add more numeric aggregations as needed
    from
        categorical_encoded
)

-- Output the final cleaned and transformed data
select
    *
from
    numeric_aggregations;

