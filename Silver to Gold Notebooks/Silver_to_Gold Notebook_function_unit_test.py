#!/usr/bin/env python
# coding: utf-8

# ## Silver_to_Gold Notebook_function_unit_test
# 
# New notebook

# ### Silver_to_Gold Function and Testing

# Import library for logging, error handling

# In[1]:


import logging
from pyspark.sql.utils import AnalysisException
from pyspark.sql import functions as F
import pandas as pd

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s") ## python logger
logger = logging.getLogger("silver_to_gold_pipeline.log") ##pyspark logger


# ---
# #### Validation Function

# The function comprises of 3 checks:
# - table existence  - if table is successfully created
# - unique column - check if the suggested column fit the uniqueness criteria to be a primary key
# - null existence in the column - check if the suggested column fit the non-null criteria to be a primary key

# In[2]:


def pri_key_validation(table_name, key_name):
    logger.info(f"Prepare to validate {table_name}...")

    check_unique = f"""
            SELECT {key_name}, count(*)
            FROM {table_name}
            GROUP BY {key_name}
            HAVING COUNT(*) > 1 """

    null_check = f"""
        SELECT {key_name} 
        FROM {table_name}
        WHERE {key_name} IS NULL"""

    try:
        ## Validate table existence
        table_exists = spark.catalog.tableExists(table_name)
        if table_exists:
            logger.info(f"1/3 VALIDATION SUCCESS: {table_name} is created successfully.")
        else:
            logger.warning(f"1/3 VALIDATION FAIL: {table_name} is not found. Unable to proceed", exc_info=True)
            raise RuntimeError (f"{table_name} is not found.")
        logger.info(f"1/3 - Existence Check completed")

        ## Validate uniqueness
        unique_result= spark.sql(check_unique)

        if unique_result.count()== 0:
            logger.info(f"2/3 VALIDATION SUCCESS: {key_name} is unique")
        else:
            logger.warning(f"2/3 VALIDATION FAIL: {key_name} is not unique - {unique_result.count()} found")
            raise ValueError (f"{key_name} is not unique - {unique_result.count()} is/are found")
        logger.info("2/3 - Unique check completed.")

        ## Validate null
        null_check_result = spark.sql(null_check)
        if null_check_result.count() == 0:
            logger.info(f"3/3 VALIDATION SUCCESS: No nulls in {key_name}")
        else:
            logger.warning(f"3/3 VALIDATION FAILS: Null exists in {key_name}. May affect down stream")
            raise ValueError(f"Null exists in {key_name}. May affect down stream")
        logger.info("3/3 - Null check completed.")

    except AnalysisException as e:
        logger.error(f"ANALYSIS EXCEPTION ERROR: \n\n Details:{e} ") 
        raise ## reraise it
    except Exception as e:
        logger.error(f"UNEXPECTED ERROR:\n\n {e}.", exc_info=True) 
        raise ## reraise it
    
    logger.info("Full validation completed.")


# ---
# #### 1. Integration testing

# The function to be tested has a fail-fast design, hence using a try-except method to check if it is working.

# In[4]:


def test_table_existence_xfail():
    #prepare test data
    logger.info("[Integration Test - xfail] Preparing non-existence table...")
    table_name = "nhl_silver_lakehouse.dbo.non_existence_table"
    key_name = "reward"
    #administer test
    try:
        existence_test_result = pri_key_validation(table_name, key_name)
        logger.info("[Integration Test - xfail] Function executed") 
        logger.error("[Integration Test - xfail] FAILED. Ran with non-existence table without any errors") #Only will run if provided non-existence table really exists  
        raise AssertionError("[Integration Test - xfail] FAILED. Expected Runtime Error but code able to execute without issue")
    except RuntimeError as e:
        logger.info("[Integration Test - xfail] Test ended. PASSED. Expected error is raised.")

test_table_existence_xfail()


# #### 2. Unit Testing - Unique check

# In[4]:


def test_check_unique_xfail():
    #prepare test data
    logger.info("[Unit Testing - xfail] Preparing columns with duplicates to test unique_check..")
    column_not_unique = [
        {"Name": "Alpha", "Reward": "cup", "Address": "Joo Koon"}, 
        {"Name":"Beta", "Reward": "spoon", "Address": "Toa Payoh"}, 
        {"Name": "Beta", "Reward":"fork", "Address": "Tampines"}] 

    test_df= spark.createDataFrame(pd.DataFrame(column_not_unique))
    test_df.createOrReplaceTempView("temp_table")
    table_name= "temp_table"
    key_name = "Name"
    #administer test
    try:
        pri_key_validation(table_name, key_name)
        logger.info("[Unit Testing - xfail] Function executed")
    except ValueError as e:
        logger.info("[Unit Testing - xfail] PASSED. Expected error is raised.")
    except Exception as e:
        logger.error("[Unit Testing - xfail] FAILED: UNEXPECTED ERROR")
    finally:
        spark.catalog.dropTempView(table_name)
        logger.info("[Unit Testing - xfail] Test ended. Temp table cleared")
test_check_unique_xfail()


# In[5]:


def test_check_unique_xSuccess():
   #prepare test data
    logger.info("[Unit Testing - xSuccess] Preparing columns with duplicates to test unique_check..")
    column_unique = [
        {"Name": "Alpha", "Reward": "cup", "Address": "Joo Koon"}, 
        {"Name":"Beta", "Reward": "spoon", "Address": "Toa Payoh"}, 
        {"Name": "Beta", "Reward":"fork", "Address": "Tampines"}] 

    test_df= spark.createDataFrame(pd.DataFrame(column_unique))
    test_df.createOrReplaceTempView("temp_table")
    table_name= "temp_table"
    key_name = "Reward"
    #administer test
    try:
        pri_key_validation(table_name, key_name)
        logger.info("[Unit Testing - xSuccess] Function executed")
    except Exception as e:
        logger.info(f"[Unit Testing - xSuccess] UNEXPECTED ERROR {e}.")
    
    finally: #clean up test data
        spark.catalog.dropTempView(table_name)
        logger.info("[Unit Testing - xSuccess] Temp Table dropped. Cleaned up")

test_check_unique_xSuccess()


# #### 3. Unit Testing - Null Check

# In[6]:


def test_null_check_xfail():
    #prepare test data
    logger.info("[Unit Testing - xfail] Preparing table with nulls..")
    data = [
        {"Name": "Alpha", "Reward": 5, "Address": "Joo Koon"}, 
        {"Name":"Beta", "Reward": None , "Address": "Toa Payoh"}, 
        {"Name": "Beta", "Reward":2, "Address": "Tampines"}]
    
    null_data = spark.createDataFrame(pd.DataFrame(data))
    null_data.createOrReplaceTempView("table_with_null")
    table_name = "table_with_null"
    key_name = "Reward"
    #administer test
    try:    
        pri_key_validation(table_name, key_name)
        logger.info("[Unit Testing - xfail] Function executed")
    except ValueError as e:
        logger.info("[Unit Testing - xfail] PASSED. Expected error is raised")
    except Exception as e:
        logger.error(f"[Unit Testing - xfail] FAILED. Unexpected error {e}")
    finally: #clean up test data
        spark.catalog.dropTempView(table_name)
        logger.info("[Unit Testing - xfail] Temp Table dropped. Cleaned up")

test_null_check_xfail()


# In[7]:


def test_null_check_xsuccess():
    #prepare test data
    logger.info("[Unit Testing - xsuccess] Preparing table with nulls..")
    data = [
        {"Name": "Alpha", "Reward": 5, "Address": "Joo Koon"}, 
        {"Name":"Beta", "Reward": 3 , "Address": "Toa Payoh"}, 
        {"Name": "Beta", "Reward":2, "Address": "Tampines"}]
    
    null_data = spark.createDataFrame(pd.DataFrame(data))
    null_data.createOrReplaceTempView("table_with_null")
    table_name = "table_with_null"
    key_name = "Reward"
    # administer test
    try:    
        pri_key_validation(table_name, key_name)
        logger.info("[Unit Testing - xsuccess] Function executed")
    except ValueError as e:
        logger.info("[Unit Testing - xsuccess] PASSED. Expected error is raised")
    except Exception as e:
        logger.error(f"[Unit Testing - xsuccess] FAILED. Unexpected error {e}")
    finally: #clean up test data
        spark.catalog.dropTempView(table_name)
        logger.info("[Unit Testing - xsuccess] Temp Table dropped. Cleaned up")

test_null_check_xsuccess()

