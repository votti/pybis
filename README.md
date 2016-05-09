# Summary

Python module for interacting with openBIS.

# Plan

We plan to implement the following commands:

- login
- get_sample_with_data
- get_data_set
- create_data_set
- logout

The methods get_sample_with_data and get_data_set will write files to the file system that can be read in using standard libraries. We hope to store the files as json, but we need to verify that they are easy to work with from python and R.
