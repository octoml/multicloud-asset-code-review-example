# Directions
For this exercise, please review the following methods in the `s3_utils.py` and `test_s3_utils.py` files:
`_get_byte_offsets_and_parts`
`_large_copy_object_to_aws`
`_upload_part_to_aws`

This code was intentionally designed for interview purposes -- so fear not, you won't offend anyone!

***Criteria:***
- These methods are part of a multicloud solution intended to decrease costs of transferring assets across regions.
- Please do not spend time independently validating or invalidating the math for any of the methods, as that’s beyond the intent of this exercise.
- Please review the methods and included tests. We are looking for technical communication that happens asynchronously, thinking through technical problems, and clarification where information is not complete.
- This code was intentionally modified for interview purposes -- so fear not, you won't offend anyone!

## Setup Directions
It's recommended you use a virtual environment such as venv or conda.
You can run `pip install poetry` or install the packages in the `pyproject.toml` file.
If using poetry, you can install all packages by running the following from the root directory:
`poetry install`

## Running tests
You can run tests using the following command from the root directory:
`pytest test_s3_utils.py`