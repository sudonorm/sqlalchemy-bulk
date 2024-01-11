import pandas as pd
import numpy as np
from packaging import version
from typing import List, Dict, Union, Any
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")


class HelperFunctions:
    """This class contains helper functions which help make interacting with the database easier"""

    def __init__(self):
        pass

    def get_or_return(
        self, val: Union[int, str], valDict: Dict, hasInt: bool = False
    ) -> Union[int, str]:
        """
        This function is used to try to get a value from a dictionary and if this cannot be found, it simply returns the value.

        Args:
            val (Union[int, str]): the value you are searching for in the dictionary. It can be a string or integer
            valDict (Dict): a dictionary containing key:value pairs
            hasInt (bool, optional): Should be set to True if the key of the dictionary is an integer. The value is converted to an int before checking the dictionary. Defaults to False.

        Returns:
            Union[int, str]: returns either a value from the valDict or the original value passed in
        """

        try:
            if hasInt:
                try:
                    return valDict[int(val)]
                except:
                    return val
            else:
                return valDict[val]
        except:
            return val

    def get_or_return_nan(
        self, val: Union[int, str], valDict: Dict, hasInt: bool = False
    ) -> Union[int, str]:
        """
        This function is used to try to get a value from a dictionary and if this cannot be found, it simply returns the value.

        Args:
            val (Union[int, str]): the value you are searching for in the dictionary. It can be a string or integer
            valDict (Dict): a dictionary containing key:value pairs
            hasInt (bool, optional): Should be set to True if the key of the dictionary is an integer. The value is converted to an int before checking the dictionary. Defaults to False.

        Returns:
            Union[int, str, nan]: returns either a value from the valDict or np.nan
        """

        try:
            if hasInt:
                try:
                    return valDict[int(val)]
                except:
                    return np.nan
            else:
                return valDict[val]
        except:
            return np.nan

    def slice_dictionary(
        self, input_dict: dict, start: int = "", stop: int = ""
    ) -> dict:
        """Slice a dictionary like it's a list based on a start and stop index

        Args:
            input_dict (dict): input dictionary to be sliced
            start (int): start index e.g. 0
            stop (int): stop index e.g. 1

        Notes:
            If start and stop are not passed, the full dictionary is returned

        Returns:
            Dict: sliced dictionary
        """
        if start and stop:
            return dict(list(input_dict.items())[start:stop])

        elif start and not stop:
            return dict(list(input_dict.items())[start:])

        elif not start and stop:
            return dict(list(input_dict.items())[:stop])

        else:
            return dict(list(input_dict.items())[:])

    def split_to_list_or_return(self, text: str, sep: str) -> List:
        """
        This function is used to split a text on some separator and return the split text as a list

        Args:
            text (str): text to be split e.g., "the_quick_brown_fox"
            sep (str): the separator to split it on e.g., "_"

        Returns:
            List: text split on the separator
        """

        if sep in text:
            try:
                return [x.strip() for x in text.split(sep)]
            except:
                return [text]
        else:
            return [text]

    def try_date_conversion_or_return(
        self, date_text: str
    ) -> Union[datetime.date, None]:
        """
        This function helps us to convert a date given as a string to a date object required by the database.

        Args:
            date_text (str): date given as string e.g., 2018-01-01

        Returns:
            Union[datetime.date, None]: date object or None
        """
        try:
            return datetime.strptime(date_text, "%Y-%m-%d").date()
        except:
            return None

    def replace_nan_with_none(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Replace NaN values with None in the given pandas DataFrame.

        Args:
            df (pd.DataFrame): Input DataFrame containing NaN values.

        Returns:
            pd.DataFrame: DataFrame with NaN values replaced by None.

        Raises:
            None

        Notes:
            - If the version of pandas is 1.3.0 or higher, the function uses the `replace` method to replace NaN values with None.
            - For older versions of pandas, the function uses the `where` method in combination with `pd.notnull` to replace NaN values with None.

        """

        df_cpy = df.copy()
        if version.parse(pd.__version__) >= version.parse("1.3.0"):
            df_cpy = df_cpy.replace({np.nan: None})
        else:
            df_cpy = df_cpy.where(pd.notnull(df_cpy), None)

        return df_cpy

    def replace_values(self, func: Any, *args, **kwargs) -> Union[Any, None]:
        """
        This function can be used in a list comprehension to apply a function to returned elements of a loop

        Args:
            func (Any): Function to be applied
            args: list arguments to passed function
            kwargs: dictionary arguments to passed function

        Returns:
            output of function or NaN

        """

        try:
            return func(*args, **kwargs)
        except:
            return np.nan
