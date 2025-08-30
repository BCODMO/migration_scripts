from decimal import Decimal, ROUND_HALF_UP
import os
import io
import six
import shutil
import atexit
import openpyxl
import datetime
import re

from typing import List, Dict, Any, Tuple, Optional


def convert_old(excel_number: str, value: Any) -> Any:
    """
    A basic attempt to convert excel number_format to a number string

    The important goal here is to get proper amount of rounding
    """
    if "@" in excel_number:
        # We don't try to parse complicated strings
        return str(value)
    percentage = False
    if excel_number.endswith("%"):
        value = value * 100
        excel_number = excel_number[:-1]
        percentage = True
    if excel_number == "General":
        return value
    multi_codes = excel_number.split(";")
    if value < 0 and len(multi_codes) > 1:
        excel_number = multi_codes[1]
    else:
        excel_number = multi_codes[0]

    code = excel_number.split(".")

    if len(code) > 2:
        return None
    if len(code) < 2:
        # No decimals
        new_value = "{0:.0f}".format(value)

    # Currently we do not support "engineering notation"
    elif re.match(r"^#+0*E\+0*$", code[1]):
        return value
    elif re.match(r"^0*E\+0*$", code[1]):
        # Handle scientific notation

        # Note, it will only actually be returned as a string if
        # type is not inferred

        prec = len(code[1]) - len(code[1].lstrip("0"))
        exp_digits = len(code[1]) - len(code[1].rstrip("0"))
        return eformat(value, prec, exp_digits)

    else:
        decimal_section = code[1]
        # Only pay attention to the 0, # and ? characters as they provide precision information
        decimal_section = "".join(d for d in decimal_section if d in ["0", "#", "?"])

        # Count the number of hashes at the end of the decimal_section in order to know how
        # the number should be truncated
        number_hash = 0
        for i in reversed(range(len(decimal_section))):
            if decimal_section[i] == "#":
                number_hash += 1
            else:
                break
        string_format_code = "{0:." + str(len(decimal_section)) + "f}"
        new_value = string_format_code.format(value)
        if number_hash > 0:
            for i in range(number_hash):
                if new_value.endswith("0"):
                    new_value = new_value[:-1]
    if percentage:
        return new_value + "%"

    return new_value


def convert_new(excel_number: str, value: Any) -> Any:
    """
    A basic attempt to convert excel number_format to a number string

    The important goal here is to get proper amount of rounding
    """
    if "@" in excel_number:
        # We don't try to parse complicated strings
        return str(value)
    percentage = False
    if excel_number.endswith("%"):
        value = value * 100
        excel_number = excel_number[:-1]
        percentage = True
    if excel_number == "General":
        return value
    multi_codes = excel_number.split(";")
    if value < 0 and len(multi_codes) > 1:
        excel_number = multi_codes[1]
    else:
        excel_number = multi_codes[0]

    code = excel_number.split(".")

    if len(code) > 2:
        return None
    if len(code) < 2:
        # No decimals
        # We use Decimal to fix floating point error
        decimal_value = Decimal(str(value))
        quantized = decimal_value.quantize(0, rounding=ROUND_HALF_UP)
        new_value = str(quantized)

    # Currently we do not support "engineering notation"
    elif re.match(r"^#+0*E\+0*$", code[1]):
        return value
    elif re.match(r"^0*E\+0*$", code[1]):
        # Handle scientific notation

        # Note, it will only actually be returned as a string if
        # type is not inferred

        prec = len(code[1]) - len(code[1].lstrip("0"))
        exp_digits = len(code[1]) - len(code[1].rstrip("0"))
        return eformat(value, prec, exp_digits)

    else:
        decimal_section = code[1]
        # Only pay attention to the 0, # and ? characters as they provide precision information
        decimal_section = "".join(d for d in decimal_section if d in ["0", "#", "?"])

        # Count the number of hashes at the end of the decimal_section in order to know how
        # the number should be truncated
        number_hash = 0
        for i in reversed(range(len(decimal_section))):
            if decimal_section[i] == "#":
                number_hash += 1
            else:
                break

        # Round using Decimal to prevent floating point error
        decimal_value = Decimal(str(value))
        precision = Decimal(10) ** -len(decimal_section)
        quantized = decimal_value.quantize(precision, rounding=ROUND_HALF_UP)
        new_value = str(quantized)
        if number_hash > 0:
            for i in range(number_hash):
                if new_value.endswith("0"):
                    new_value = new_value[:-1]
    if percentage:
        return new_value + "%"

    return new_value


def handle_general_old(number_format: str, value: Any) -> Any:
    return ""
    integer_digits = len(str(int(value)))
    # Set the precision to 15 minus the number of integer digits
    precision = 15 - (integer_digits)
    return str(round(value, precision))


def handle_general_new(number_format: str, value: Any) -> Any:
    return ""
    integer_digits = len(str(int(value)))

    decimal_value = Decimal(str(value))
    precision = Decimal(10) ** -integer_digits
    quantized = decimal_value.quantize(precision, rounding=ROUND_HALF_UP)
    new_value = str(quantized)
    return new_value


def eformat(f, prec, exp_digits):
    """
    Formats to Scientific Notation, including precise exponent digits

    """
    s = "%.*e" % (prec, f)
    mantissa, exp = s.split("e")
    # add 1 to digits as 1 is taken by sign +/-
    return "%sE%+0*d" % (mantissa, exp_digits + 1, int(exp))
