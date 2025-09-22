from decimal import Decimal


def has_significant_difference(str1, str2):
    """
    Check if str1 and str2 are exactly one unit of precision apart.
    For positive numbers: str1 should be greater than str2
    For negative numbers: str1 should have greater absolute value (more negative) than str2

    Args:
        str1, str2: String representations of numbers

    Returns:
        bool: True if the numbers are exactly one unit of precision apart in the expected direction
    """
    # Parse as Decimal to avoid floating point errors
    num1 = Decimal(str1.strip())
    num2 = Decimal(str2.strip())

    def get_decimal_places(s):
        """Get number of decimal places in a string representation of a number."""
        s = s.strip()
        if "." in s:
            decimal_part = s.split(".")[1]
            return len(decimal_part)
        else:
            return 0

    # Determine decimal places for each string
    decimal_places1 = get_decimal_places(str1)
    decimal_places2 = get_decimal_places(str2)

    # Use the maximum decimal places to determine the finest precision
    max_decimal_places = max(decimal_places1, decimal_places2)

    # Threshold is 1 unit in the least significant decimal place
    threshold = Decimal(10) ** (-max_decimal_places)

    # Check if both numbers are negative
    if num1 < 0 and num2 < 0:
        # For negative numbers, we want num1 to have greater absolute value (more negative)
        # This means num2 - num1 should equal threshold (since num1 < num2 in value)
        difference = num2 - num1
        return difference == threshold
    else:
        # For positive numbers (or mixed), we want num1 > num2
        difference = num1 - num2
        return difference == threshold


# Test cases
if __name__ == "__main__":
    test_cases = [
        # Positive numbers - str1 should be greater than str2
        ("0.54", "0.53", True),  # diff = 0.01, threshold = 0.01, 0.01 == 0.01 = True
        ("0.54", "0.52", False),  # diff = 0.02, threshold = 0.01, 0.02 == 0.01 = False
        (
            "0.53",
            "0.54",
            False,
        ),  # diff = -0.01, threshold = 0.01, -0.01 == 0.01 = False
        ("24.5", "24.4", True),  # diff = 0.1, threshold = 0.1, 0.1 == 0.1 = True
        ("24.5", "24.3", False),  # diff = 0.2, threshold = 0.1, 0.2 == 0.1 = False
        ("125", "124", True),  # diff = 1, threshold = 1, 1 == 1 = True
        ("125", "123", False),  # diff = 2, threshold = 1, 2 == 1 = False
        (
            "1.235",
            "1.234",
            True,
        ),  # diff = 0.001, threshold = 0.001, 0.001 == 0.001 = True
        (
            "1.236",
            "1.234",
            False,
        ),  # diff = 0.002, threshold = 0.001, 0.002 == 0.001 = False
        # Negative numbers - str1 should have greater absolute value (more negative)
        ("-2.230", "-2.229", True),  # str1 more negative by 0.001, threshold = 0.001
        ("-2.231", "-2.229", False),  # str1 more negative by 0.002, not exactly 0.001
        ("-2.229", "-2.230", False),  # str1 less negative, wrong direction
        ("-5.6", "-5.5", True),  # str1 more negative by 0.1, threshold = 0.1
        ("-5.7", "-5.5", False),  # str1 more negative by 0.2, not exactly 0.1
        ("-126", "-125", True),  # str1 more negative by 1, threshold = 1
        ("-127", "-125", False),  # str1 more negative by 2, not exactly 1
    ]

    print("Testing significant difference function:")
    for str1, str2, expected in test_cases:
        result = has_significant_difference(str1, str2)
        status = "✓" if result == expected else "✗"
        print(
            f"{status} has_significant_difference('{str1}', '{str2}') = {result} (expected {expected})"
        )
        if result != expected:
            num1, num2 = Decimal(str1), Decimal(str2)
            decimal_places = max(
                len(str1.split(".")[1]) if "." in str1 else 0,
                len(str2.split(".")[1]) if "." in str2 else 0,
            )
            threshold = Decimal(10) ** (-decimal_places)
            diff = num1 - num2  # Changed from abs() to show directional difference
            print(
                f"    Debug: diff={diff}, threshold={threshold}, decimal_places={decimal_places}"
            )
