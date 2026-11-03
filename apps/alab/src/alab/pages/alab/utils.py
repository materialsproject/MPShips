import re

def parse_formula(formula):
    """Parse chemical formula to element composition"""
    pattern = r"([A-Z][a-z]?)(\d*\.?\d*)"
    matches = re.findall(pattern, formula)
    composition = {}

    for element, count in matches:
        if element and element not in ["(", ")"]:
            amount = float(count) if count else 1.0
            composition[element] = composition.get(element, 0) + amount

    return composition