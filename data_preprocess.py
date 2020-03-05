"""Data preprocess module."""


def preprocess_state():
    """Preprocess state table."""
    result = []
    with open('mapping/i94addrl.txt', 'r') as file:
        for line in file:
            line = line.replace("'", "").strip()
            state_code, state_name = tuple(line.split('='))
            result.append({'state_code': state_code, 'state_name': state_name})
    return result


def preprocess_airport():
    """Preprocess airport table."""
    result = []
    with open('mapping/i94prtl.txt', 'r') as file:
        for line in file:
            line = line.replace("'", "").strip()
            airport_code, name = tuple(
                map(lambda x: x.strip(), line.split('='))
            )
            temp = list(map(lambda x: x.strip(), name.split(',')))
            if len(temp) > 1:
                state_code = temp[-1]
                airport_name = ",".join(temp[:-1])
            else:
                airport_name = temp[0]
                state_code = None
            result.append({
                'airport_code': airport_code,
                'airport_name': airport_name,
                'state_code': state_code
            })
    return result
