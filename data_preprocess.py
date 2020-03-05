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


def preprocess_country():
    """Preprocess country table."""
    result = []
    with open('mapping/i94cntyl.txt', 'r') as file:
        for line in file:
            line = line.replace("'", "").strip()
            country_code, country_name = tuple(
                map(lambda x: x.strip(), line.split('='))
            )
            result.append(
                {'country_code': country_code, 'country_name': country_name}
            )
    return result


def preprocess_transport_type():
    """Preprocess transport_type table."""
    result = []
    with open('mapping/i94model.txt', 'r') as file:
        for line in file:
            line = line.replace("'", "").strip()
            transport_code, transport_name = tuple(
                map(lambda x: x.strip(), line.split('='))
            )
            result.append(
                {
                    'transport_code': transport_code,
                    'transport_name': transport_name
                }
            )
    return result


def preprocess_visa_type():
    """Preprocess visa_type table."""
    result = []
    with open('mapping/i94visa.txt', 'r') as file:
        for line in file:
            line = line.replace("'", "").strip()
            visa_code, visa_name = tuple(
                map(lambda x: x.strip(), line.split('='))
            )
            result.append(
                {'visa_code': visa_code, 'visa_name': visa_name}
            )
    return result
