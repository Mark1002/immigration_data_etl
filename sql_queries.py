"""SQL for redshift."""

immigration_table_create = (
    """
    CREATE TABLE IF NOT EXISTS immigration (
        cicid    VARCHAR,
        i94mon   INT,
        i94yr    INT,
        i94cit   VARCHAR,
        i94res   VARCHAR,
        i94addr  VARCHAR,
        i94mode  VARCHAR,
        i94port  VARCHAR,
        i94visa  VARCHAR,
        arrdate  VARCHAR,
        depdate  VARCHAR,
        biryear  VARCHAR,
        gender   VARCHAR,
        visatype VARCHAR,
        fltno    VARCHAR,
        airline  VARCHAR,
        matflag  VARCHAR,
        admnum   VARCHAR,
    )
    """
)
# from i94addrl.txt
state_table_create = (
    """
    CREATE TABLE IF NOT EXISTS state (
        state_code VARCHAR,
        state_name VARCHAR
    )
    """
)
# from i94cntyl.txt
country_table_create = (
    """
    CREATE TABLE IF NOT EXISTS country (
        country_code VARCHAR,
        country_name VARCHAR
    )
    """
)
# from i94model.txt
transport_type_table_create = (
    """
    CREATE TABLE IF NOT EXISTS transport_type (
        transport_code INT,
        transport_name VARCHAR
    )
    """
)
# from i94prtl.txt
airport_table_create = (
    """
    CREATE TABLE IF NOT EXISTS airport (
        airport_code VARCHAR,
        airport_name VARCHAR,
        state_code VARCHAR
    )
    """
)

# from i94visa.txt
visa_type_table_create = (
    """
    CREATE TABLE IF NOT EXISTS visa_type (
        visa_code INT,
        visa_name VARCHAR
    )
    """
)
