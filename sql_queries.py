"""SQL for redshift."""

# i94addr -> state_code
# i94cit -> birth_country
# i94res -> residence_country
# i94port -> airport_code
immigration_table_create = (
    """
    CREATE TABLE IF NOT EXISTS immigration (
        cicid             VARCHAR,
        i94mon            INT,
        i94yr             INT,
        birth_country     VARCHAR,
        residence_country VARCHAR,
        state_code        VARCHAR,
        i94mode           VARCHAR,
        airport_code      VARCHAR,
        i94visa           VARCHAR,
        arrdate           VARCHAR,
        depdate           VARCHAR,
        biryear           VARCHAR,
        gender            VARCHAR,
        visatype          VARCHAR,
        fltno             VARCHAR,
        airline           VARCHAR,
        matflag           VARCHAR,
        admnum            VARCHAR
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
# airport-codes_csv.csv
# municipality -> city
# iata_code -> airport_code
airport_detail_table_create = (
    """
    CREATE TABLE IF NOT EXISTS airport_detail (
        airport_id   INT,
        type         VARCHAR,
        name         VARCHAR,
        elevation_ft INT,
        country      VARCHAR,
        city         VARCHAR,
        airport_code VARCHAR,
        latitude     FLOAT,
        longitude    FLOAT
    )
    """
)
# us-cities-demographics.csv
cities_demographics_table_create = (
    """
    CREATE TABLE IF NOT EXISTS cities_demographics (
        city_id                INT,
        city                   VARCHAR,
        state                  VARCHAR,
        median_age             REAL,
        male_population        INT,
        female_population      INT,
        total_population       INT,
        veteran_count          INT,
        foreign_born           INT,
        average_household_size REAL,
        state_code             VARCHAR
    )
    """
)

# join (immigration, airport, airport_detail)
# join cities_demographics
