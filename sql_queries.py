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
