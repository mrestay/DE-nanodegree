class SqlQueries:
    """Class with SQL queries"""
    immigration_fact_create = """
    CREATE TABLE public.immigration (
    immigration_id BIGINT PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    flight_number VARCHAR(255),
    port_code VARCHAR(255)
    )
    """

    flights_dimension_create = """
    CREATE TABLE public.flights (
    flight_number VARCHAR(255) PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    airline VARCHAR(255)
    )
    """

    visitors_dimension_create = """
    CREATE TABLE public.visitors (
    immigration_id BIGINT PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    residency_country VARCHAR(255),
    citizenship_country VARCHAR(255),
    age INT2,
    gender VARCHAR(255),
    occupation VARCHAR(255)
    )
    """

    dates_dimension_create = """
    CREATE TABLE public.dates(
    immigration_id BIGINT PRIMARY KEY DISTKEY NOT NULL,
    arrival_date DATE SORTKEY,
    arrival_year INT2,
    arrival_month INT2,
    arrival_day INT2,
    departure_date DATE,
    departure_year INT2,
    departure_month INT2,
    departure_day INT2    
    )
    """

    visas_dimension_create = """
    CREATE TABLE public.visas (
    immigration_id BIGINT PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    visa_type VARCHAR(255),
    visa_code VARCHAR(255),
    department_state_visa_issued VARCHAR(255)
    )
    """

    general_demographics_dimension_create = """
    CREATE TABLE public.general_demographics (
    port_code VARCHAR(255) PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    state VARCHAR(255),
    state_code VARCHAR(255),
    city VARCHAR(255),
    male_population BIGINT,
    female_population BIGINT,
    total_population BIGINT,
    median_age FLOAT,
    number_of_veterans BIGINT,
    foreign_born BIGINT,
    average_household_size FLOAT
    )
    """

    race_demographics_dimension_create = """
    CREATE TABLE public.race_demographics (
    port_code VARCHAR(255) PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    state_code VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255),
    native_count BIGINT,
    asian_count BIGINT,
    black_count BIGINT,
    hispanic_count BIGINT,
    white_count BIGINT
    )
    """

    airports_dimension_create = """
    CREATE TABLE public.airports (
    port_code VARCHAR(255) PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    local_code VARCHAR(255),
    iata_code VARCHAR(255),
    type VARCHAR(255),
    name VARCHAR(255),
    state VARCHAR(255),
    city VARCHAR(255),
    elevation_ft INT4,
    x_coordinate FLOAT,
    y_coordinate FLOAT    
    )
    """

    port_codes_dimension_create = """
    CREATE TABLE public.port_codes (
    port_code VARCHAR(255) PRIMARY KEY SORTKEY DISTKEY NOT NULL,
    state VARCHAR(255),
    city VARCHAR(255)
    )
    """
