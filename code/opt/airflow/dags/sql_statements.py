
get_most_used_aicraft_model = ("""

WITH cte AS (
    SELECT aircraft_code, 
           COUNT(*),
           RANK() OVER (ORDER BY COUNT(*) DESC) rnk
    FROM flights
    GROUP BY aircraft_code
)

SELECT aircraft_code
FROM cte
where rnk = 1;

""")


get_flights_with_most_used_aicraft_model = (
    """
    SELECT flight_id,
          flight_no,
          scheduled_departure,
          scheduled_arrival,
          departure_airport,
          arrival_airport,
          status,
          aircraft_code,
          actual_departure,
          actual_arrival
    FROM flights
    WHERE aircraft_code in ({});
    """    
)

get_tickets_booked_last_6_months = ("""

WITH cte_2 as (
    
    SELECT tf.ticket_no, tf.flight_id as flight_id_2, tf.amount, b.book_date
    FROM bookings b
    JOIN tickets t ON b.book_ref = t.book_ref
    JOIN ticket_flights tf ON t.ticket_no = tf.ticket_no
    JOIN flights f ON tf.flight_id = f.flight_id
    WHERE b.book_date >= date_trunc('month', (select max(book_date) from bookings)-interval '6 months')   
)
select *
from cte_2

""")


get_average_count_tickets = ("""

INSERT INTO staging_4
    SELECT avg(t.rcount) as average_tickets
        FROM ( 
        SELECT aircraft_code, count(*) as rcount 
        FROM staging_3
        GROUP BY aircraft_code
    ) t;
""")


create_staging_1 = ("""


    DROP TABLE IF EXISTS staging_1;
    CREATE TABLE staging_1 (

        flight_id integer NOT NULL,
        flight_no character(6) NOT NULL,
        scheduled_departure timestamp with time zone NOT NULL,
        scheduled_arrival timestamp with time zone NOT NULL,
        departure_airport character(3) NOT NULL,
        arrival_airport character(3) NOT NULL,
        status character varying(20) NOT NULL,
        aircraft_code character(3) NOT NULL,
        actual_departure timestamp with time zone,
        actual_arrival timestamp with time zone
    );

""")

create_staging_2 = ("""

    DROP TABLE IF EXISTS staging_2;
    CREATE TABLE staging_2 (
        ticket_no character(13) NOT NULL,
        flight_id_2 integer NOT NULL,
        amount numeric(10,2) NOT NULL,
        book_date timestamp with time zone NOT NULL
        );
""")


create_staging_3 = ("""

    DROP TABLE IF EXISTS staging_3;
    CREATE TABLE staging_3 (
        flight_id integer NOT NULL,
        flight_no character(6) NOT NULL,
        scheduled_departure timestamp with time zone NOT NULL,
        scheduled_arrival timestamp with time zone NOT NULL,
        departure_airport character(3) NOT NULL,
        arrival_airport character(3) NOT NULL,
        status character varying(20) NOT NULL,
        aircraft_code character(3) NOT NULL,
        actual_departure timestamp with time zone,
        actual_arrival timestamp with time zone,
        ticket_no character(13) NOT NULL,
        flight_id_2 integer NOT NULL,
        amount numeric(10,2) NOT NULL,
        book_date timestamp with time zone NOT NULL
        );

""")

create_staging_4 = ("""

    DROP TABLE IF EXISTS staging_4;
    CREATE TABLE staging_4 (
        average_tickets numeric(10,2) NOT NULL
        );
""")

drop_staging = ("""
    DROP TABLE IF EXISTS staging_1;
    DROP TABLE IF EXISTS staging_2;
    DROP TABLE IF EXISTS staging_3;
    DROP TABLE IF EXISTS staging_4;
""")