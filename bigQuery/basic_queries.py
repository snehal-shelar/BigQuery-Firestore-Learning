from google.cloud import bigquery

client = bigquery.Client()


def run_queries():
    """
    This function reads a query and prints out the rows.
    Some SQL queries to practice:
    -- SELECT * FROM `robotic-octane-484810-s6.patient_synthetic_data.observation` LIMIT 10;

    -- SELECT person_id, observation_id FROM `patient_synthetic_data.observation`;

    -- SELECT person_id FROM patient_synthetic_data.observation WHERE observation_date>"2010-03-16";

    -- SELECT observation_id, person_id FROM `patient_synthetic_data.observation` WHERE person_id = 1331825 LIMIT 10;

    -- select * from `patient_synthetic_data.observation` where person_id=1331825 order by observation_date desc;

    -- select observation_id, person_id from `patient_synthetic_data.observation` where person_id in (select person_id from `patient_synthetic_data.person`);

    -- select * from `patient_synthetic_data.person` as ob left join `patient_synthetic_data.person` as person on ob.person_id=person.person_id limit 10;

    -- select * from `patient_synthetic_data.person` as per right join `patient_synthetic_data.observation` as obs on per.person_id=obs.person_id limit 10;

    -- CRUD Operations(DML operations are only available in Billed account)
        -- create table patient_synthetic_data.users (id int, name string)
        -- insert into `patient_synthetic_data.users` (id, name) values(1, 'Joy')
        -- delete from `patient_synthetic_data.person` where person_source_value='F8B619B21A5BD0EE';
    """

    select_query = """
                   SELECT *
                   FROM `robotic-octane-484810-s6.patient_synthetic_data.observation` LIMIT 10; \
                   """

    results = client.query(select_query)
    for row in results:
        for key, val in row.items():
            print(f'{key} : {val}')


run_queries()
